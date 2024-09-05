/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datagen.connector.storage.ozone;


import com.datagen.config.ApplicationConfigs;
import com.datagen.connector.ConnectorInterface;
import com.datagen.connector.storage.utils.FileUtils;
import com.datagen.connector.storage.utils.OrcUtils;
import com.datagen.model.Model;
import com.datagen.model.OptionsConverter;
import com.datagen.model.Row;
import com.datagen.model.type.Field;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


@Slf4j
public class OzoneOrcConnector extends OzoneUtils implements ConnectorInterface {

  private TypeDescription schema;
  private Writer writer;
  private Map<String, ColumnVector> vectors;
  private VectorizedRowBatch batch;

  private final Boolean oneFilePerIteration;
  private final Model model;
  private int counter;


  public OzoneOrcConnector(Model model,
                           Map<ApplicationConfigs, String> properties) {
    super(model, properties);
    this.oneFilePerIteration = (Boolean) model.getOptionsOrDefault(
        OptionsConverter.Options.ONE_FILE_PER_ITERATION);
    this.model = model;
    this.counter = 0;
  }

  @Override
  public void init(Model model, boolean writer) {
    if (writer) {
      try {
        schema = model.getOrcSchema();
        batch = schema.createRowBatch();
        vectors = model.createOrcVectors(batch);

        if ((Boolean) model.getOptionsOrDefault(
            OptionsConverter.Options.DELETE_PREVIOUS)) {
          deleteEverythingUnderABucket();
        }
        createVolumeIfItDoesNotExist();
        this.volume = objectStore.getVolume(volumeName);
        createBucketIfNotExist();
        this.bucket = volume.getBucket(bucketName);

        // Will use a local directory before pushing data to Ozone
        FileUtils.createLocalDirectory(localFileTempDir);
        FileUtils.deleteAllLocalFiles(localFileTempDir, keyNamePrefix, "orc");

        if (!oneFilePerIteration) {
          this.writer = OrcUtils.createLocalFileWithOverwrite(localFileTempDir + keyNamePrefix + ".orc", this.writer, schema);
        }

      } catch (IOException e) {
        log.error(
            "Could not connect and create Volume into Ozone, due to error: ",
            e);
      }

    }
  }

  @Override
  public void terminate() {
    try {
      if (!oneFilePerIteration) {
        writer.close();
        // Send local file to Ozone
        pushKeyToOzone(localFileTempDir + keyNamePrefix + ".orc", keyNamePrefix + ".orc");
      }
      closeOzone();
    } catch (IOException e) {
      log.warn("Could not close properly Ozone connection, due to error: ", e);
    } finally {
      FileUtils.deleteAllLocalFiles(localFileTempDir, keyNamePrefix, "orc");
    }
  }

  @Override
  public void sendOneBatchOfRows(List<Row> rows) {
    // Let's create a temp local file and then pushes it to ozone ?
    String keyName =
        keyNamePrefix + "-" + String.format("%010d", counter) + ".orc";
    // Write to local file
    if (oneFilePerIteration) {
      this.writer = OrcUtils.createLocalFileWithOverwrite(localFileTempDir + keyName, this.writer, schema);
      counter++;
    }

    for (Row row : rows) {
      int rowNumber = batch.size++;
      row.fillinOrcVector(rowNumber, vectors);
      try {
        if (batch.size == batch.getMaxSize()) {
          writer.addRowBatch(batch);
          batch.reset();
        }
      } catch (IOException e) {
        log.error("Can not write data to the local file due to error: ", e);
      }
    }
    try {
      if (batch.size != 0) {
        writer.addRowBatch(batch);
        batch.reset();
      }
    } catch (IOException e) {
      log.error("Can not write data to the local file due to error: ", e);
    }

    if (oneFilePerIteration) {
      try {
        writer.close();
      } catch (IOException e) {
        log.error(" Unable to close local file with error :", e);
      }

      // Send local file to Ozone
      pushKeyToOzone(localFileTempDir + keyName, keyName);
    }

  }

  @Override
  public Model generateModel(Boolean deepAnalysis) {
    LinkedHashMap<String, Field> fields = new LinkedHashMap<String, Field>();
    Map<String, List<String>> primaryKeys = new HashMap<>();
    Map<String, String> tableNames = new HashMap<>();
    Map<String, String> options = new HashMap<>();
    try {
      OzoneKeyDetails ozoneKeyDetails = this.bucket.getKey(this.keyNamePrefix);
      // To prevent OOM, we will not get and read file over 1GB
      long dataSize = ozoneKeyDetails.getDataSize();
      if (dataSize < 1073741824) {
        byte[] readBuffer = new byte[(int) (dataSize + 1)];
        ozoneKeyDetails.getContent().read(readBuffer);

        // Use of a local temp file to write Ozone file and finally delete it
        String filepath =
            "/tmp/" + this.keyNamePrefix + "_" + System.currentTimeMillis();
        File file = new File(filepath);
        if (file.exists()) {
          file.delete();
        }
        file.setReadable(true, true);
        file.setWritable(true, true);
        file.setExecutable(true, true);

        try {
          file.createNewFile();
          FileOutputStream localTempFile = new FileOutputStream(file);
          localTempFile.write(readBuffer);
          localTempFile.close();

          Reader reader =
              OrcFile.createReader(new Path(filepath),
                  OrcFile.readerOptions(new Configuration()));

          OrcUtils.setBasicFields(fields, reader);
          if (deepAnalysis) {
            OrcUtils.analyzeFields(fields, reader);
          }

        } catch (Exception e) {
          log.warn("Cannot write and read local file taken from Ozone");
        } finally {
          file.delete();
        }

      } else {
        log.warn(
            "File {} under volume {} in bucket {} is more than 1GB, so cannot be read",
            this.keyNamePrefix, this.volume, this.bucket);
      }
      ozClient.close();
    } catch (IOException e) {
      log.error(
          "Could not connect and read key: {} into Ozone, due to error: ",
          keyNamePrefix, e);
    }
    return new Model("",fields, primaryKeys, tableNames, options, null);
  }

}
