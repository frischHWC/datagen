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
package com.cloudera.frisch.datagen.connector.storage.s3;


import com.cloudera.frisch.datagen.config.ApplicationConfigs;
import com.cloudera.frisch.datagen.connector.ConnectorInterface;
import com.cloudera.frisch.datagen.connector.storage.utils.FileUtils;
import com.cloudera.frisch.datagen.connector.storage.utils.OrcUtils;
import com.cloudera.frisch.datagen.model.Model;
import com.cloudera.frisch.datagen.model.OptionsConverter;
import com.cloudera.frisch.datagen.model.Row;
import com.cloudera.frisch.datagen.model.type.Field;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.cloudera.frisch.datagen.config.ApplicationConfigs.DATA_HOME_DIRECTORY;

/**
 * This is a ORC connector to write to one or multiple ORC files to S3
 */
@Slf4j
public class S3OrcConnector extends S3Utils implements ConnectorInterface  {

  private final Model model;
  private final Boolean oneFilePerIteration;
  private final String localFilePathForModelGeneration;

  private int counter;
  private String currentLocalFileName;
  private String currentKeyName;

  private final TypeDescription schema;
  private Writer orcWriter;
  private final Map<String, ColumnVector> vectors;
  private final VectorizedRowBatch batch;

  /**
   * Init S3 ORC
   */
  public S3OrcConnector(Model model,
                        Map<ApplicationConfigs, String> properties) {
    super(model, properties);
    this.model = model;
    this.counter = 0;
    this.oneFilePerIteration = (Boolean) model.getOptionsOrDefault(
        OptionsConverter.Options.ONE_FILE_PER_ITERATION);
    this.localFilePathForModelGeneration = properties.get(DATA_HOME_DIRECTORY) + "/model-gen/s3/";
    this.schema = model.getOrcSchema();
    this.batch = schema.createRowBatch();
    this.vectors = model.createOrcVectors(batch);
  }

  @Override
  public void init(Model model, boolean writer) {
    if (writer) {
      if ((Boolean) model.getOptionsOrDefault(
          OptionsConverter.Options.DELETE_PREVIOUS)) {
        s3Client.listObjects(
                ListObjectsRequest.builder().bucket(bucketName)
                    .prefix(localDirectoryName)
                    .build())
            .contents()
            .forEach(k -> s3Client.deleteObject(
                DeleteObjectRequest.builder().bucket(bucketName).key(k.key())
                    .build()));
      }

      // Will use a local directory before pushing data to S3
      FileUtils.createLocalDirectory(localFileTempDir);
      FileUtils.deleteAllLocalFiles(localFileTempDir, keyNamePrefix, "orc");

      createBucketIfNotExists();

      if (!oneFilePerIteration) {
        this.currentLocalFileName = localFileNamePrefix + ".orc";
        this.currentKeyName = localDirectoryName + currentLocalFileName;
        this.orcWriter = OrcUtils.createLocalFileWithOverwrite(localFileTempDir +
            currentLocalFileName, this.orcWriter, this.schema);
      }
    } else {
      FileUtils.createLocalDirectory(localFilePathForModelGeneration);
    }
  }

  @Override
  public void terminate() {
    try {
      if (!oneFilePerIteration) {
        this.orcWriter.close();
        pushLocalFileToS3(localFileTempDir + currentLocalFileName, currentKeyName);
      }
    } catch (IOException e) {
      log.error(" Unable to close local file with error :", e);
    } finally {
      FileUtils.deleteAllLocalFiles(localFileTempDir, localFileNamePrefix, "orc");
    }
  }

  @Override
  public void sendOneBatchOfRows(List<Row> rows) {
    try {
      if (oneFilePerIteration) {
        this.currentLocalFileName = localFileNamePrefix + "-" + String.format("%010d", counter) + ".orc";
        this.currentKeyName = localDirectoryName + currentLocalFileName;
        this.orcWriter = OrcUtils.createLocalFileWithOverwrite(localFileTempDir +
            currentLocalFileName, this.orcWriter, this.schema);
        counter++;
      }

      for (Row row : rows) {
        int rowNumber = batch.size++;
        row.fillinOrcVector(rowNumber, vectors);
        try {
          if (batch.size == batch.getMaxSize()) {
            orcWriter.addRowBatch(batch);
            batch.reset();
          }
        } catch (IOException e) {
          log.error("Can not write data to the local file due to error: ", e);
        }
      }
      try {
        if (batch.size != 0) {
          orcWriter.addRowBatch(batch);
          batch.reset();
        }
      } catch (IOException e) {
        log.error("Can not write data to the local file due to error: ", e);
      }

      if (oneFilePerIteration) {
        this.orcWriter.close();
        pushLocalFileToS3(localFileTempDir + currentLocalFileName, currentKeyName);
        FileUtils.deleteLocalFile(localFileTempDir + currentLocalFileName);
      }
    } catch (IOException e) {
      log.error("Can not write data to the local file due to error: ", e);
    }
  }

  @Override
  public Model generateModel(Boolean deepAnalysis) {
    LinkedHashMap<String, Field> fields = new LinkedHashMap<String, Field>();
    Map<String, List<String>> primaryKeys = new HashMap<>();
    Map<String, String> tableNames = new HashMap<>();
    Map<String, String> options = new HashMap<>();

    tableNames.put("S3_LOCAL_FILE_PATH", this.localDirectoryName);
    tableNames.put("S3_KEY_NAME", this.keyNamePrefix);
    tableNames.put("S3_BUCKET", this.bucketName);

    try {
      String localFile = this.localFilePathForModelGeneration + this.localFileNamePrefix;
      readFileFromS3(localFile, this.keyNamePrefix);
      File file = new File(localFile);
      if (file.exists() && file.isFile()) {
        Reader reader =
            OrcFile.createReader(new Path(localFile),
                OrcFile.readerOptions(new Configuration()));

        OrcUtils.setBasicFields(fields, reader);
        if (deepAnalysis) {
          OrcUtils.analyzeFields(fields, reader);
        }
      }
    } catch (IOException e) {
      log.error("Tried to read file : {} with no success :", this.localDirectoryName,
          e);
    }

    return new Model(fields, primaryKeys, tableNames, options);
  }


}
