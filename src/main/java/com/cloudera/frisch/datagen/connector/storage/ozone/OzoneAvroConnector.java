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
package com.cloudera.frisch.datagen.connector.storage.ozone;


import com.cloudera.frisch.datagen.config.ApplicationConfigs;
import com.cloudera.frisch.datagen.connector.ConnectorInterface;
import com.cloudera.frisch.datagen.connector.storage.utils.AvroUtils;
import com.cloudera.frisch.datagen.connector.storage.utils.FileUtils;
import com.cloudera.frisch.datagen.model.Model;
import com.cloudera.frisch.datagen.model.OptionsConverter;
import com.cloudera.frisch.datagen.model.Row;
import com.cloudera.frisch.datagen.model.type.Field;
import com.cloudera.frisch.datagen.utils.KerberosUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


// TODO: Refactor to use one abstract class
@Slf4j
public class OzoneAvroConnector extends OzoneUtils implements ConnectorInterface {


  private Schema schema;
  private DataFileWriter<GenericRecord> dataFileWriter;
  private DatumWriter<GenericRecord> datumWriter;
  private File file;

  private final Boolean oneFilePerIteration;
  private final Model model;
  private int counter;


  public OzoneAvroConnector(Model model,
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
        FileUtils.deleteAllLocalFiles(localFileTempDir,
            keyNamePrefix, "avro");

        schema = model.getAvroSchema();
        datumWriter = new GenericDatumWriter<>(schema);

        if (!oneFilePerIteration) {
          this.dataFileWriter = AvroUtils.createFileWithOverwrite(localFileTempDir + keyNamePrefix + ".avro", schema, datumWriter);
        }

      } catch (IOException e) {
        log.error(
            "Could not connect and create Volume into Ozone, due to error: ",
            e);
      }
    } else {
      try {
        this.volume = objectStore.getVolume(volumeName);
        this.bucket = volume.getBucket(bucketName);
      } catch (IOException e) {
        log.error(
            "Could not connect and read Volume {} and bucket: {} into Ozone, due to error: ",
            volume, bucket,
            e);
      }
    }

  }

  @Override
  public void terminate() {
    try {
      if (!oneFilePerIteration) {
        dataFileWriter.close();
        // Send local file to Ozone
        String keyName = keyNamePrefix + ".avro";
        pushKeyToOzone(localFileTempDir + keyName, keyName);
      }
      closeOzone();
      FileUtils.deleteAllLocalFiles(localFileTempDir, keyNamePrefix, "avro");
    } catch (IOException e) {
      log.warn("Could not close properly Ozone connection, due to error: ", e);
    }
    if (useKerberos) {
      KerberosUtils.logoutUserWithKerberos();
    }
  }

  @Override
  public void sendOneBatchOfRows(List<Row> rows) {
    // Let's create a temp local file and then pushes it to ozone ?
    String keyName =
        keyNamePrefix + "-" + String.format("%010d", counter) + ".avro";
    // Write to local file
    if (oneFilePerIteration) {
      this.dataFileWriter = AvroUtils.createFileWithOverwrite(localFileTempDir + keyNamePrefix + ".avro", schema, datumWriter);
      counter++;
    }
    rows.stream().map(row -> row.toGenericRecord(schema))
        .forEach(genericRecord -> {
          try {
            dataFileWriter.append(genericRecord);
          } catch (IOException e) {
            log.error("Can not write data to the local file due to error: ", e);
          }
        });
    if (oneFilePerIteration) {
      try {
        dataFileWriter.close();
      } catch (IOException e) {
        log.error(" Unable to close local file with error :", e);
      }

      // Send local file to Ozone
      pushKeyToOzone(localFileTempDir + keyName, keyName);
      FileUtils.deleteAllLocalFiles(localFileTempDir, keyNamePrefix, "avro");
    } else {
      try {
        dataFileWriter.flush();
      } catch (IOException e) {
        log.error("Can not flush data to the local file due to error: ", e);
      }
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

        DataFileStream<GenericRecord> dataFileStream = new DataFileStream<>(new ByteArrayInputStream(readBuffer), new GenericDatumReader<>());
        AvroUtils.setBasicFields(fields, dataFileStream.getSchema());
        dataFileStream.close();
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


    return new Model(fields, primaryKeys, tableNames, options);
  }



}
