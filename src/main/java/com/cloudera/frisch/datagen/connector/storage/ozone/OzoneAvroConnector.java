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
import com.cloudera.frisch.datagen.model.Model;
import com.cloudera.frisch.datagen.model.OptionsConverter;
import com.cloudera.frisch.datagen.model.Row;
import com.cloudera.frisch.datagen.model.type.Field;
import com.cloudera.frisch.datagen.utils.Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.*;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.exceptions.OMException;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


/**
 * This is an Ozone Sink base on 0.4 API
 * Note that it could produce some Timeout on heavy workload but it still inserts correctly
 */
@Slf4j
public class OzoneAvroConnector implements ConnectorInterface {

  private OzoneClient ozClient;
  private ObjectStore objectStore;
  private OzoneVolume volume;
  private final String volumeName;
  private final String bucketName;
  private final String keyNamePrefix;
  private final ReplicationFactor replicationFactor;
  private final String localFileTempDir;
  private Boolean useKerberos;

  private final Schema schema;
  private DataFileWriter<GenericRecord> dataFileWriter;
  private final DatumWriter<GenericRecord> datumWriter;
  private File file;
  private final Boolean oneFilePerIteration;
  private final Model model;
  private int counter;
  private OzoneBucket bucket;


  public OzoneAvroConnector(Model model,
                            Map<ApplicationConfigs, String> properties) {
    this.volumeName = (String) model.getTableNames()
        .get(OptionsConverter.TableNames.OZONE_VOLUME);
    this.bucketName = (String) model.getTableNames()
        .get(OptionsConverter.TableNames.OZONE_BUCKET);
    this.keyNamePrefix = (String) model.getTableNames()
        .get(OptionsConverter.TableNames.OZONE_KEY_NAME);
    this.localFileTempDir = (String) model.getTableNames()
        .get(OptionsConverter.TableNames.OZONE_LOCAL_FILE_PATH);
    this.replicationFactor = ReplicationFactor.valueOf(
        (int) model.getOptionsOrDefault(
            OptionsConverter.Options.OZONE_REPLICATION_FACTOR));
    this.oneFilePerIteration = (Boolean) model.getOptionsOrDefault(
        OptionsConverter.Options.ONE_FILE_PER_ITERATION);
    this.model = model;
    this.counter = 0;
    this.schema = model.getAvroSchema();
    this.datumWriter = new GenericDatumWriter<>(schema);
    this.useKerberos = Boolean.parseBoolean(
        properties.get(ApplicationConfigs.OZONE_AUTH_KERBEROS));

    OzoneConfiguration config = new OzoneConfiguration();
    Utils.setupHadoopEnv(config, properties);

    if (useKerberos) {
      Utils.loginUserWithKerberos(
          properties.get(ApplicationConfigs.OZONE_AUTH_KERBEROS_USER),
          properties.get(
              ApplicationConfigs.OZONE_AUTH_KERBEROS_KEYTAB), config);
    }

    try {
      this.ozClient = OzoneClientFactory.getRpcClient(
          properties.get(ApplicationConfigs.OZONE_SERVICE_ID), config);
    } catch (IOException e) {
      log.error("Could get Ozone Client, due to error: ", e);
    }
    this.objectStore = ozClient.getObjectStore();
  }

  @Override
  public void init(Model model, boolean writer) {
    if (writer) {
      try {

        if ((Boolean) model.getOptionsOrDefault(
            OptionsConverter.Options.DELETE_PREVIOUS)) {
          deleteEverythingUnderAVolume(volumeName);
        }
        createVolumeIfItDoesNotExist(volumeName);
        this.volume = objectStore.getVolume(volumeName);
        createBucketIfNotExist(bucketName);
        this.bucket = volume.getBucket(bucketName);

        // Will use a local directory before pushing data to Ozone
        Utils.createLocalDirectory(localFileTempDir);
        Utils.deleteAllLocalFiles(localFileTempDir,
            keyNamePrefix, "avro");

        if (!oneFilePerIteration) {
          createLocalFileWithOverwrite(
              localFileTempDir + keyNamePrefix + ".avro");
          appendAvscHeader();
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
        dataFileWriter.flush();
        dataFileWriter.close();
        // Send local file to Ozone
        String keyName = keyNamePrefix + ".avro";
        try {
          byte[] dataToWrite = Files.readAllBytes(
              java.nio.file.Path.of(localFileTempDir + keyName));
          OzoneOutputStream os = bucket.createKey(keyName, dataToWrite.length,
              ReplicationType.RATIS, replicationFactor, new HashMap<>());
          os.write(dataToWrite);
          os.getOutputStream().flush();
          os.close();
        } catch (IOException e) {
          log.error(
              "Could not write row to Ozone volume: {} bucket: {}, key: {} ; error: ",
              volumeName, bucketName, keyName, e);
        }
      }
      ozClient.close();
      Utils.deleteAllLocalFiles(localFileTempDir, keyNamePrefix, "avro");
    } catch (IOException e) {
      log.warn("Could not close properly Ozone connection, due to error: ", e);
    }
    if (useKerberos) {
      Utils.logoutUserWithKerberos();
    }
  }

  @Override
  public void sendOneBatchOfRows(List<Row> rows) {
    // Let's create a temp local file and then pushes it to ozone ?
    String keyName =
        keyNamePrefix + "-" + String.format("%010d", counter) + ".avro";
    // Write to local file
    if (oneFilePerIteration) {
      createLocalFileWithOverwrite(localFileTempDir + keyName);
      appendAvscHeader();
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
      try {
        byte[] dataToWrite = Files.readAllBytes(
            java.nio.file.Path.of(localFileTempDir + keyName));
        OzoneOutputStream os =
            bucket.createKey(keyName, dataToWrite.length, ReplicationType.RATIS,
                replicationFactor, new HashMap<>());
        os.write(dataToWrite);
        os.getOutputStream().flush();
        os.close();
      } catch (IOException e) {
        log.error(
            "Could not write row to Ozone volume: {} bucket: {}, key: {} ; error: ",
            volumeName, bucketName, keyName, e);
      }
      Utils.deleteAllLocalFiles(localFileTempDir, keyNamePrefix, "avro");
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
        this.bucket.readFile(this.keyNamePrefix).read(readBuffer);

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

  /**
   * Create a bucket if it does not exist
   * In case it exists, it just skips the error and log that bucket already exists
   *
   * @param bucketName
   */
  private void createBucketIfNotExist(String bucketName) {
    try {
      volume.createBucket(bucketName);
      log.debug(
          "Created successfully bucket : " + bucketName + " under volume : " +
              volume);
    } catch (OMException e) {
      if (e.getResult() == OMException.ResultCodes.BUCKET_ALREADY_EXISTS) {
        log.info(
            "Bucket: " + bucketName + " under volume : " + volume.getName() +
                " already exists ");
      } else {
        log.error("An error occurred while creating volume " +
            this.volumeName + " : ", e);
      }
    } catch (IOException e) {
      log.error("Could not create bucket to Ozone volume: " +
              this.volumeName + " and bucket : " + bucketName + " due to error: ",
          e);
    }

  }

  /**
   * Try to create a volume if it does not already exist
   */
  private void createVolumeIfItDoesNotExist(String volumeName) {
    try {
            /*
            In class RPCClient of Ozone (which is the one used by default as a ClientProtocol implementation)
            Function createVolume() uses UserGroupInformation.createRemoteUser().getGroupNames() to get groups
            hence it gets all the groups of the logged user and adds them (which is not really good when you're working from a desktop or outside of the cluster machine)
             */
      objectStore.createVolume(volumeName);
    } catch (OMException e) {
      if (e.getResult() == OMException.ResultCodes.VOLUME_ALREADY_EXISTS) {
        log.info("Volume: " + volumeName + " already exists ");
      } else {
        log.error(
            "An error occurred while creating volume " + volumeName + " : ", e);
      }
    } catch (IOException e) {
      log.error("An unexpected exception occurred while creating volume " +
          volumeName + ": ", e);
    }
  }

  /**
   * Delete all keys in all buckets of a specified volume
   * This is helpful as Ozone does not provide natively this type of function
   *
   * @param volumeName name of the volume to clean and delete
   */
  public void deleteEverythingUnderAVolume(String volumeName) {
    try {
      OzoneVolume volume = objectStore.getVolume(volumeName);

      volume.listBuckets("bucket").forEachRemaining(bucket -> {
        log.debug("Deleting everything in bucket: " + bucket.getName() +
            " in volume: " + volumeName);
        try {
          bucket.listKeys(null).forEachRemaining(key -> {
            try {
              log.debug("Deleting key: " + key.getName() +
                  " in bucket: " + bucket.getName() +
                  " in volume: " + volumeName);
              bucket.deleteKey(key.getName());
            } catch (IOException e) {
              log.error(
                  "cannot delete key : " + key.getName() +
                      " in bucket: " + bucket.getName() +
                      " in volume: " + volumeName +
                      " due to error: ", e);
            }
          });
        } catch (IOException e) {
          log.error("Could not list keys in bucket " + bucket.getName() +
              " in volume: " + volumeName);
        }
        try {
          volume.deleteBucket(bucket.getName());
        } catch (IOException e) {
          log.error(
              "cannot delete bucket : " + bucket.getName() + " in volume: " +
                  volumeName + " due to error: ", e);
        }
      });

      objectStore.deleteVolume(volumeName);
    } catch (IOException e) {
      log.error("Could not delete volume: " + volumeName + " due to error: ",
          e);
    }
  }

  void createLocalFileWithOverwrite(String path) {
    try {
      file = new File(path);
      file.getParentFile().mkdirs();
      if (!file.createNewFile()) {
        log.warn("Could not create file");
      }
      dataFileWriter = new DataFileWriter<>(datumWriter);
      log.debug("Successfully created local file : " + path);
    } catch (IOException e) {
      log.error("Tried to create file : " + path + " with no success :", e);
    }
  }

  void appendAvscHeader() {
    try {
      dataFileWriter.create(schema, file);
    } catch (IOException e) {
      log.error("Can not write header to the local file due to error: ", e);
    }
  }


}
