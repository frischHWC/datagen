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


import com.cloudera.frisch.datagen.connector.ConnectorInterface;
import com.cloudera.frisch.datagen.model.type.Field;
import com.cloudera.frisch.datagen.utils.Utils;
import com.cloudera.frisch.datagen.config.ApplicationConfigs;
import com.cloudera.frisch.datagen.model.Model;
import com.cloudera.frisch.datagen.model.OptionsConverter;
import com.cloudera.frisch.datagen.model.Row;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.*;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.exceptions.OMException;

import java.io.File;
import java.io.FileOutputStream;
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
public class OzoneJsonConnector implements ConnectorInterface {

  private OzoneClient ozClient;
  private ObjectStore objectStore;
  private OzoneVolume volume;
  private final String volumeName;
  private final String bucketName;
  private final String keyNamePrefix;
  private final ReplicationFactor replicationFactor;
  private final String localFileTempDir;
  private Boolean useKerberos;

  private FileOutputStream outputStream;
  private final String lineSeparator;
  private final Boolean oneFilePerIteration;
  private final Model model;
  private int counter;
  private OzoneBucket bucket;


  public OzoneJsonConnector(Model model,
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
    this.lineSeparator = System.getProperty("line.separator");
    this.useKerberos = Boolean.parseBoolean(
        properties.get(ApplicationConfigs.OZONE_AUTH_KERBEROS));

    OzoneConfiguration config = new OzoneConfiguration();
    Utils.setupHadoopEnv(config, properties);

    if (useKerberos) {
      Utils.loginUserWithKerberos(
          properties.get(ApplicationConfigs.OZONE_AUTH_KERBEROS_USER),
          properties.get(ApplicationConfigs.OZONE_AUTH_KERBEROS_KEYTAB),
          config);
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
        Utils.deleteAllLocalFiles(localFileTempDir, keyNamePrefix,
            "json");

        if (!oneFilePerIteration) {
          createLocalFileWithOverwrite(
              localFileTempDir + keyNamePrefix + ".json");
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
        outputStream.close();
        // Send local file to Ozone
        String keyName = keyNamePrefix + ".json";
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
      Utils.deleteAllLocalFiles(localFileTempDir, keyNamePrefix, "json");
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
        keyNamePrefix + "-" + String.format("%010d", counter) + ".json";
    // Write to local file
    if (oneFilePerIteration) {
      createLocalFileWithOverwrite(localFileTempDir + keyName);
      counter++;
    }
    rows.stream().map(Row::toJSON).forEach(r -> {
      try {
        outputStream.write(r.getBytes());
        outputStream.write(lineSeparator.getBytes());
      } catch (IOException e) {
        log.error("Could not write row: " + r + " to file: " +
            outputStream.getChannel());
      }
    });
    if (oneFilePerIteration) {
      try {
        outputStream.close();
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
      Utils.deleteAllLocalFiles(localFileTempDir, keyNamePrefix, "json");
    }

  }

  @Override
  public Model generateModel(Boolean deepAnalysis) {
    LinkedHashMap<String, Field> fields = new LinkedHashMap<String, Field>();
    Map<String, List<String>> primaryKeys = new HashMap<>();
    Map<String, String> tableNames = new HashMap<>();
    Map<String, String> options = new HashMap<>();
    // TODO : Implement logic to create a model with at least names, pk, options and column names/types
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

  private void createLocalFileWithOverwrite(String path) {
    try {
      File file = new File(path);
      file.getParentFile().mkdirs();
      if (!file.createNewFile()) {
        log.warn("Could not create file: {}", path);
      }
      outputStream = new FileOutputStream(path, false);
      log.debug("Successfully created local file : " + path);

    } catch (IOException e) {
      log.error(
          "Tried to create Parquet local file : " + path + " with no success :",
          e);
    }
  }


}
