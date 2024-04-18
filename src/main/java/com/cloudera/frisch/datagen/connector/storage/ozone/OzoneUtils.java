package com.cloudera.frisch.datagen.connector.storage.ozone;


import com.cloudera.frisch.datagen.config.ApplicationConfigs;
import com.cloudera.frisch.datagen.connector.storage.utils.FileUtils;
import com.cloudera.frisch.datagen.model.Model;
import com.cloudera.frisch.datagen.model.OptionsConverter;
import com.cloudera.frisch.datagen.utils.KerberosUtils;
import com.cloudera.frisch.datagen.utils.Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.*;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.exceptions.OMException;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Map;

@Slf4j
public class OzoneUtils {

  protected OzoneClient ozClient;
  protected ObjectStore objectStore;

  protected OzoneVolume volume;
  protected OzoneBucket bucket;

  protected final String volumeName;
  protected final String bucketName;
  protected final String keyNamePrefix;

  protected final ReplicationFactor replicationFactor;
  protected final ReplicationConfig replicationConfig;
  protected final OzoneConfiguration config;
  protected final String localFileTempDir;
  protected Boolean useKerberos;

  OzoneUtils(Model model,
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

    this.useKerberos = Boolean.parseBoolean(
        properties.get(ApplicationConfigs.OZONE_AUTH_KERBEROS));
    this.config = new OzoneConfiguration();
    Utils.setupHadoopEnv(config, properties);
    this.replicationConfig = ReplicationConfig.parse(ReplicationType.RATIS, String.valueOf(replicationFactor), config);

    if (useKerberos) {
      KerberosUtils.loginUserWithKerberos(
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

  protected void closeOzone() {
    try {
      ozClient.close();
    } catch (IOException e) {
      log.warn("Cannot close connection to ozone due to error: ", e);
    } finally {
      if (useKerberos) {
        KerberosUtils.logoutUserWithKerberos();
      }
    }
  }

  /**
   * Push a local file to Ozone and deletes it after it has been pushed *
   * @param localPath to the file to upload
   * @param keyName to set in ozone
   */
  protected void pushKeyToOzone(String localPath, String keyName) {
    try {
      byte[] dataToWrite = Files.readAllBytes(
          java.nio.file.Path.of(localPath));
      OzoneOutputStream os = bucket.createKey(keyName, dataToWrite.length,
          replicationConfig, Collections.emptyMap());
      os.write(dataToWrite);
      os.getOutputStream().flush();
      os.close();
      FileUtils.deleteLocalFile(localPath);
    } catch (IOException e) {
      log.error(
          "Could not write row to Ozone volume: {} bucket: {}, key: {} ; error: ",
          volumeName, bucketName, keyName, e);
    }
  }

  /**
   * Create a bucket if it does not exist
   * In case it exists, it just skips the error and log that bucket already exists
   *
   */
  protected void createBucketIfNotExist() {
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
  protected void createVolumeIfItDoesNotExist() {
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
   * Delete all keys inside a bucket
   * This is helpful as Ozone does not provide natively this type of function
   *
   */
  protected void deleteEverythingUnderABucket() {
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
      });

    } catch (IOException e) {
      log.error("Could not delete everything under bucket: " + bucketName + " due to error: ",
          e);
    }
  }


}
