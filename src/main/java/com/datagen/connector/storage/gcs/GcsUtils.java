package com.datagen.connector.storage.gcs;

import com.datagen.config.ApplicationConfigs;
import com.datagen.model.Model;
import com.datagen.model.OptionsConverter;
import com.google.api.gax.paging.Page;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.*;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.Map;

import static com.datagen.config.ApplicationConfigs.DATA_HOME_DIRECTORY;

/**
 * Everything that is only related to GCS is set here and GCS connectors extends this class
 */
@Slf4j
public abstract class GcsUtils {

  protected final String projectId;
  protected final String bucketName;
  protected final String directoryName;
  protected final String objectNamePrefix;

  protected final String localDirectory;
  protected final String localFilePathForModelGeneration;

  protected final String serviceAccountKeyPath;

  protected final Storage storage;
  protected final String region;


  GcsUtils(Model model,
           Map<ApplicationConfigs, String> properties) {
    this.projectId = properties.get(ApplicationConfigs.GCS_PROJECT_ID);
    this.serviceAccountKeyPath =
        properties.get(ApplicationConfigs.GCS_ACCOUNT_KEY_PATH);
    this.region = properties.get(ApplicationConfigs.GCS_REGION).toUpperCase(
        Locale.ROOT);
    this.bucketName = (String) model.getTableNames()
        .get(OptionsConverter.TableNames.GCS_BUCKET);
    this.objectNamePrefix = (String) model.getTableNames()
        .get(OptionsConverter.TableNames.GCS_OBJECT_NAME);
    String directoryNotFormatted = (String) model.getTableNames()
        .get(OptionsConverter.TableNames.GCS_DIRECTORY);
    if (directoryNotFormatted.startsWith("/")) {
      directoryNotFormatted = directoryNotFormatted.substring(1);
    }
    if (directoryNotFormatted.endsWith("/")) {
      directoryNotFormatted = directoryNotFormatted.substring(0,
          directoryNotFormatted.length() - 1);
    }
    this.directoryName = directoryNotFormatted;
    this.localDirectory = (String) model.getTableNames()
        .get(OptionsConverter.TableNames.GCS_LOCAL_FILE_PATH);
    this.localFilePathForModelGeneration =
        properties.get(DATA_HOME_DIRECTORY) + "/model-gen/GCS/";

    if (serviceAccountKeyPath != null && !serviceAccountKeyPath.isBlank()) {
      System.setProperty("GOOGLE_APPLICATION_CREDENTIALS",
          serviceAccountKeyPath);
    }

    this.storage = StorageOptions.newBuilder().setProjectId(projectId).build()
        .getService();

  }

  /**
   * Close current GCS connection *
   */
  protected void closeGCS() {
    try {
      this.storage.close();
    } catch (Exception e) {
      log.error(" Unable to close GCS connection with error :", e);
    }

  }

  /**
   * Push a local file to ADLS (into a bucket and inside a directory defined in this class)
   *
   * @param localPath  of the local file to push
   * @param objectName of the object name on GCS
   * @return success of the operation
   */
  boolean pushLocalFileToGCS(
      String localPath,
      String objectName) {

    log.info(
        "Starting to push local file: {} to GCS in bucket {} inside directory: {} with name: {}",
        localPath, bucketName, directoryName, objectName);
    boolean success = false;

    try {
      BlobInfo blobInfo = BlobInfo.newBuilder(
          BlobId.of(bucketName, directoryName + "/" + objectName)).build();
      storage.createFrom(blobInfo, Paths.get(localPath));
    } catch (Exception e) {
      log.warn(
          "Could not upload local file: {} to GCS bucket: {} in directory: {}, due to error",
          localPath, bucketName, directoryName, e);
    }

    return success;
  }

  /**
   * Create a file on GCS and return its writer *
   *
   * @param objectName to create
   * @return
   */
  protected WriteChannel createFileOnGcs(String objectName) {
    String fullObjectName = directoryName + "/" + objectName;
    WriteChannel writeChannel = null;
    log.info("Starting to create object: {} in GCS bucket: {}",
        fullObjectName, bucketName);
    try {
      writeChannel = storage.writer(
          BlobInfo.newBuilder(BlobId.of(bucketName, fullObjectName)).build());
    } catch (Exception e) {
      log.error(
          "Cannot create object: {} in bucket: {} from GCS due to error: ",
          fullObjectName, bucketName, e);
    }
    return writeChannel;
  }

  /**
   * Write data to GCS using the writer provided *
   *
   * @param writeChannel pointing to the object to write to
   * @param content      to write
   */
  protected void writeData(WriteChannel writeChannel, String content) {
    try {
      writeChannel.write(
          ByteBuffer.wrap(content.getBytes(StandardCharsets.UTF_8)));
    } catch (IOException e) {
      log.warn("Could not write content to GCS due to error: ", e);
    }

  }

  /**
   * Read a file from GCS and write it locally
   *
   * @param localPath  where to write the file
   * @param objectName on GCS to read
   */
  void readFileFromGCS(String localPath, String objectName) {
    String fullObjectName = directoryName + "/" + objectName;
    log.info(
        "Starting to read blob: {} in GCS bucket: {} and write it to local directory: {}",
        fullObjectName, bucketName, localPath);
    try {
      Blob blob = storage.get(BlobId.of(bucketName, fullObjectName));
      blob.downloadTo(Paths.get(localPath));
    } catch (Exception e) {
      log.error("Cannot read blob: {} in bucket: {} from GCS due to error: ",
          fullObjectName, bucketName, e);
    }

  }

  /**
   * Create bucket in GCS if it does not exists *
   *
   * @return
   */
  boolean createBucketIfNotExists() {
    log.info("Will create bucket if not exists: {}", bucketName);
    boolean success = true;
    try {
      storage.create(
          BucketInfo.newBuilder(bucketName)
              .setStorageClass(StorageClass.STANDARD)
              .setLocation(region)
              .build());
    } catch (StorageException e) {
      if(e.getMessage().contains("succeeded")) {
        log.info("Bucket: {} already exists", bucketName);
      } else {
        log.warn("Cannot create bucket: {} due to error: ", bucketName, e);
      }
    } catch (Exception e) {
      log.warn("Cannot create bucket: {} due to error: ", bucketName, e);
      success = false;
    }
    return success;
  }

  /**
   * Delete all objects under a directory *
   *
   * @param fileNamePrefix to filter files to delete
   * @param suffix         to filter files to delete
   * @return if it is succesfull
   */
  boolean deleteAllObjects(String fileNamePrefix, String suffix) {
    boolean success = true;
    log.debug(
        "Start to delete all blobs starting with: {} and ending with: {} inside directory: {}",
        fileNamePrefix, suffix, directoryName);
    try {
      Page<Blob> blobs =
          storage.list(
              bucketName,
              Storage.BlobListOption.prefix(directoryName),
              Storage.BlobListOption.currentDirectory());

      for (Blob blob : blobs.iterateAll()) {
        String blobFileName = blob.getName();
        log.debug("Found blob: {} to check for deletion", blobFileName);
        if (blobFileName.contains("/")) {
          blobFileName =
              blobFileName.substring(blobFileName.lastIndexOf("/"));
        }
        if (blobFileName.startsWith(fileNamePrefix) &&
            blobFileName.endsWith(suffix)) {
          log.debug("Marked blob: {} to be deleted", blob.getName());
          blob.delete();
        }
      }


    } catch (Exception e) {
      log.warn(
          "Cannot delete files: {} under directory: {} in bucket: {} due to error: ",
          fileNamePrefix, directoryName, bucketName, e);
    }

    return success;
  }
}
