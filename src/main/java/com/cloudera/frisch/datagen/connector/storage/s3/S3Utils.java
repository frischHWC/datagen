package com.cloudera.frisch.datagen.connector.storage.s3;

import com.cloudera.frisch.datagen.config.ApplicationConfigs;
import com.cloudera.frisch.datagen.connector.storage.utils.FileUtils;
import com.cloudera.frisch.datagen.model.Model;
import com.cloudera.frisch.datagen.model.OptionsConverter;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;
import software.amazon.awssdk.transfer.s3.progress.LoggingTransferListener;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.Map;

import static com.cloudera.frisch.datagen.config.ApplicationConfigs.DATA_HOME_DIRECTORY;
import static software.amazon.awssdk.transfer.s3.SizeConstant.MB;

/**
 * Everything that is only related to S3 is set here and S3 connectors extends this class
 */
@Slf4j
public abstract class S3Utils {

  protected final String bucketName;
  protected final String directoryName;
  protected final String keyNamePrefix;

  protected final String localDirectoryName;
  protected final String localFilePathForModelGeneration;

  protected final String accesKeyId;
  protected final String accesKeySecret;

  protected final String region;
  protected final S3Client s3Client;
  protected final S3AsyncClient s3AsyncClient;
  protected final S3TransferManager transferManager;


  S3Utils(Model model,
          Map<ApplicationConfigs, String> properties) {
    this.bucketName = (String) model.getTableNames()
        .get(OptionsConverter.TableNames.S3_BUCKET);
    String directoryNotFormatted = (String) model.getTableNames()
        .get(OptionsConverter.TableNames.S3_DIRECTORY);
    if (directoryNotFormatted.startsWith("/")) {
      directoryNotFormatted = directoryNotFormatted.substring(1);
    }
    if (directoryNotFormatted.endsWith("/")) {
      directoryNotFormatted = directoryNotFormatted.substring(0,
          directoryNotFormatted.length() - 1);
    }
    this.directoryName = directoryNotFormatted;
    this.keyNamePrefix = (String) model.getTableNames()
        .get(OptionsConverter.TableNames.S3_KEY_NAME);
    this.localDirectoryName = (String) model.getTableNames()
        .get(OptionsConverter.TableNames.S3_LOCAL_FILE_PATH);
    this.accesKeyId = properties.get(ApplicationConfigs.S3_ACCESS_KEY_ID);
    this.accesKeySecret =
        properties.get(ApplicationConfigs.S3_ACCESS_KEY_SECRET);
    this.region = properties.get(ApplicationConfigs.S3_REGION);

    this.localFilePathForModelGeneration = properties.get(DATA_HOME_DIRECTORY) + "/model-gen/s3/";

    AwsCredentialsProvider awsCredentialsProvider =
        StaticCredentialsProvider.create(
            AwsBasicCredentials.create(accesKeyId, accesKeySecret));

    this.s3Client = S3Client.builder()
        .region(Region.of(region))
        .credentialsProvider(awsCredentialsProvider)
        .build();

    this.s3AsyncClient = S3AsyncClient.crtBuilder()
        .credentialsProvider(awsCredentialsProvider)
        .region(Region.of(region))
        .targetThroughputInGbps(20.0)
        .minimumPartSizeInBytes(8 * MB)
        .build();

    this.transferManager = S3TransferManager.builder()
        .s3Client(s3AsyncClient)
        .build();

  }

  /**
   * Close current AWS S3 connection *
   */
  protected void closeS3() {
    try {
      this.transferManager.close();
      this.s3Client.close();
      this.s3AsyncClient.close();
    } catch (Exception e) {
      log.error(" Unable to close S3 Filesystem file with error :", e);
    }

  }

  boolean pushLocalFileToS3(String localPath,
                            String keyName) {
    return pushLocalFileToS3(localPath, keyName, false);
  }

  /**
   * Push a local file to S3 (into bucket defined in this class)
   *  if a file is > 256 MB then a multi part upload is used, otherwise a simple PUT is used
   * @param localPath of the local file to push
   * @param keyName of the key on S3
   * @param computeChecksum if it needs to checksum file and compare with one returned by S3 to ensure that file was well uplaoded
   * @return success of the operation
   */
  boolean pushLocalFileToS3(
      String localPath,
      String keyName,
      boolean computeChecksum) {

    String fullKeyName = directoryName + "/" + keyName;
    log.info("Starting to push local file: {} to S3 in bucket {} with key: {}", localPath, bucketName, fullKeyName);
    String checksum = "";
    String checksumFromS3 = "";
    boolean largeFile = false;
    boolean success = true;

    try {
      // Files above 256 MB are considered as large (this may change in the future)
      if (Files.size(Paths.get(localPath)) > 1024 * 1024 * 256) {
        largeFile = true;
      }
    } catch (IOException e) {
      log.warn("Cannot determine size of local file: {} with error: ",
          localPath, e);
    }

    if (computeChecksum) {
      try {
        byte[] hash = MessageDigest.getInstance("MD5")
            .digest(Files.readAllBytes(Paths.get(localPath)));
        checksum = new BigInteger(1, hash).toString(16);
      } catch (Exception e) {
        log.warn("Cannot compute checksum due to error: ", e);
      }
    }

    if (largeFile) {
      try {
        UploadFileRequest uploadFileRequest = UploadFileRequest.builder()
            .putObjectRequest(b -> b.bucket(bucketName).key(fullKeyName))
            .addTransferListener(LoggingTransferListener.create())
            .source(Paths.get(localPath))
            .build();

        FileUpload fileUpload = transferManager.uploadFile(uploadFileRequest);
        checksumFromS3 =
            fileUpload.completionFuture().join().response().eTag();
        log.debug("Finished to upload file to S3 with checksum: {}",
            checksumFromS3);
      } catch (S3Exception e) {
        log.warn("Could not upload file to bucket {} as key {}",
            bucketName, keyName);
        success = false;
      }

    } else {
      try {
        PutObjectRequest putOb = PutObjectRequest.builder()
            .bucket(bucketName)
            .key(fullKeyName)
            .build();

        checksumFromS3 =
            s3Client.putObject(putOb, RequestBody.fromFile(new File(localPath)))
                .eTag();
        log.debug("Finished to upload file to S3 with checksum: {}",
            checksumFromS3);
      } catch (S3Exception e) {
        log.warn("Could not upload file to bucket {} as key {}",
            bucketName, fullKeyName);
        success = false;
      }
    }

    if (computeChecksum) {
      if (checksum.equalsIgnoreCase(checksumFromS3)) {
        log.warn(
            "Checksum are not equals, file may have been corrupted during transfer");
      }
    }

    return success;
  }

  /**
   * Read a file from S3 and write it locally
   * @param localPath where to write the file
   * @param keyName on S3 to read
   */
  void readFileFromS3(String localPath, String keyName) {
    String fullKeyName = directoryName + "/" + keyName;
    log.info("Starting to read key: {} in S3 bucket: {} and write it to local directory: {}", fullKeyName, bucketName, localPath);
    try {
      GetObjectRequest objectRequest = GetObjectRequest
          .builder()
          .key(fullKeyName)
          .bucket(bucketName)
          .build();

      ResponseBytes<GetObjectResponse> objectBytes =
          s3Client.getObjectAsBytes(objectRequest);
      byte[] data = objectBytes.asByteArray();

      FileOutputStream fileOutputStream = FileUtils.createLocalFileAsOutputStream(localPath);
      fileOutputStream.write(data);
      fileOutputStream.close();
    } catch (Exception e) {
      log.error("Cannot read file: {} in bucket: {} from S3 due to error: ", fullKeyName, bucketName, e);
    }

  }

  /**
   * Create a bucket in S3 iff it does not exists and wait for it to be created
   * @return success of the operation
   */
  boolean createBucketIfNotExists() {
    log.info("Will create bucket if not exists: {}", bucketName);
    boolean success = true;
    try {
      S3Waiter s3Waiter = s3Client.waiter();
      CreateBucketRequest bucketRequest = CreateBucketRequest.builder()
          .bucket(bucketName)
          .build();

      s3Client.createBucket(bucketRequest);
      HeadBucketRequest bucketRequestWait = HeadBucketRequest.builder()
          .bucket(bucketName)
          .build();

      WaiterResponse<HeadBucketResponse> waiterResponse =
          s3Waiter.waitUntilBucketExists(bucketRequestWait);
      waiterResponse.matched().exception().ifPresent(t -> log.warn("Exception during bucket creation: {} ", t.getMessage()));
    } catch (BucketAlreadyExistsException|
    BucketAlreadyOwnedByYouException e) {
      log.info("Bucket {} already exists", bucketName);
    } catch (S3Exception e) {
      log.warn("Cannot create bucket: {} due to error: ", bucketName, e);
      success = false;
    }
    return success;
  }

  /**
   * Delete all files under a directory *
   *
   * @param fileNamePrefix to filter files to delete
   * @param suffix         to filter files to delete
   * @return if it is succesfull
   */
  boolean deleteAllfiles(String fileNamePrefix, String suffix) {
    boolean success = true;
    log.debug(
        "Start to delete all files starting with: {} and ending with: {} inside directory: {}",
        fileNamePrefix, suffix, directoryName);
    try {
      s3Client.listObjects(
              ListObjectsRequest.builder().bucket(bucketName)
                  .prefix(directoryName)
                  .build())
          .contents()
          .stream().filter(s -> {
            String keyNameWithoutDir = s.key();
            if(keyNameWithoutDir.contains("/")) {
              keyNameWithoutDir = keyNameWithoutDir.substring(keyNameWithoutDir.lastIndexOf("/")+1);
            }
            if(keyNameWithoutDir.startsWith(fileNamePrefix) && keyNameWithoutDir.endsWith(suffix)) {
              return true;
            } else {
              return false;
            }
          })
          .forEach(k -> s3Client.deleteObject(
              DeleteObjectRequest.builder().bucket(bucketName).key(k.key())
                  .build()));

    } catch (Exception e) {
      log.warn(
          "Cannot delete files: {} under directory: {} in bucket: {} due to error: ",
          fileNamePrefix, directoryName, bucketName, e);
    }

    return success;
  }
}
