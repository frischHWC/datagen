package com.datagen.connector.storage.adls;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.azure.storage.blob.options.BlobUploadFromFileOptions;
import com.azure.storage.file.datalake.*;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import com.azure.storage.file.datalake.models.PathItem;
import com.datagen.config.ApplicationConfigs;
import com.datagen.model.Model;
import com.datagen.model.OptionsConverter;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

import static com.datagen.config.ApplicationConfigs.DATA_HOME_DIRECTORY;

/**
 * Everything that is only related to ADLS is set here and ADLS connectors extends this class
 */
@Slf4j
public abstract class AdlsUtils {

  protected final String accountName;
  protected final String accountType;

  protected final String containerName;
  protected final String directoryName;
  protected final String fileNamePrefix;

  protected final String localDirectory;
  protected final String localFilePathForModelGeneration;

  protected final String sasToken;
  protected final DataLakeServiceClient dataLakeServiceClient;
  protected final DataLakeFileSystemClient dataLakeFileSystemClient;
  protected final BlobServiceClient blobServiceClient;
  protected final BlobContainerClient blobContainerClient;
  protected DataLakeDirectoryClient directoryClient;

  protected final ParallelTransferOptions parallelTransferOptions;
  protected final long blockSize;
  protected final int maxConcurency;
  protected final long maxUploadSizePerRequest;


  AdlsUtils(Model model,
            Map<ApplicationConfigs, String> properties) {
    this.containerName = (String) model.getTableNames()
        .get(OptionsConverter.TableNames.ADLS_CONTAINER);
    this.fileNamePrefix = (String) model.getTableNames()
        .get(OptionsConverter.TableNames.ADLS_FILE_NAME);
    String directoryNotFormatted = (String) model.getTableNames()
        .get(OptionsConverter.TableNames.ADLS_DIRECTORY);
    if (directoryNotFormatted.startsWith("/")) {
      directoryNotFormatted = directoryNotFormatted.substring(1);
    }
    if (directoryNotFormatted.endsWith("/")) {
      directoryNotFormatted = directoryNotFormatted.substring(0,
          directoryNotFormatted.length() - 1);
    }
    this.directoryName = directoryNotFormatted;
    this.localDirectory = (String) model.getTableNames()
        .get(OptionsConverter.TableNames.ADLS_LOCAL_FILE_PATH);
    this.localFilePathForModelGeneration = properties.get(DATA_HOME_DIRECTORY) + "/model-gen/azure/";

    this.sasToken = properties.get(ApplicationConfigs.ADLS_SAS_TOKEN);
    this.accountName =
        properties.get(ApplicationConfigs.ADLS_ACCOUNT_NAME);
    this.accountType = properties.get(ApplicationConfigs.ADLS_ACCOUNT_TYPE);

    this.blockSize = Long.parseLong((String) model.getOptionsOrDefault(
        OptionsConverter.Options.ADLS_BLOCK_SIZE));
    this.maxConcurency = Integer.parseInt((String) model.getOptionsOrDefault(
        OptionsConverter.Options.ADLS_MAX_CONCURRENCY));
    this.maxUploadSizePerRequest = Long.parseLong(
        (String) model.getOptionsOrDefault(
            OptionsConverter.Options.ADLS_MAX_UPLOAD_SIZE));

    if (this.accountType.equalsIgnoreCase("dfs")) {
      this.dataLakeServiceClient = new DataLakeServiceClientBuilder()
          .endpoint("https://" + accountName + ".dfs.core.windows.net")
          .sasToken(sasToken)
          .buildClient();

      this.dataLakeFileSystemClient =
          dataLakeServiceClient.getFileSystemClient(containerName);
      this.directoryClient =
          dataLakeFileSystemClient.getDirectoryClient(directoryName);
      this.blobServiceClient = null;
      this.blobContainerClient = null;
      this.parallelTransferOptions = null;
    } else if (this.accountType.equalsIgnoreCase("blob")) {
      this.blobServiceClient = new BlobServiceClientBuilder()
          .endpoint("https://" + accountName + ".blob.core.windows.net")
          .sasToken(sasToken)
          .buildClient();
      this.blobContainerClient =
          blobServiceClient.getBlobContainerClient(containerName);
      this.parallelTransferOptions = new ParallelTransferOptions()
          .setBlockSizeLong(this.blockSize)
          .setMaxConcurrency(this.maxConcurency)
          .setMaxSingleUploadSizeLong(this.maxUploadSizePerRequest);
      this.dataLakeServiceClient = null;
      this.dataLakeFileSystemClient = null;
    } else {
      this.dataLakeServiceClient = null;
      this.dataLakeFileSystemClient = null;
      this.blobServiceClient = null;
      this.blobContainerClient = null;
      this.parallelTransferOptions = null;
      log.error(
          "Can not initialize Azure client as it account type is wrong, check property azure.account.type in properties file, it should be dfs or blob and not: {}",
          this.accountType);
    }

  }

  /**
   * Push a local file to ADLS (into a container and inside a directory defined in this class)
   *
   * @param localPath of the local file to push
   * @param fileName  of the file name on ADLS
   * @return success of the operation
   */
  boolean pushLocalFileToADLS(
      String localPath,
      String fileName) {
    String fullFileName = directoryName + "/" + fileName;

    log.info(
        "Starting to push local file: {} to ADLS in container {} inside directory: {} with name: {}",
        localPath, containerName, directoryName, fileName);
    boolean success = false;

    try {
      if (this.accountType.equalsIgnoreCase("dfs")) {
        DataLakeFileClient fileClient =
            this.directoryClient.getFileClient(fullFileName);

        fileClient.uploadFromFile(localPath);
      } else if (this.accountType.equalsIgnoreCase("blob")) {
        BlobUploadFromFileOptions options =
            new BlobUploadFromFileOptions(localPath);
        options.setParallelTransferOptions(parallelTransferOptions);
        blobContainerClient.getBlobClient(fullFileName)
            .uploadFromFileWithResponse(options, null, null);
      }
      success = true;
    } catch (Exception e) {
      if (e.getMessage().contains("PathAlreadyExists")) {
        log.warn(
            "Could not upload local file: {} to ADLS container: {} in directory: {}, because it already exists:" +
                " Delete it before or set DELETE_PREVIOUS: true in model file");
      } else {
        log.warn(
            "Could not upload local file: {} to ALDS container: {} in directory: {}, due to error",
            localPath, containerName, directoryName, e);
      }
    }

    return success;
  }

  /**
   * Read a file from ADLS and write it locally
   *
   * @param localPath where to write the file
   * @param fileName  on ADLS to read
   */
  void readFileFromADLS(String localPath, String fileName) {
    log.info(
        "Starting to read file: {} in ADLS container: {} and write it to local directory: {}",
        directoryName + fileName, containerName, localPath);
    try {
      if (this.accountType.equalsIgnoreCase("dfs")) {
        DataLakeFileClient fileClient =
            this.directoryClient.getFileClient(fileName);
        fileClient.readToFile(localPath, true);
      } else if (this.accountType.equalsIgnoreCase("blob")) {
        blobContainerClient.getBlobClient(fileName).downloadToFile(localPath);
      }
    } catch (Exception e) {
      log.error("Cannot read file: {} in container: {} from S3 due to error: ",
          directoryName + fileName, containerName, e);
    }

  }

  /**
   * Create a directory in ADLS if it does not exists
   * There are no concept of directory if working with blobs
   *
   * @return success of the operation
   */
  boolean createDirectoryIfNotExists() {
    boolean success = true;
    if (this.accountType.equalsIgnoreCase("dfs")) {
      log.info("Will create directory if it does not exists: {}",
          directoryName);
      try {
        if (!directoryClient.exists()) {
          this.directoryClient =
              this.dataLakeFileSystemClient.createDirectory(directoryName,
                  false);
        }
      } catch (Exception e) {
        log.warn("Cannot create directory: {} due to error: ", directoryName,
            e);
        success = false;
      }
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
      if (this.accountType.equalsIgnoreCase("dfs")) {
        ListPathsOptions options =
            new ListPathsOptions().setPath(directoryName);
        PagedIterable<PathItem> pagedIterable =
            dataLakeFileSystemClient.listPaths(options, null);
        java.util.Iterator<PathItem> iterator = pagedIterable.iterator();
        while (iterator.hasNext()) {
          PathItem item = iterator.next();
          log.debug("Found file: {} to check for deletion", item.getName());
          if (!item.isDirectory() && item.getName().endsWith(suffix) &&
              item.getName().startsWith(directoryName + "/" + fileNamePrefix)) {
            log.debug("Found file: {} to delete", item.getName());
            directoryClient.getFileClient(
                    item.getName().substring(item.getName().lastIndexOf("/") + 1))
                .delete();
          }
        }
      } else if (this.accountType.equalsIgnoreCase("blob")) {
        ListBlobsOptions options = new ListBlobsOptions()
            .setPrefix(fileNamePrefix);

        blobContainerClient.listBlobsByHierarchy("/", options, null)
            .forEach(b -> {
              if (b.getName().endsWith(suffix)) {
                log.debug("Found blob: {} to delete", b.getName());
                blobContainerClient.getBlobClient(b.getName()).delete();
              }
            });
      }
    } catch (Exception e) {
      log.warn(
          "Cannot delete files: {} under directory: {} in container: {} due to error: ",
          fileNamePrefix, directoryName, containerName, e);
    }

    return success;
  }
}
