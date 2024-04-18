package com.cloudera.frisch.datagen.connector.storage.hdfs;

import com.cloudera.frisch.datagen.config.ApplicationConfigs;
import com.cloudera.frisch.datagen.model.Model;
import com.cloudera.frisch.datagen.model.OptionsConverter;
import com.cloudera.frisch.datagen.utils.KerberosUtils;
import com.cloudera.frisch.datagen.utils.Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

/**
 * All hDFS connectors should extends this utils class that provides: connection to HDFS & a set of basic functions *
 */
@Slf4j
public abstract class HdfsUtils {

  protected FileSystem fileSystem;
  protected final String hdfsUri;
  protected final Configuration configuration;
  protected final short replicationFactor;
  protected final Boolean useKerberos;

  protected final String directoryName;
  protected final String fileName;


  HdfsUtils(Model model,
            Map<ApplicationConfigs, String> properties) {

    // If using an HDFS connector, we want it to use the Hive HDFS File path and not the Hdfs file path
    if (properties.get(ApplicationConfigs.HDFS_FOR_HIVE) != null
        && properties.get(ApplicationConfigs.HDFS_FOR_HIVE)
        .equalsIgnoreCase("true")) {
      this.directoryName = (String) model.getTableNames()
          .get(OptionsConverter.TableNames.HIVE_HDFS_FILE_PATH);
    } else {
      this.directoryName = (String) model.getTableNames()
          .get(OptionsConverter.TableNames.HDFS_FILE_PATH);
    }
    log.debug("HDFS connector will generates data into HDFS directory: " +
        this.directoryName);

    this.fileName = (String) model.getTableNames()
        .get(OptionsConverter.TableNames.HDFS_FILE_NAME);
    this.replicationFactor = (short) model.getOptionsOrDefault(
        OptionsConverter.Options.HDFS_REPLICATION_FACTOR);
    this.hdfsUri = properties.get(ApplicationConfigs.HDFS_URI);
    this.useKerberos = Boolean.parseBoolean(
        properties.get(ApplicationConfigs.HDFS_AUTH_KERBEROS));

    this.configuration = new Configuration();
    configuration.set("dfs.replication", String.valueOf(replicationFactor));
    Utils.setupHadoopEnv(configuration, properties);

    // Set all kerberos if needed (Note that connection will require a user and its appropriate keytab with right privileges to access folders and files on HDFSCSV)
    if (useKerberos) {
      KerberosUtils.loginUserWithKerberos(
          properties.get(ApplicationConfigs.HDFS_AUTH_KERBEROS_USER),
          properties.get(ApplicationConfigs.HDFS_AUTH_KERBEROS_KEYTAB),
          configuration);
    }

    try {
      this.fileSystem = FileSystem.get(URI.create(hdfsUri), configuration);
    } catch (IOException e) {
      log.error("Could not access to HDFSCSV !", e);
    }

  }

  /**
   * Close current HDFS connection *
   */
  protected void closeHDFS() {
    try {
      fileSystem.close();
      if (useKerberos) {
        KerberosUtils.logoutUserWithKerberos();
      }
    } catch (IOException e) {
      log.error(" Unable to close HDFS Filesystem file with error :", e);
    }

  }

  /**
   * Create a file on HDFS with overwrite and return it as an output stream *
   *
   * @param path
   */
  protected FSDataOutputStream createFileWithOverwrite(String path) {
    FSDataOutputStream fsDataOutputStream = null;
    try {
      deleteHdfsFile(path);
      fsDataOutputStream = fileSystem.create(new Path(path), replicationFactor);
      log.debug("Successfully created hdfs file : " + path);
    } catch (IOException e) {
      log.error("Tried to create hdfs file : " + path + " with no success :",
          e);
    }
    return fsDataOutputStream;
  }

  /**
   * Delete all HDFS files in a specified directory with a specified extension and a name
   *
   * @param directory where to delete files
   * @param prefix    of files to delete
   * @param extension of files to delete
   */
  protected void deleteAllHdfsFiles(String directory,
                                    String prefix, String extension) {
    try {
      RemoteIterator<LocatedFileStatus> fileiterator =
          fileSystem.listFiles(new Path(directory), false);
      while (fileiterator.hasNext()) {
        LocatedFileStatus file = fileiterator.next();
        if (file.getPath().getName().matches(prefix + ".*[.]" + extension)) {
          log.debug("Will delete HDFS file: " + file.getPath());
          fileSystem.delete(file.getPath(), false);
        }
      }
    } catch (Exception e) {
      log.warn("Could not delete files under " + directory + " due to error: ",
          e);
    }
  }

  /**
   * Creates a directory on HDFS
   *
   * @param path of directory
   */
  protected void createHdfsDirectory(String path) {
    try {
      fileSystem.mkdirs(new Path(path));
    } catch (IOException e) {
      log.error(
          "Unable to create hdfs directory of : " + path + " due to error: ",
          e);
    }
  }


  /**
   * Delete an HDFS file
   *
   * @param path to file
   */
  void deleteHdfsFile(String path) {
    try {
      fileSystem.delete(new Path(path), true);
      log.debug("Successfully deleted hdfs file : " + path);
    } catch (IOException e) {
      log.error("Tried to delete hdfs file : " + path + " with no success :",
          e);
    }
  }
}
