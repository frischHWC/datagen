package com.cloudera.frisch.datagen.connector.storage.hdfs;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;

@Slf4j
public abstract class HdfsUtils {


  /**
   * Delete all HDFS files in a specified directory with a specified extension and a name
   *
   * @param directory
   * @param extension
   */
  void deleteAllHdfsFiles(FileSystem fileSystem, String directory,
                                        String name, String extension) {
    try {
      RemoteIterator<LocatedFileStatus> fileiterator =
          fileSystem.listFiles(new Path(directory), false);
      while (fileiterator.hasNext()) {
        LocatedFileStatus file = fileiterator.next();
        if (file.getPath().getName().matches(name + ".*[.]" + extension)) {
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
   * @param fileSystem
   * @param path
   */
  void createHdfsDirectory(FileSystem fileSystem, String path) {
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
   * @param fileSystem
   * @param path
   */
  void deleteHdfsFile(FileSystem fileSystem, String path) {
    try {
      fileSystem.delete(new Path(path), true);
      log.debug("Successfully deleted hdfs file : " + path);
    } catch (IOException e) {
      log.error("Tried to delete hdfs file : " + path + " with no success :",
          e);
    }
  }
}
