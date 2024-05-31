package com.datagen.connector.storage.utils;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

@Slf4j
public class FileUtils {

  /**
   * Create a local file and all required parent directories if needed
   * and return it as an outputStream
   * @param path to the file
   * @return outputstream to the file
   */
  public static FileOutputStream createLocalFileAsOutputStream(String path) {
    log.info("Creating local file: {}", path);
    FileOutputStream outputStream = null;
    try {
      File file = new File(path);
      file.getParentFile().mkdirs();
      file.setReadable(true, true);
      file.setWritable(true, true);
      file.setExecutable(true, true);
      file.createNewFile();
      outputStream = new FileOutputStream(path, false);
      log.info("Successfully created local file : " + path);
    } catch (IOException e) {
      log.error("Tried to create file : " + path + " with no success :", e);
    }
    return outputStream;
  }

  /**
   * Create a local file and all required parent directories if needed
   * and return it as a @java.io.File
   * @param path to the file
   * @return File opened
   */
  public static File createLocalFileAsFile(String path) {
    File file = null;
    try {
      file = new File(path);
      file.getParentFile().mkdirs();
      file.delete();
      file.setReadable(true, true);
      file.setWritable(true, true);
      file.setExecutable(true, true);
      file.createNewFile();
    } catch (IOException e) {
      log.error(
          "Failed to create local file : " + path + " with no success :",
          e);
    }
    return file;
  }

  /**
   * Create a local file and all required parent directories if needed
   * and return it as a @java.io.File
   * @param path to the file
   * @return File opened
   */
  public static void createLocalFile(String path) {
    try {
      File file = new File(path);
      file.getParentFile().mkdirs();
      file.delete();
      file.setReadable(true, true);
      file.setWritable(true, true);
      file.setExecutable(true, true);
      file.createNewFile();
    } catch (IOException e) {
      log.error(
          "Failed to create local file : " + path + " with no success :",
          e);
    }
  }

  /**
   * Creates a directory locally
   * @param path
   */
  public static void createLocalDirectory(String path) {
    try {
      File file = new File(path);
      file.getParentFile().mkdirs();
      file.mkdirs();
    } catch (Exception e) {
      log.error("Unable to create local directory of : " + path + " due to error: ",
          e);
    }
  }

  /**
   *  Move a file locally
   * @param pathSrc
   * @param pathDest
   */
  public static void moveLocalFile(String pathSrc, String pathDest) {
    try {
      File fileDest = new File(pathDest);
      fileDest.getParentFile().mkdirs();
      new File(pathSrc).renameTo(fileDest);
    } catch (Exception e) {
      log.error("Unable to move file from : {} to {} due to error: ", pathSrc,
          pathDest, e);
    }
  }

  /**
   * Delete a file locally if it exists
   * @param path
   */
  public static void deleteLocalFile(String path) {
    try {
      File file = new File(path);
      if(file.exists()) {
        file.delete();
      }
      log.debug("Successfully delete local file : " + path);
    } catch (Exception e) {
      log.error("Tried to delete file : " + path + " with no success :", e);
    }
  }

  /**
   * Delete all local files in a specified directory with a specified extension and a prefix
   * @param directory where to search files
   * @param prefix to filter files
   * @param extension to filter files
   */
  public static void deleteAllLocalFiles(String directory, String prefix,
                                         String extension) {
    File folder = new File(directory);
    File[] files =
        folder.listFiles((dir, f) -> f.matches(prefix + ".*[.]" + extension));
    if (files != null) {
      for (File f : files) {
        log.debug("Will delete local file: " + f);
        if (f.isFile() && !f.delete()) {
          log.warn("Could not delete file: " + f);
        }
      }
    }
  }

}
