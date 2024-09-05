package com.datagen.connector.storage.utils;

import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.file.Files;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@Slf4j
public class FileUtils {

  /**
   * Create a local file and all required parent directories if needed
   * and return it as an outputStream
   * @param path to the file
   * @return outputstream to the file
   */
  public static FileOutputStream createLocalFileAsOutputStream(String path)  {
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
      //throw new CommandException("Cannot create local file: " +  path, CommandException.ExceptionType.FILE_NOT_FOUND);
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
   * Creates a directory locally
   * @param path
   */
  public static void createLocalDirectoryWithStrongRights(String path) {
    try {
      File file = new File(path);
      file.getParentFile().mkdirs();
      file.mkdirs();
      file.setReadable(true, true);
      file.setWritable(true, true);
      file.setExecutable(true, true);
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

  /**
   * List all files under a directory
   * @param path
   */
  public static File[] listLocalFiles(String path) {
    try {
      File file = new File(path);
      if(file.exists()) {
        return file.listFiles();
      }
    } catch (Exception e) {
      log.error("Tried to list all files under : " + path + " with no success :", e);
    }
    return null;
  }

  /**
   * Check a file (or directory) exists
   * @param path
   */
  public static boolean checkLocalFileExists(String path) {
    try {
      File file = new File(path);
      return file.exists();
    } catch (Exception e) {
      log.error("Tried to check file: {} exist with no success :", path, e);
    }
    return false;
  }

  /**
   * List all files under a directory
   * @param path
   */
  public static File[] listLocalFilesWithPrefix(String path, String prefix) {
    try {
      File file = new File(path);
      if(file.exists()) {
        return file.listFiles(f -> f.getName().startsWith(prefix));
      }
    } catch (Exception e) {
      log.error("Tried to list all files under : " + path + " with no success :", e);
    }
    return null;
  }


  /**
   * Return content of a file
   * @param path
   */
  public static List<String> getfileContent(String path) {
    try {
      File file = new File(path);
      if(file.exists()) {
        return Files.readAllLines(file.toPath());
      }
    } catch (Exception e) {
      log.error("Tried to read file content : {} with no success :", path, e);
    }
    return null;
  }

  /**
   * Return content of a file
   * @param path
   */
  public static String getfileContentInString(String path) {
    try {
      File file = new File(path);
      if(file.exists()) {
        return Files.readString(file.toPath());
      }
    } catch (Exception e) {
      log.error("Tried to read file content : {} with no success :", path, e);
    }
    return null;
  }

  /**
   * From a list of files, read them as byte array and write them in a ZipOutputStream, before being rendered as a byteArrayOutputStream
   * @param filesToZip
   * @return
   */
  public static ByteArrayOutputStream zipFilesIntoAByteArray(List<File> filesToZip) {
    var baOutputStream = new ByteArrayOutputStream();
    var zipOut = new ZipOutputStream(baOutputStream);
    try {
      for (File fileToZip : filesToZip) {
        log.debug("Adding file to zip: {}", fileToZip);
        var fileInputStream = new FileInputStream(fileToZip);
        var zipEntry = new ZipEntry(fileToZip.getName());
        zipOut.putNextEntry(zipEntry);
        zipOut.write(fileInputStream.readAllBytes());
        fileInputStream.close();
      }
      zipOut.close();
      baOutputStream.close();
    } catch (IOException e) {
      log.warn("Not able to create zip from files");
    }
    return baOutputStream;

  }

}
