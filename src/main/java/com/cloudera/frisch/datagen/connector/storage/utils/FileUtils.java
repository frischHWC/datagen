package com.cloudera.frisch.datagen.connector.storage.utils;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

@Slf4j
public class FileUtils {

  public static FileOutputStream createFileWithOverwrite(String path) {
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
}
