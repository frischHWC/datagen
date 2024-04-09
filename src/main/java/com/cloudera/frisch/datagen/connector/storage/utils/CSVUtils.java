package com.cloudera.frisch.datagen.connector.storage.utils;

import com.cloudera.frisch.datagen.model.Model;
import com.cloudera.frisch.datagen.model.OptionsConverter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.OutputStream;

@Slf4j
public class CSVUtils {

  public static void appendCSVHeader(Model model, OutputStream outputStream, String lineSeparator) {
    try {
      if ((Boolean) model.getOptionsOrDefault(
          OptionsConverter.Options.CSV_HEADER)) {
        outputStream.write(model.getCsvHeader().getBytes());
        outputStream.write(lineSeparator.getBytes());
      }
    } catch (IOException e) {
      log.error("Can not write header to the local file due to error: ", e);
    }
  }

}
