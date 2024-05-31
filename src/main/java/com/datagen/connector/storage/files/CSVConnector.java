/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datagen.connector.storage.files;


import com.datagen.config.ApplicationConfigs;
import com.datagen.connector.ConnectorInterface;
import com.datagen.connector.storage.utils.CSVUtils;
import com.datagen.connector.storage.utils.FileUtils;
import com.datagen.model.Model;
import com.datagen.model.OptionsConverter;
import com.datagen.model.Row;
import com.datagen.model.type.Field;
import com.datagen.model.type.StringField;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.*;

/**
 * This is a CSV connector to write to one or multiple CSV files locally
 */
@Slf4j
public class CSVConnector implements ConnectorInterface {

  private FileOutputStream outputStream;
  private int counter;
  private final Model model;
  private final String lineSeparator;
  private final String directoryName;
  private final String fileName;
  private final Boolean oneFilePerIteration;

  /**
   * Init local CSV file with header
   */
  public CSVConnector(Model model, Map<ApplicationConfigs, String> properties) {
    this.model = model;
    this.counter = 0;
    this.lineSeparator = System.getProperty("line.separator");
    this.directoryName = (String) model.getTableNames()
        .get(OptionsConverter.TableNames.LOCAL_FILE_PATH);
    this.fileName = (String) model.getTableNames()
        .get(OptionsConverter.TableNames.LOCAL_FILE_NAME);
    this.oneFilePerIteration = (Boolean) model.getOptionsOrDefault(
        OptionsConverter.Options.ONE_FILE_PER_ITERATION);

  }

  @Override
  public void init(Model model, boolean writer) {
    if (writer) {
      FileUtils.createLocalDirectory(directoryName);

      if ((Boolean) model.getOptionsOrDefault(
          OptionsConverter.Options.DELETE_PREVIOUS)) {
        FileUtils.deleteAllLocalFiles(directoryName, fileName, "csv");
      }

      if (!oneFilePerIteration) {
        this.outputStream = FileUtils.createLocalFileAsOutputStream(directoryName + fileName + ".csv");
        CSVUtils.appendCSVHeader(model, outputStream, lineSeparator);
      }
    }
  }

  @Override
  public void terminate() {
    try {
      if (!oneFilePerIteration) {
        outputStream.close();
      }
    } catch (IOException e) {
      log.error(" Unable to close local file with error :", e);
    }
  }

  @Override
  public void sendOneBatchOfRows(List<Row> rows) {
    try {
      if (oneFilePerIteration) {
        this.outputStream = FileUtils.createLocalFileAsOutputStream(
            directoryName + fileName + "-" + String.format("%010d", counter) +
                ".csv");
        CSVUtils.appendCSVHeader(model, outputStream, lineSeparator);
        counter++;
      }

      rows.stream().map(Row::toCSV).forEach(r -> {
        try {
          outputStream.write(r.getBytes());
          outputStream.write(lineSeparator.getBytes());
        } catch (IOException e) {
          log.error("Could not write row: " + r + " to file: " +
              outputStream.getChannel());
        }
      });
      outputStream.write(lineSeparator.getBytes());

      if (oneFilePerIteration) {
        outputStream.close();
      }
    } catch (IOException e) {
      log.error("Can not write data to the local file due to error: ", e);
    }
  }

  @Override
  public Model generateModel(Boolean deepAnalysis) {
    LinkedHashMap<String, Field> fields = new LinkedHashMap<String, Field>();
    Map<String, List<String>> primaryKeys = new HashMap<>();
    Map<String, String> tableNames = new HashMap<>();
    Map<String, String> options = new HashMap<>();

    tableNames.put("LOCAL_FILE_PATH", this.directoryName.substring(0, this.directoryName.lastIndexOf("/"))+"/");
    tableNames.put("LOCAL_FILE_NAME", this.directoryName.substring(this.directoryName.lastIndexOf("/")+1));

    try {
      File file = new File(this.directoryName);
      if (file.exists() && file.isFile()) {
        String csvHeader = new BufferedReader(new FileReader(file)).readLine();
        Arrays.stream(csvHeader.split(","))
            .forEach(f -> fields.put(f, new StringField(f, null, Collections.emptyList(), new LinkedHashMap<>())));
      }
    } catch (IOException e) {
      log.error("Tried to read file : {} with no success :", this.directoryName,
          e);
    }

    return new Model(fields, primaryKeys, tableNames, options);
  }

}
