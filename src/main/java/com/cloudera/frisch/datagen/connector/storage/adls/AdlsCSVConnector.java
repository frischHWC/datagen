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
package com.cloudera.frisch.datagen.connector.storage.adls;


import com.cloudera.frisch.datagen.config.ApplicationConfigs;
import com.cloudera.frisch.datagen.connector.ConnectorInterface;
import com.cloudera.frisch.datagen.connector.storage.utils.CSVUtils;
import com.cloudera.frisch.datagen.connector.storage.utils.FileUtils;
import com.cloudera.frisch.datagen.model.Model;
import com.cloudera.frisch.datagen.model.OptionsConverter;
import com.cloudera.frisch.datagen.model.Row;
import com.cloudera.frisch.datagen.model.type.Field;
import com.cloudera.frisch.datagen.model.type.StringField;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.*;

import static com.cloudera.frisch.datagen.config.ApplicationConfigs.DATA_HOME_DIRECTORY;

/**
 * This is a CSV connector to write to one or multiple CSV files to ADLS
 */
@Slf4j
public class AdlsCSVConnector extends AdlsUtils implements ConnectorInterface  {

  private final Model model;
  private FileOutputStream outputStream;
  private final String lineSeparator;
  private final Boolean oneFilePerIteration;
  private final String localFilePathForModelGeneration;

  private int counter;
  private String currentFileName;

  /**
   * Init S3 CSV
   */
  public AdlsCSVConnector(Model model,
                          Map<ApplicationConfigs, String> properties) {
    super(model, properties);
    this.model = model;
    this.counter = 0;
    this.lineSeparator = System.getProperty("line.separator");
    this.oneFilePerIteration = (Boolean) model.getOptionsOrDefault(
        OptionsConverter.Options.ONE_FILE_PER_ITERATION);
    this.localFilePathForModelGeneration = properties.get(DATA_HOME_DIRECTORY) + "/model-gen/azure/";
  }

  @Override
  public void init(Model model, boolean writer) {
    if (writer) {
      if ((Boolean) model.getOptionsOrDefault(
          OptionsConverter.Options.DELETE_PREVIOUS)) {
        deleteAllfiles(fileNamePrefix, "csv");
      }

      // Will use a local directory before pushing data to S3
      FileUtils.createLocalDirectory(localDirectory);
      FileUtils.deleteAllLocalFiles(localDirectory, fileNamePrefix, "csv");

      createDirectoryIfNotExists();

      if (!oneFilePerIteration) {
        this.currentFileName = fileNamePrefix + ".csv";
        this.outputStream = FileUtils.createLocalFileAsOutputStream(
            localDirectory +
                currentFileName);
        CSVUtils.appendCSVHeader(model, outputStream, lineSeparator);
      }
    } else {
      FileUtils.createLocalDirectory(localFilePathForModelGeneration);
    }
  }

  @Override
  public void terminate() {
    try {
      if (!oneFilePerIteration) {
        outputStream.close();
        pushLocalFileToADLS(localDirectory + currentFileName, currentFileName);
      }
    } catch (IOException e) {
      log.error(" Unable to close local file with error :", e);
    } finally {
      FileUtils.deleteAllLocalFiles(localDirectory, fileNamePrefix, "csv");
    }
  }

  @Override
  public void sendOneBatchOfRows(List<Row> rows) {
    try {
      if (oneFilePerIteration) {
        this.currentFileName = fileNamePrefix + "-" + String.format("%010d", counter) + ".csv";
        this.outputStream = FileUtils.createLocalFileAsOutputStream(
            localDirectory + currentFileName);
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
        pushLocalFileToADLS(localDirectory + currentFileName, currentFileName);
        FileUtils.deleteLocalFile(localDirectory + currentFileName);
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

    tableNames.put("AZURE_CONTAINER", this.containerName);
    tableNames.put("AZURE_DIRECTORY", this.directoryName);
    tableNames.put("AZURE_FILE_NAME", this.fileNamePrefix);
    tableNames.put("AZURE_LOCAL_FILE_PATH", this.localDirectory);

    try {
      String localFile = this.localFilePathForModelGeneration + this.fileNamePrefix;
      readFileFromADLS(localFile, this.fileNamePrefix);
      File file = new File(localFile);
      if (file.exists() && file.isFile()) {
        String csvHeader = new BufferedReader(new FileReader(file)).readLine();
        Arrays.stream(csvHeader.split(","))
            .forEach(f -> fields.put(f,
                new StringField(f, null, Collections.emptyList(),
                    new LinkedHashMap<>())));
      }
    } catch (IOException e) {
      log.error("Tried to read file : {} with no success :", this.localDirectory,
          e);
    }

    return new Model(fields, primaryKeys, tableNames, options);
  }


}
