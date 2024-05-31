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
package com.datagen.connector.storage.hdfs;


import com.datagen.config.ApplicationConfigs;
import com.datagen.connector.ConnectorInterface;
import com.datagen.connector.storage.utils.CSVUtils;
import com.datagen.model.Model;
import com.datagen.model.OptionsConverter;
import com.datagen.model.Row;
import com.datagen.model.type.Field;
import com.datagen.model.type.StringField;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Collectors;

/**
 * This is an HDFSCSV connector using Hadoop 3.2 API
 * Each instance manages one connection to a file system
 */
@Slf4j
public class HdfsCsvConnector extends HdfsUtils implements ConnectorInterface {

  private FSDataOutputStream fsDataOutputStream;
  private final String lineSeparator;

  private int counter;
  private final Model model;
  private final Boolean oneFilePerIteration;


  /**
   * Initiate HDFSCSV connection with Kerberos or not
   */
  public HdfsCsvConnector(Model model,
                          Map<ApplicationConfigs, String> properties) {
    super(model, properties);
    this.model = model;
    this.counter = 0;
    this.lineSeparator = System.getProperty("line.separator");
    this.oneFilePerIteration = (Boolean) model.getOptionsOrDefault(
        OptionsConverter.Options.ONE_FILE_PER_ITERATION);

  }

  @Override
  public void init(Model model, boolean writer) {
    if (writer) {

      createHdfsDirectory(directoryName);

      if ((Boolean) model.getOptionsOrDefault(
          OptionsConverter.Options.DELETE_PREVIOUS)) {
        deleteAllHdfsFiles(directoryName, fileName, "csv");
      }

      if (!oneFilePerIteration) {
        this.fsDataOutputStream = createFileWithOverwrite(directoryName + fileName + ".csv");
        CSVUtils.appendCSVHeader(model, fsDataOutputStream, lineSeparator);
      }
    }

  }

  @Override
  public void terminate() {
    try {
      fsDataOutputStream.close();
      closeHDFS();
    } catch (IOException e) {
      log.error(" Unable to close HDFSCSV file with error :", e);
    }
  }

  @Override
  public void sendOneBatchOfRows(List<Row> rows) {
    try {
      if (oneFilePerIteration) {
        this.fsDataOutputStream = createFileWithOverwrite(
            directoryName + fileName + "-" + String.format("%010d", counter) +
                ".csv");
        CSVUtils.appendCSVHeader(model, fsDataOutputStream, lineSeparator);
        counter++;
      }

      List<String> rowsInString =
          rows.stream().map(Row::toCSV).collect(Collectors.toList());
      fsDataOutputStream.writeChars(String.join(lineSeparator, rowsInString));
      fsDataOutputStream.writeChars(lineSeparator);

      if (oneFilePerIteration) {
        fsDataOutputStream.close();
      }
    } catch (IOException e) {
      log.error("Can not write data to the HDFSCSV file due to error: ", e);
    }
  }

  @Override
  public Model generateModel(Boolean deepAnalysis) {
    LinkedHashMap<String, Field> fields = new LinkedHashMap<String, Field>();
    Map<String, List<String>> primaryKeys = new HashMap<>();
    Map<String, String> tableNames = new HashMap<>();
    Map<String, String> options = new HashMap<>();

    tableNames.put("HDFS_FILE_PATH",
        this.directoryName.substring(0, this.directoryName.lastIndexOf("/")) +
            "/");
    tableNames.put("HDFS_FILE_NAME",
        this.directoryName.substring(this.directoryName.lastIndexOf("/") + 1));

    try {
      FSDataInputStream fsDataInputStream =
          fileSystem.open(new Path(this.directoryName));
      String csvHeader = new BufferedReader(
          new InputStreamReader(fsDataInputStream)).readLine();
      Arrays.stream(csvHeader.split(","))
          .forEach(f -> fields.put(f,
              new StringField(f, null, Collections.emptyList(),
                  new LinkedHashMap<>())));
      fsDataInputStream.close();
      fileSystem.close();
    } catch (IOException e) {
      log.error("Tried to read file : {} with no success :", this.directoryName,
          e);
    }

    return new Model(fields, primaryKeys, tableNames, options);
  }


}
