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
import com.datagen.model.Model;
import com.datagen.model.OptionsConverter;
import com.datagen.model.Row;
import com.datagen.model.type.Field;
import com.datagen.utils.KerberosUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This is an HDFSJSON connector using Hadoop 3.1 API
 * Each instance manages one connection to a file system
 */
@Slf4j
public class HdfsJsonConnector extends HdfsUtils implements ConnectorInterface {

  private FSDataOutputStream fsDataOutputStream;
  private final String lineSeparator;
  private int counter;
  private final Model model;
  private final Boolean oneFilePerIteration;

  /**
   * Initiate HDFSJSON connection with Kerberos or not
   *
   * @return filesystem connection to HDFSJSON
   */
  public HdfsJsonConnector(Model model,
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
        deleteAllHdfsFiles(directoryName,
            fileName, "json");
      }

      if (!oneFilePerIteration) {
        this.fsDataOutputStream = createFileWithOverwrite(directoryName + fileName + ".json");
      }
    }

  }

  @Override
  public void terminate() {
    try {
      fsDataOutputStream.close();
      fileSystem.close();
      if (useKerberos) {
        KerberosUtils.logoutUserWithKerberos();
      }
    } catch (IOException e) {
      log.error(" Unable to close HDFSJSON file with error :", e);
    }
  }

  @Override
  public void sendOneBatchOfRows(List<Row> rows) {
    try {
      if (oneFilePerIteration) {
        this.fsDataOutputStream = createFileWithOverwrite(
            directoryName + fileName + "-" + String.format("%010d", counter) +
                ".json");
        counter++;
      }

      List<String> rowsInString =
          rows.stream().map(Row::toJSON).collect(Collectors.toList());
      fsDataOutputStream.writeChars(String.join(lineSeparator, rowsInString));
      fsDataOutputStream.writeChars(lineSeparator);

      if (oneFilePerIteration) {
        fsDataOutputStream.close();
      }
    } catch (IOException e) {
      log.error("Can not write data to the HDFSJSON file due to error: ", e);
    }
  }

  @Override
  public Model generateModel(Boolean deepAnalysis) {
    LinkedHashMap<String, Field> fields = new LinkedHashMap<String, Field>();
    Map<String, List<String>> primaryKeys = new HashMap<>();
    Map<String, String> tableNames = new HashMap<>();
    Map<String, String> options = new HashMap<>();
    // TODO : Implement logic to create a model with at least names, pk, options and column names/types
    return new Model("",fields, primaryKeys, tableNames, options, null);
  }


}
