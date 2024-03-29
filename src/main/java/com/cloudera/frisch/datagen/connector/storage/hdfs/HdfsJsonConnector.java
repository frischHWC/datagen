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
package com.cloudera.frisch.datagen.connector.storage.hdfs;


import com.cloudera.frisch.datagen.connector.ConnectorInterface;
import com.cloudera.frisch.datagen.model.type.Field;
import com.cloudera.frisch.datagen.utils.Utils;
import com.cloudera.frisch.datagen.config.ApplicationConfigs;
import com.cloudera.frisch.datagen.model.Model;
import com.cloudera.frisch.datagen.model.OptionsConverter;
import com.cloudera.frisch.datagen.model.Row;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This is an HDFSJSON sink using Hadoop 3.1 API
 * Each instance manages one connection to a file system and one specific file
 */
@Slf4j
public class HdfsJsonConnector implements ConnectorInterface {

  private FileSystem fileSystem;
  private FSDataOutputStream fsDataOutputStream;
  private int counter;
  private final Model model;
  private final String directoryName;
  private final String fileName;
  private final Boolean oneFilePerIteration;
  private final short replicationFactor;
  private String hdfsUri;
  private Boolean useKerberos;

  /**
   * Initiate HDFSJSON connection with Kerberos or not
   *
   * @return filesystem connection to HDFSJSON
   */
  public HdfsJsonConnector(Model model,
                           Map<ApplicationConfigs, String> properties) {
    // If using an HDFS sink, we want it to use the Hive HDFS File path and not the Hdfs file path
    if (properties.get(ApplicationConfigs.HDFS_FOR_HIVE) != null
        && properties.get(ApplicationConfigs.HDFS_FOR_HIVE)
        .equalsIgnoreCase("true")) {
      this.directoryName = (String) model.getTableNames()
          .get(OptionsConverter.TableNames.HIVE_HDFS_FILE_PATH);
    } else {
      this.directoryName = (String) model.getTableNames()
          .get(OptionsConverter.TableNames.HDFS_FILE_PATH);
    }
    log.debug("HDFS sink will generates data into HDFS directory: " +
        this.directoryName);
    this.fileName = (String) model.getTableNames()
        .get(OptionsConverter.TableNames.HDFS_FILE_NAME);
    this.oneFilePerIteration = (Boolean) model.getOptionsOrDefault(
        OptionsConverter.Options.ONE_FILE_PER_ITERATION);
    this.model = model;
    this.counter = 0;
    this.replicationFactor = (short) model.getOptionsOrDefault(
        OptionsConverter.Options.HDFS_REPLICATION_FACTOR);
    this.hdfsUri = properties.get(ApplicationConfigs.HDFS_URI);
    this.useKerberos = Boolean.parseBoolean(
        properties.get(ApplicationConfigs.HDFS_AUTH_KERBEROS));

    Configuration config = new Configuration();
    Utils.setupHadoopEnv(config, properties);

    // Set all kerberos if needed (Note that connection will require a user and its appropriate keytab with right privileges to access folders and files on HDFSCSV)
    if (useKerberos) {
      Utils.loginUserWithKerberos(
          properties.get(ApplicationConfigs.HDFS_AUTH_KERBEROS_USER),
          properties.get(ApplicationConfigs.HDFS_AUTH_KERBEROS_KEYTAB),
          config);
    }

    log.debug("Setting up access to HDFSJSON");
    try {
      fileSystem = FileSystem.get(URI.create(hdfsUri), config);
    } catch (IOException e) {
      log.error("Could not access to HDFSJSON !", e);
    }
  }

  @Override
  public void init(Model model, boolean writer) {
    if (writer) {

      Utils.createHdfsDirectory(fileSystem, directoryName);

      if ((Boolean) model.getOptionsOrDefault(
          OptionsConverter.Options.DELETE_PREVIOUS)) {
        Utils.deleteAllHdfsFiles(fileSystem, directoryName,
            fileName, "json");
      }

      if (!oneFilePerIteration) {
        createFileWithOverwrite(directoryName + fileName + ".json");
      }
    }

  }

  @Override
  public void terminate() {
    try {
      fsDataOutputStream.close();
      fileSystem.close();
      if (useKerberos) {
        Utils.logoutUserWithKerberos();
      }
    } catch (IOException e) {
      log.error(" Unable to close HDFSJSON file with error :", e);
    }
  }

  @Override
  public void sendOneBatchOfRows(List<Row> rows) {
    try {
      if (oneFilePerIteration) {
        createFileWithOverwrite(
            directoryName + fileName + "-" + String.format("%010d", counter) +
                ".json");
        counter++;
      }

      List<String> rowsInString =
          rows.stream().map(Row::toJSON).collect(Collectors.toList());
      fsDataOutputStream.writeChars(
          String.join(System.getProperty("line.separator"), rowsInString));
      fsDataOutputStream.writeChars(System.getProperty("line.separator"));

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
    return new Model(fields, primaryKeys, tableNames, options);
  }

  void createFileWithOverwrite(String path) {
    try {
      Utils.deleteHdfsFile(fileSystem, path);
      fsDataOutputStream = fileSystem.create(new Path(path), replicationFactor);
      log.debug("Successfully created hdfs file : " + path);
    } catch (IOException e) {
      log.error("Tried to create file : " + path + " with no success :", e);
    }
  }

}
