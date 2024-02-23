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
import com.cloudera.frisch.datagen.connector.storage.utils.OrcUtils;
import com.cloudera.frisch.datagen.model.type.Field;
import com.cloudera.frisch.datagen.utils.Utils;
import com.cloudera.frisch.datagen.config.ApplicationConfigs;
import com.cloudera.frisch.datagen.model.Model;
import com.cloudera.frisch.datagen.model.OptionsConverter;
import com.cloudera.frisch.datagen.model.Row;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * This is an ORC HDFS sink using Hadoop 3.2 API
 */
@SuppressWarnings("unchecked")
@Slf4j
public class HdfsOrcConnector implements ConnectorInterface {

  private FileSystem fileSystem;
  private TypeDescription schema;
  private Writer writer;
  private Map<String, ColumnVector> vectors;
  private VectorizedRowBatch batch;
  private int counter;
  private final Model model;
  private final String directoryName;
  private final String fileName;
  private final Boolean oneFilePerIteration;
  private final short replicationFactor;
  private final Configuration conf;
  private String hdfsUri;
  private Boolean useKerberos;

  /**
   * Initiate HDFS connection with Kerberos or not
   *
   * @return filesystem connection to HDFS
   */
  public HdfsOrcConnector(Model model,
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
    this.model = model;
    this.counter = 0;
    this.fileName = (String) model.getTableNames()
        .get(OptionsConverter.TableNames.HDFS_FILE_NAME);
    this.oneFilePerIteration = (Boolean) model.getOptionsOrDefault(
        OptionsConverter.Options.ONE_FILE_PER_ITERATION);
    this.replicationFactor = (short) model.getOptionsOrDefault(
        OptionsConverter.Options.HDFS_REPLICATION_FACTOR);
    this.conf = new Configuration();
    conf.set("dfs.replication", String.valueOf(replicationFactor));
    this.hdfsUri = properties.get(ApplicationConfigs.HDFS_URI);
    this.useKerberos = Boolean.parseBoolean(
        properties.get(ApplicationConfigs.HDFS_AUTH_KERBEROS));

    org.apache.hadoop.conf.Configuration config =
        new org.apache.hadoop.conf.Configuration();
    Utils.setupHadoopEnv(config, properties);

    // Set all kerberos if needed (Note that connection will require a user and its appropriate keytab with right privileges to access folders and files on HDFSCSV)
    if (useKerberos) {
      Utils.loginUserWithKerberos(
          properties.get(ApplicationConfigs.HDFS_AUTH_KERBEROS_USER),
          properties.get(ApplicationConfigs.HDFS_AUTH_KERBEROS_KEYTAB),
          config);
    }

    try {
      fileSystem = FileSystem.get(URI.create(hdfsUri), config);
    } catch (IOException e) {
      log.error("Could not access to ORC HDFS !", e);
    }

  }

  @Override
  public void init(Model model, boolean writer) {
    if (writer) {
      schema = model.getOrcSchema();
      batch = schema.createRowBatch();
      vectors = model.createOrcVectors(batch);

      Utils.createHdfsDirectory(fileSystem, directoryName);

      if ((Boolean) model.getOptionsOrDefault(
          OptionsConverter.Options.DELETE_PREVIOUS)) {
        Utils.deleteAllHdfsFiles(fileSystem, directoryName, fileName,
            "orc");
      }

      if (!oneFilePerIteration) {
        creatFileWithOverwrite(
            hdfsUri + directoryName + fileName + ".orc");
      }
    }

  }

  @Override
  public void terminate() {
    try {
      if (!oneFilePerIteration) {
        writer.close();
      }
      if (useKerberos) {
        Utils.logoutUserWithKerberos();
      }
    } catch (IOException e) {
      log.error(" Unable to close ORC HDFS file with error :", e);
    }
  }

  @Override
  public void sendOneBatchOfRows(List<Row> rows) {
    if (oneFilePerIteration) {
      creatFileWithOverwrite(hdfsUri + directoryName + fileName + "-" +
          String.format("%010d", counter) + ".orc");
      counter++;
    }

    for (Row row : rows) {
      int rowNumber = batch.size++;
      row.fillinOrcVector(rowNumber, vectors);
      try {
        if (batch.size == batch.getMaxSize()) {
          writer.addRowBatch(batch);
          batch.reset();
        }
      } catch (IOException e) {
        log.error("Can not write data to the ORC HDFS file due to error: ", e);
      }
    }

    try {
      if (batch.size != 0) {
        writer.addRowBatch(batch);
        batch.reset();
      }
    } catch (IOException e) {
      log.error("Can not write data to the ORC HDFS file due to error: ", e);
    }

    if (oneFilePerIteration) {
      try {
        writer.close();
      } catch (IOException e) {
        log.error(" Unable to close ORC HDFS file with error :", e);
      }
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
      Reader reader =
          OrcFile.createReader(new Path(this.hdfsUri + this.directoryName),
              OrcFile.readerOptions(new Configuration()));

      OrcUtils.setBasicFields(fields, reader);
      if (deepAnalysis) {
        OrcUtils.analyzeFields(fields, reader);
      }

      fileSystem.close();
    } catch (IOException e) {
      log.warn("Could not create reader to ORC local file due to error:", e);
    }

    return new Model(fields, primaryKeys, tableNames, options);
  }

  private void creatFileWithOverwrite(String path) {
    try {
      Utils.deleteHdfsFile(fileSystem, path);
      writer = OrcFile.createWriter(new Path(path),
          OrcFile.writerOptions(conf)
              .setSchema(schema));
    } catch (IOException e) {
      log.warn("Could not create writer to ORC HDFS file due to error:", e);
    }
  }
}
