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
import com.cloudera.frisch.datagen.connector.storage.utils.ParquetUtils;
import com.cloudera.frisch.datagen.model.type.Field;
import com.cloudera.frisch.datagen.utils.KerberosUtils;
import com.cloudera.frisch.datagen.utils.Utils;
import com.cloudera.frisch.datagen.config.ApplicationConfigs;
import com.cloudera.frisch.datagen.model.Model;
import com.cloudera.frisch.datagen.model.OptionsConverter;
import com.cloudera.frisch.datagen.model.Row;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * This is an HDFS PARQUET connector using Hadoop 3.2 API
 */
// TODO: Refactor to use one abstract class
@Slf4j
public class HdfsParquetConnector extends HdfsUtils implements ConnectorInterface {

  private FileSystem fileSystem;
  private Schema schema;
  private ParquetWriter<GenericRecord> writer;
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
   * @return filesystem connection to HDFS
   */
  public HdfsParquetConnector(Model model,
                              Map<ApplicationConfigs, String> properties) {
    // If using an HDFS connector, we want it to use the Hive HDFS File path and not the Hdfs file path
    if (properties.get(ApplicationConfigs.HDFS_FOR_HIVE) != null
        && properties.get(ApplicationConfigs.HDFS_FOR_HIVE)
        .equalsIgnoreCase("true")) {
      this.directoryName = (String) model.getTableNames()
          .get(OptionsConverter.TableNames.HIVE_HDFS_FILE_PATH);
    } else {
      this.directoryName = (String) model.getTableNames()
          .get(OptionsConverter.TableNames.HDFS_FILE_PATH);
    }
    log.debug("HDFS connector will generates data into HDFS directory: " +
        this.directoryName);
    this.fileName = (String) model.getTableNames()
        .get(OptionsConverter.TableNames.HDFS_FILE_NAME);
    this.oneFilePerIteration = (Boolean) model.getOptionsOrDefault(
        OptionsConverter.Options.ONE_FILE_PER_ITERATION);
    this.model = model;
    this.counter = 0;
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
      KerberosUtils.loginUserWithKerberos(
          properties.get(ApplicationConfigs.HDFS_AUTH_KERBEROS_USER),
          properties.get(ApplicationConfigs.HDFS_AUTH_KERBEROS_KEYTAB),
          config);
    }

    try {
      this.fileSystem = FileSystem.get(URI.create(hdfsUri), config);
    } catch (IOException e) {
      log.error("Could not access to HDFS PARQUET !", e);
    }
  }

  @Override
  public void init(Model model, boolean writer) {
    if (writer) {
      schema = model.getAvroSchema();

      createHdfsDirectory(fileSystem, directoryName);

      if ((Boolean) model.getOptionsOrDefault(
          OptionsConverter.Options.DELETE_PREVIOUS)) {
        deleteAllHdfsFiles(fileSystem, directoryName, fileName,
            "parquet");
      }

      if (!oneFilePerIteration) {
        createFileWithOverwrite(
            hdfsUri + directoryName + fileName + ".parquet");
      }
    }

  }


  @Override
  public void terminate() {
    try {
      writer.close();
      if (useKerberos) {
        KerberosUtils.logoutUserWithKerberos();
      }
    } catch (IOException e) {
      log.error(" Unable to close HDFS PARQUET file with error :", e);
    }
  }

  @Override
  public void sendOneBatchOfRows(List<Row> rows) {
    try {
      if (oneFilePerIteration) {
        createFileWithOverwrite(hdfsUri + directoryName + fileName + "-" +
            String.format("%010d", counter) + ".parquet");
        counter++;
      }

      rows.stream().map(row -> row.toGenericRecord(schema))
          .forEach(genericRecord -> {
            try {
              writer.write(genericRecord);
            } catch (IOException e) {
              log.error(
                  "Can not write data to the HDFS PARQUET file due to error: ",
                  e);
            }
          });

      if (oneFilePerIteration) {
        writer.close();
      }
    } catch (IOException e) {
      log.error("Can not write data to the HDFS PARQUET file due to error: ",
          e);
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
    tableNames.put("AVRO_NAME",
        this.directoryName.substring(this.directoryName.lastIndexOf("/") + 1) +
            "_avro");

    try {
      ParquetFileReader parquetReader =
          ParquetFileReader.open(new Configuration(),
              new Path(this.hdfsUri + this.directoryName));
      ParquetUtils.setBasicFields(fields, parquetReader);
      if (deepAnalysis) {
        ParquetUtils.analyzeFields(fields, parquetReader);
      }
      parquetReader.close();
      fileSystem.close();
    } catch (IOException e) {
      log.warn("Cannot read local Parquet file: {} due to error",
          this.directoryName, e);
    }

    return new Model(fields, primaryKeys, tableNames, options);
  }

  private void createFileWithOverwrite(String path) {
    try {
      deleteHdfsFile(fileSystem, path);
      this.writer = AvroParquetWriter
          .<GenericRecord>builder(new Path(path))
          .withSchema(schema)
          .withConf(conf)
          .withCompressionCodec(CompressionCodecName.SNAPPY)
          .withPageSize((int) model.getOptionsOrDefault(
              OptionsConverter.Options.PARQUET_PAGE_SIZE))
          .withDictionaryEncoding((Boolean) model.getOptionsOrDefault(
              OptionsConverter.Options.PARQUET_DICTIONARY_ENCODING))
          .withDictionaryPageSize((int) model.getOptionsOrDefault(
              OptionsConverter.Options.PARQUET_DICTIONARY_PAGE_SIZE))
          .withRowGroupSize((int) model.getOptionsOrDefault(
              OptionsConverter.Options.PARQUET_ROW_GROUP_SIZE))
          .build();
      log.debug("Successfully created local Parquet file : " + path);

    } catch (IOException e) {
      log.error(
          "Tried to create Parquet local file : " + path + " with no success :",
          e);
    }
  }

}
