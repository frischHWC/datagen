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
import com.datagen.connector.storage.utils.ParquetUtils;
import com.datagen.model.Model;
import com.datagen.model.OptionsConverter;
import com.datagen.model.Row;
import com.datagen.model.type.Field;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * This is an HDFS PARQUET connector using Hadoop 3.2 API
 */
@Slf4j
public class HdfsParquetConnector extends HdfsUtils implements ConnectorInterface {

  private Schema schema;
  private ParquetWriter<GenericRecord> writer;

  private int counter;
  private final Model model;
  private final Boolean oneFilePerIteration;

  /**
   * Initiate HDFS connection with Kerberos or not
   * @return filesystem connection to HDFS
   */
  public HdfsParquetConnector(Model model,
                              Map<ApplicationConfigs, String> properties) {

    super(model, properties);
    this.counter = 0;
    this.model = model;
    this.oneFilePerIteration = (Boolean) model.getOptionsOrDefault(
        OptionsConverter.Options.ONE_FILE_PER_ITERATION);
  }

  @Override
  public void init(Model model, boolean writer) {
    if (writer) {
      schema = model.getAvroSchema();

      createHdfsDirectory(directoryName);

      if ((Boolean) model.getOptionsOrDefault(
          OptionsConverter.Options.DELETE_PREVIOUS)) {
        deleteAllHdfsFiles(directoryName, fileName,
            "parquet");
      }

      if (!oneFilePerIteration) {
        this.writer = ParquetUtils.createParquetWriter(
            hdfsUri + directoryName + fileName + ".parquet", schema, this.writer, this.model, configuration);
      }
    }

  }


  @Override
  public void terminate() {
    try {
      writer.close();
      closeHDFS();
    } catch (IOException e) {
      log.error(" Unable to close HDFS PARQUET file with error :", e);
    }
  }

  @Override
  public void sendOneBatchOfRows(List<Row> rows) {
    try {
      if (oneFilePerIteration) {
        this.writer = ParquetUtils.createParquetWriter(hdfsUri + directoryName + fileName + "-" +
            String.format("%010d", counter) + ".parquet", schema, this.writer, this.model, configuration);
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

    return new Model("",fields, primaryKeys, tableNames, options, null);
  }

}
