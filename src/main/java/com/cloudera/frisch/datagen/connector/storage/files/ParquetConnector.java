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
package com.cloudera.frisch.datagen.connector.storage.files;


import com.cloudera.frisch.datagen.config.ApplicationConfigs;
import com.cloudera.frisch.datagen.connector.ConnectorInterface;
import com.cloudera.frisch.datagen.connector.storage.utils.ParquetUtils;
import com.cloudera.frisch.datagen.model.Model;
import com.cloudera.frisch.datagen.model.OptionsConverter;
import com.cloudera.frisch.datagen.model.Row;
import com.cloudera.frisch.datagen.model.type.*;
import com.cloudera.frisch.datagen.utils.Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;
import java.io.IOException;
import java.util.*;

@Slf4j
public class ParquetConnector implements ConnectorInterface {


  private Schema schema;
  private ParquetWriter<GenericRecord> writer;
  private int counter;
  private final Model model;
  private final String directoryName;
  private final String fileName;
  private final Boolean oneFilePerIteration;

  /**
   * Init local Parquet file
   */
  public ParquetConnector(Model model,
                          Map<ApplicationConfigs, String> properties) {
    this.counter = 0;
    this.model = model;
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
      schema = model.getAvroSchema();

      Utils.createLocalDirectory(directoryName);

      if ((Boolean) model.getOptionsOrDefault(
          OptionsConverter.Options.DELETE_PREVIOUS)) {
        Utils.deleteAllLocalFiles(directoryName, fileName, "parquet");
      }

      if (!oneFilePerIteration) {
        createFileWithOverwrite(directoryName + fileName + ".parquet");
      }
    }

  }


  @Override
  public void terminate() {
    try {
      if (!oneFilePerIteration) {
        writer.close();
      }
    } catch (IOException e) {
      log.error(" Unable to close local file with error :", e);
    }
  }

  @Override
  public void sendOneBatchOfRows(List<Row> rows) {
    if (oneFilePerIteration) {
      createFileWithOverwrite(
          directoryName + fileName + "-" + String.format("%010d", counter) +
              ".parquet");
      counter++;
    }
    rows.stream().map(row -> row.toGenericRecord(schema))
        .forEach(genericRecord -> {
          try {
            writer.write(genericRecord);
          } catch (IOException e) {
            log.error("Can not write data to the local file due to error: ", e);
          }
        });
    if (oneFilePerIteration) {
      try {
        writer.close();
      } catch (IOException e) {
        log.error(" Unable to close local file with error :", e);
      }
    }
  }

  @Override
  public Model generateModel(Boolean deepAnalysis) {
    LinkedHashMap<String, Field> fields = new LinkedHashMap<String, Field>();
    Map<String, List<String>> primaryKeys = new HashMap<>();
    Map<String, String> tableNames = new HashMap<>();
    Map<String, String> options = new HashMap<>();

    tableNames.put("LOCAL_FILE_PATH",
        this.directoryName.substring(0, this.directoryName.lastIndexOf("/")) +
            "/");
    tableNames.put("LOCAL_FILE_NAME",
        this.directoryName.substring(this.directoryName.lastIndexOf("/") + 1));
    tableNames.put("AVRO_NAME",
        this.directoryName.substring(this.directoryName.lastIndexOf("/") + 1) +
            "_avro");

    try {
      ParquetFileReader parquetReader =
          ParquetFileReader.open(new Configuration(),
              new Path(this.directoryName));
      ParquetUtils.setBasicFields(fields, parquetReader);
      if (deepAnalysis) {
        ParquetUtils.analyzeFields(fields, parquetReader);
      }
      parquetReader.close();
    } catch (IOException e) {
      log.warn("Cannot read local Parquet file: {} due to error",
          this.directoryName, e);
    }

    return new Model(fields, primaryKeys, tableNames, options);
  }

  private void createFileWithOverwrite(String path) {
    try {
      Utils.deleteLocalFile(path);
      new File(path).getParentFile().mkdirs();
      this.writer = AvroParquetWriter
          .<GenericRecord>builder(new Path(path))
          .withSchema(schema)
          .withConf(new Configuration())
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