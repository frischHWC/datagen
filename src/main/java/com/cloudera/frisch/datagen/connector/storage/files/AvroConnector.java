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
import com.cloudera.frisch.datagen.connector.storage.utils.AvroUtils;
import com.cloudera.frisch.datagen.connector.storage.utils.FileUtils;
import com.cloudera.frisch.datagen.model.Model;
import com.cloudera.frisch.datagen.model.OptionsConverter;
import com.cloudera.frisch.datagen.model.Row;
import com.cloudera.frisch.datagen.model.type.Field;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Avro connector to create Local Avro files
 */
@Slf4j
public class AvroConnector implements ConnectorInterface {

  private Schema schema;
  private DataFileWriter<GenericRecord> dataFileWriter;
  private DatumWriter<GenericRecord> datumWriter;
  private int counter;
  private Model model;
  private final String directoryName;
  private final String fileName;
  private final Boolean oneFilePerIteration;

  /**
   * Init local Avro file with header
   */
  public AvroConnector(Model model,
                       Map<ApplicationConfigs, String> properties) {
    this.counter = 0;
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
      this.model = model;
      schema = model.getAvroSchema();
      datumWriter = new GenericDatumWriter<>(schema);
      FileUtils.createLocalDirectory(directoryName);

      if ((Boolean) model.getOptionsOrDefault(
          OptionsConverter.Options.DELETE_PREVIOUS)) {
        FileUtils.deleteAllLocalFiles(directoryName, fileName, "avro");
      }

      if (!oneFilePerIteration) {
        this.dataFileWriter = AvroUtils.createFileWithOverwrite(directoryName + fileName + ".avro", schema, datumWriter);
      }
    }
  }

  @Override
  public void terminate() {
    try {
      if (!oneFilePerIteration) {
        dataFileWriter.close();
      }
    } catch (IOException e) {
      log.error(" Unable to close local file with error :", e);
    }
  }

  @Override
  public void sendOneBatchOfRows(List<Row> rows) {

    if (oneFilePerIteration) {
      this.dataFileWriter = AvroUtils.createFileWithOverwrite(
          directoryName + fileName + "-" + String.format("%010d", counter) +
              ".avro", schema, datumWriter);
      counter++;
    }

    rows.stream().map(row -> row.toGenericRecord(schema))
        .forEach(genericRecord -> {
          try {
            dataFileWriter.append(genericRecord);
          } catch (IOException e) {
            log.error("Can not write data to the local file due to error: ", e);
          }
        });

    if (oneFilePerIteration) {
      try {
        dataFileWriter.close();
      } catch (IOException e) {
        log.error(" Unable to close local file with error :", e);
      }
    } else {
      try {
        dataFileWriter.flush();
      } catch (IOException e) {
        log.error(" Unable to flush local file with error :", e);
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
      FileReader<GenericRecord> fileReader =
          DataFileReader.openReader(new File(this.directoryName),
              new GenericDatumReader<>());
      AvroUtils.setBasicFields(fields, fileReader.getSchema());
      if (deepAnalysis) {
        AvroUtils.analyzeFields(fields, fileReader);
      }
      fileReader.close();
    } catch (IOException e) {
      log.warn("Could not read Avro local file: {} due to error",
          this.directoryName, e);
    }

    return new Model(fields, primaryKeys, tableNames, options);
  }

}
