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
package com.datagen.connector.storage.adls;


import com.datagen.config.ApplicationConfigs;
import com.datagen.connector.ConnectorInterface;
import com.datagen.connector.storage.utils.AvroUtils;
import com.datagen.connector.storage.utils.FileUtils;
import com.datagen.model.Model;
import com.datagen.model.OptionsConverter;
import com.datagen.model.Row;
import com.datagen.model.type.Field;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
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
 * This is a Avro connector to write to one or multiple Avro files to ADLS
 */
@Slf4j
public class AdlsAvroConnector extends AdlsUtils implements ConnectorInterface  {

  private final Model model;
  private final Boolean oneFilePerIteration;

  private int counter;
  private String currentFileName;

  private final Schema schema;
  private DataFileWriter<GenericRecord> dataFileWriter;
  private final DatumWriter<GenericRecord> datumWriter;

  /**
   * Init ADLS Avro
   */
  public AdlsAvroConnector(Model model,
                           Map<ApplicationConfigs, String> properties) {
    super(model, properties);
    this.model = model;
    this.counter = 0;
    this.oneFilePerIteration = (Boolean) model.getOptionsOrDefault(
        OptionsConverter.Options.ONE_FILE_PER_ITERATION);

    this.schema = model.getAvroSchema();
    this.datumWriter = new GenericDatumWriter<>(schema);
  }

  @Override
  public void init(Model model, boolean writer) {
    if (writer) {
      if ((Boolean) model.getOptionsOrDefault(
          OptionsConverter.Options.DELETE_PREVIOUS)) {
        deleteAllfiles(fileNamePrefix, "avro");
      }

      // Will use a local directory before pushing data to ADLS
      FileUtils.createLocalDirectory(localDirectory);
      FileUtils.deleteAllLocalFiles(localDirectory, fileNamePrefix, "avro");

      createDirectoryIfNotExists();

      if (!oneFilePerIteration) {
        this.currentFileName = fileNamePrefix + ".avro";
        this.dataFileWriter = AvroUtils.createFileWithOverwrite(localDirectory +
            fileNamePrefix + ".avro", schema, datumWriter);
      }
    }
  }

  @Override
  public void terminate() {
    try {
      if (!oneFilePerIteration) {
        dataFileWriter.flush();
        dataFileWriter.close();
        pushLocalFileToADLS(localDirectory + currentFileName,
            currentFileName);
      }
    } catch (IOException e) {
      log.error(" Unable to close local file with error :", e);
    } finally {
      FileUtils.deleteAllLocalFiles(localDirectory, currentFileName, "avro");
    }
  }

  @Override
  public void sendOneBatchOfRows(List<Row> rows) {
    try {
      if (oneFilePerIteration) {
        this.currentFileName = fileNamePrefix + "-" + String.format("%010d", counter) + ".avro";
        this.dataFileWriter = AvroUtils.createFileWithOverwrite(localDirectory +
            currentFileName, schema, datumWriter);
        counter++;
      }

      rows.stream().map(row -> row.toGenericRecord(schema))
          .forEach(genericRecord -> {
            try {
              this.dataFileWriter.append(genericRecord);
            } catch (IOException e) {
              log.error("Can not write data to the local file due to error: ", e);
            }
          });

      if (oneFilePerIteration) {
        this.dataFileWriter.close();
        pushLocalFileToADLS(localDirectory + currentFileName,
            currentFileName);
        FileUtils.deleteLocalFile(localDirectory + currentFileName);
      } else {
        this.dataFileWriter.flush();
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
        DataFileStream<GenericRecord> dataFileStream =
            new DataFileReader<>(file, new GenericDatumReader<>());
        AvroUtils.setBasicFields(fields, dataFileStream.getSchema());
        dataFileStream.close();
      }
    } catch (IOException e) {
      log.error("Tried to read file : {} with no success :", this.localDirectory,
          e);
    }

    return new Model(fields, primaryKeys, tableNames, options, null);
  }


}
