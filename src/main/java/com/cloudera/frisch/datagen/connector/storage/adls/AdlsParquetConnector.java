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
import com.cloudera.frisch.datagen.connector.storage.utils.FileUtils;
import com.cloudera.frisch.datagen.connector.storage.utils.ParquetUtils;
import com.cloudera.frisch.datagen.model.Model;
import com.cloudera.frisch.datagen.model.OptionsConverter;
import com.cloudera.frisch.datagen.model.Row;
import com.cloudera.frisch.datagen.model.type.Field;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * This is a Parquet connector to write to one or multiple Parquet files to ADLS
 */
@Slf4j
public class AdlsParquetConnector extends AdlsUtils implements ConnectorInterface  {

  private final Model model;
  private final Boolean oneFilePerIteration;

  private int counter;
  private String currentFileName;

  private Schema schema;
  private ParquetWriter<GenericRecord> parquetWriter;

  /**
   * Init ADLS Parquet
   */
  public AdlsParquetConnector(Model model,
                              Map<ApplicationConfigs, String> properties) {
    super(model, properties);
    this.model = model;
    this.counter = 0;
    this.oneFilePerIteration = (Boolean) model.getOptionsOrDefault(
        OptionsConverter.Options.ONE_FILE_PER_ITERATION);

  }

  @Override
  public void init(Model model, boolean writer) {
    if (writer) {
      this.schema = model.getAvroSchema();
      if ((Boolean) model.getOptionsOrDefault(
          OptionsConverter.Options.DELETE_PREVIOUS)) {
        deleteAllfiles(fileNamePrefix, "parquet");
      }

      // Will use a local directory before pushing data to S3
      FileUtils.createLocalDirectory(localDirectory);
      FileUtils.deleteAllLocalFiles(localDirectory, fileNamePrefix, "parquet");

      createDirectoryIfNotExists();

      if (!oneFilePerIteration) {
        this.currentFileName = currentFileName + ".parquet";
        this.parquetWriter = ParquetUtils.createLocalFileWithOverwrite(
            localDirectory +
                currentFileName, schema, this.parquetWriter, model);
      }
    } else {
      FileUtils.createLocalDirectory(localFilePathForModelGeneration);
    }
  }

  @Override
  public void terminate() {
    try {
      if (!oneFilePerIteration) {
        parquetWriter.close();
        pushLocalFileToADLS(localDirectory + currentFileName, currentFileName);
      }
    } catch (IOException e) {
      log.error(" Unable to close local file with error :", e);
    } finally {
      FileUtils.deleteAllLocalFiles(localDirectory, fileNamePrefix, "parquet");
    }
  }

  @Override
  public void sendOneBatchOfRows(List<Row> rows) {
    try {
      if (oneFilePerIteration) {
        this.currentFileName = fileNamePrefix + "-" + String.format("%010d", counter) + ".parquet";
        this.parquetWriter = ParquetUtils.createLocalFileWithOverwrite(
            localDirectory +
                currentFileName, schema, this.parquetWriter, model);
        counter++;
      }

      rows.stream().map(row -> row.toGenericRecord(schema))
          .forEach(genericRecord -> {
            try {
              parquetWriter.write(genericRecord);
            } catch (IOException e) {
              log.error("Can not write data to the local file due to error: ", e);
            }
          });

      if (oneFilePerIteration) {
        parquetWriter.close();
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
        ParquetFileReader parquetReader =
            ParquetFileReader.open(HadoopInputFile.fromPath(new Path(file.toURI()), new Configuration()));
        ParquetUtils.setBasicFields(fields, parquetReader);
        if (deepAnalysis) {
          ParquetUtils.analyzeFields(fields, parquetReader);
        }
        parquetReader.close();
        FileUtils.deleteLocalFile(localFile);
      }
    } catch (IOException e) {
      log.error("Tried to read file : {} with no success :", this.localDirectory,
          e);
    }

    return new Model(fields, primaryKeys, tableNames, options);
  }


}
