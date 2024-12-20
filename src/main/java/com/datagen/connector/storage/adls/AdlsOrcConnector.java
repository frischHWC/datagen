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
import com.datagen.connector.storage.utils.FileUtils;
import com.datagen.connector.storage.utils.OrcUtils;
import com.datagen.model.Model;
import com.datagen.model.OptionsConverter;
import com.datagen.model.Row;
import com.datagen.model.type.Field;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * This is a ORC connector to write to one or multiple ORC files to ADLS
 */
@Slf4j
public class AdlsOrcConnector extends AdlsUtils implements ConnectorInterface  {

  private final Model model;
  private final Boolean oneFilePerIteration;

  private int counter;
  private String currentFileName;

  private final TypeDescription schema;
  private Writer orcWriter;
  private final Map<String, ColumnVector> vectors;
  private final VectorizedRowBatch batch;

  /**
   * Init ADLS ORC
   */
  public AdlsOrcConnector(Model model,
                          Map<ApplicationConfigs, String> properties) {
    super(model, properties);
    this.model = model;
    this.counter = 0;
    this.oneFilePerIteration = (Boolean) model.getOptionsOrDefault(
        OptionsConverter.Options.ONE_FILE_PER_ITERATION);
    this.schema = model.getOrcSchema();
    this.batch = schema.createRowBatch();
    this.vectors = model.createOrcVectors(batch);
  }

  @Override
  public void init(Model model, boolean writer) {
    if (writer) {
      if ((Boolean) model.getOptionsOrDefault(
          OptionsConverter.Options.DELETE_PREVIOUS)) {
        deleteAllfiles(fileNamePrefix, "orc");
      }

      // Will use a local directory before pushing data to ADLS
      FileUtils.createLocalDirectory(localDirectory);
      FileUtils.deleteAllLocalFiles(localDirectory, fileNamePrefix, "orc");

      createDirectoryIfNotExists();

      if (!oneFilePerIteration) {
        this.currentFileName = fileNamePrefix + ".orc";
        this.orcWriter = OrcUtils.createLocalFileWithOverwrite(localDirectory +
            currentFileName, this.orcWriter, this.schema);
      }
    } else {
      FileUtils.createLocalDirectory(localFilePathForModelGeneration);
    }
  }

  @Override
  public void terminate() {
    try {
      if (!oneFilePerIteration) {
        this.orcWriter.close();
        pushLocalFileToADLS(localDirectory + currentFileName, currentFileName);
      }
    } catch (IOException e) {
      log.error(" Unable to close local file with error :", e);
    } finally {
      FileUtils.deleteAllLocalFiles(localDirectory, fileNamePrefix, "orc");
    }
  }

  @Override
  public void sendOneBatchOfRows(List<Row> rows) {
    try {
      if (oneFilePerIteration) {
        this.currentFileName = fileNamePrefix + "-" + String.format("%010d", counter) + ".orc";
        this.orcWriter = OrcUtils.createLocalFileWithOverwrite(localDirectory +
            currentFileName, this.orcWriter, this.schema);
        counter++;
      }

      for (Row row : rows) {
        int rowNumber = batch.size++;
        row.fillinOrcVector(rowNumber, vectors);
        try {
          if (batch.size == batch.getMaxSize()) {
            orcWriter.addRowBatch(batch);
            batch.reset();
          }
        } catch (IOException e) {
          log.error("Can not write data to the local file due to error: ", e);
        }
      }
      try {
        if (batch.size != 0) {
          orcWriter.addRowBatch(batch);
          batch.reset();
        }
      } catch (IOException e) {
        log.error("Can not write data to the local file due to error: ", e);
      }

      if (oneFilePerIteration) {
        this.orcWriter.close();
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
        Reader reader =
            OrcFile.createReader(new Path(localFile),
                OrcFile.readerOptions(new Configuration()));

        OrcUtils.setBasicFields(fields, reader);
        if (deepAnalysis) {
          OrcUtils.analyzeFields(fields, reader);
        }
      }
    } catch (IOException e) {
      log.error("Tried to read file : {} with no success :", this.localDirectory,
          e);
    }

    return new Model("",fields, primaryKeys, tableNames, options, null);
  }


}
