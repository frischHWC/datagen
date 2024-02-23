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

import com.cloudera.frisch.datagen.connector.ConnectorInterface;
import com.cloudera.frisch.datagen.connector.storage.utils.OrcUtils;
import com.cloudera.frisch.datagen.model.type.*;
import com.cloudera.frisch.datagen.utils.Utils;
import com.cloudera.frisch.datagen.config.ApplicationConfigs;
import com.cloudera.frisch.datagen.model.Model;
import com.cloudera.frisch.datagen.model.OptionsConverter;
import com.cloudera.frisch.datagen.model.Row;
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
import java.util.*;


/**
 * ORC File sink
 */
@SuppressWarnings("unchecked")
@Slf4j
public class ORCConnector implements ConnectorInterface {

  private TypeDescription schema;
  private Writer writer;
  private Map<String, ColumnVector> vectors;
  private VectorizedRowBatch batch;
  private int counter;
  private final Model model;
  private final String directoryName;
  private final String fileName;
  private final Boolean oneFilePerIteration;


  /**
   * Init local ORC file
   */
  public ORCConnector(Model model, Map<ApplicationConfigs, String> properties) {
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
      schema = model.getOrcSchema();
      batch = schema.createRowBatch();
      vectors = model.createOrcVectors(batch);

      Utils.createLocalDirectory(directoryName);

      if ((Boolean) model.getOptionsOrDefault(
          OptionsConverter.Options.DELETE_PREVIOUS)) {
        Utils.deleteAllLocalFiles(directoryName, fileName, "orc");
      }

      if (!oneFilePerIteration) {
        creatFileWithOverwrite(directoryName + fileName + ".orc");
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
    } catch (NullPointerException e) {
      log.info("Writer was already closed");
    }
  }

  @Override
  public void sendOneBatchOfRows(List<Row> rows) {
    if (oneFilePerIteration) {
      creatFileWithOverwrite(
          directoryName + fileName + "-" + String.format("%010d", counter) +
              ".orc");
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
        log.error("Can not write data to the local file due to error: ", e);
      }
    }

    try {
      if (batch.size != 0) {
        writer.addRowBatch(batch);
        batch.reset();
      }
    } catch (IOException e) {
      log.error("Can not write data to the local file due to error: ", e);
    }

    if ((Boolean) model.getOptionsOrDefault(
        OptionsConverter.Options.ONE_FILE_PER_ITERATION)) {
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

    try {
      Reader reader = OrcFile.createReader(new Path(this.directoryName),
          OrcFile.readerOptions(new Configuration()));

      OrcUtils.setBasicFields(fields, reader);
      if (deepAnalysis) {
        OrcUtils.analyzeFields(fields, reader);
      }

    } catch (IOException e) {
      log.warn("Could not create reader to ORC local file due to error:", e);
    }

    return new Model(fields, primaryKeys, tableNames, options);
  }

  private void creatFileWithOverwrite(String path) {
    try {
      Utils.deleteLocalFile(path);
      new File(path).getParentFile().mkdirs();
      writer = OrcFile.createWriter(new Path(path),
          OrcFile.writerOptions(new Configuration())
              .setSchema(schema));
    } catch (IOException e) {
      log.warn("Could not create writer to ORC local file due to error:", e);
    }
  }

}