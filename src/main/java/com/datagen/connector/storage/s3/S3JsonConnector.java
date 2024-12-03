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
package com.datagen.connector.storage.s3;


import com.datagen.config.ApplicationConfigs;
import com.datagen.connector.ConnectorInterface;
import com.datagen.connector.storage.utils.FileUtils;
import com.datagen.model.Model;
import com.datagen.model.OptionsConverter;
import com.datagen.model.Row;
import com.datagen.model.type.Field;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * This is a JSON connector to write to one or multiple JSON files to S3
 */
@Slf4j
public class S3JsonConnector extends S3Utils implements ConnectorInterface  {

  private final Model model;
  private FileOutputStream outputStream;
  private final String lineSeparator;
  private final Boolean oneFilePerIteration;

  private int counter;
  private String currentKeyName;

  /**
   * Init S3 JSON
   */
  public S3JsonConnector(Model model,
                         Map<ApplicationConfigs, String> properties) {
    super(model, properties);
    this.model = model;
    this.counter = 0;
    this.oneFilePerIteration = (Boolean) model.getOptionsOrDefault(
        OptionsConverter.Options.ONE_FILE_PER_ITERATION);
    this.lineSeparator = System.getProperty("line.separator");
  }

  @Override
  public void init(Model model, boolean writer) {
    if (writer) {
      if ((Boolean) model.getOptionsOrDefault(
          OptionsConverter.Options.DELETE_PREVIOUS)) {
        deleteAllfiles(keyNamePrefix, "json");
      }

      // Will use a local directory before pushing data to S3
      FileUtils.createLocalDirectory(localDirectoryName);
      FileUtils.deleteAllLocalFiles(localDirectoryName, keyNamePrefix, "json");

      createBucketIfNotExists();

      if (!oneFilePerIteration) {
        this.currentKeyName = keyNamePrefix + ".json";
        this.outputStream = FileUtils.createLocalFileAsOutputStream(localDirectoryName +
            currentKeyName);
      }
    } else {
      FileUtils.createLocalDirectory(localFilePathForModelGeneration);
    }
  }

  @Override
  public void terminate() {
    try {
      if (!oneFilePerIteration) {
        outputStream.close();
        pushLocalFileToS3(localDirectoryName + currentKeyName, currentKeyName);
      }
    } catch (IOException e) {
      log.error(" Unable to close local file with error :", e);
    } finally {
      FileUtils.deleteAllLocalFiles(localDirectoryName, keyNamePrefix, "json");
      closeS3();
    }
  }

  @Override
  public void sendOneBatchOfRows(List<Row> rows) {
    try {
      if (oneFilePerIteration) {
        this.currentKeyName = keyNamePrefix + "-" + String.format("%010d", counter) + ".json";
        this.outputStream = FileUtils.createLocalFileAsOutputStream(
            localDirectoryName + currentKeyName);
        counter++;
      }

      rows.stream().map(Row::toJSON).forEach(r -> {
        try {
          outputStream.write(r.getBytes());
          outputStream.write(lineSeparator.getBytes());
        } catch (IOException e) {
          log.error("Could not write row: " + r + " to file: " +
              outputStream.getChannel());
        }
      });
      outputStream.write(lineSeparator.getBytes());

      if (oneFilePerIteration) {
        outputStream.close();
        pushLocalFileToS3(localDirectoryName + currentKeyName, currentKeyName);
        FileUtils.deleteLocalFile(localDirectoryName + currentKeyName);
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

    tableNames.put("S3_LOCAL_FILE_PATH", this.localDirectoryName);
    tableNames.put("S3_KEY_NAME", this.keyNamePrefix);
    tableNames.put("S3_BUCKET", this.bucketName);

    try {
      String localFile = this.localFilePathForModelGeneration + this.keyNamePrefix;
      readFileFromS3(localFile, this.keyNamePrefix);
      File file = new File(localFile);
      if (file.exists() && file.isFile()) {
        // TODO : Implement logic to create a model with at least names, pk, options and column names/types

      }
    } catch (Exception e) {
      log.error("Tried to read file : {} with no success :", this.localDirectoryName,
          e);
    }

    return new Model("",fields, primaryKeys, tableNames, options, null);
  }


}
