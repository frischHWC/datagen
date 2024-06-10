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
package com.datagen.connector.storage.ozone;


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


// TODO: Refactor to use one abstract class
@Slf4j
public class OzoneJsonConnector extends OzoneUtils implements ConnectorInterface {

  private FileOutputStream outputStream;
  private final String lineSeparator;

  private final Boolean oneFilePerIteration;
  private final Model model;
  private int counter;


  public OzoneJsonConnector(Model model,
                            Map<ApplicationConfigs, String> properties) {
    super(model, properties);
    this.lineSeparator = System.getProperty("line.separator");
    this.oneFilePerIteration = (Boolean) model.getOptionsOrDefault(
        OptionsConverter.Options.ONE_FILE_PER_ITERATION);
    this.model = model;
    this.counter = 0;
  }

  @Override
  public void init(Model model, boolean writer) {
    if (writer) {
      try {

        if ((Boolean) model.getOptionsOrDefault(
            OptionsConverter.Options.DELETE_PREVIOUS)) {
          deleteEverythingUnderABucket();
        }
        createVolumeIfItDoesNotExist();
        this.volume = objectStore.getVolume(volumeName);
        createBucketIfNotExist();
        this.bucket = volume.getBucket(bucketName);

        // Will use a local directory before pushing data to Ozone
        FileUtils.createLocalDirectory(localFileTempDir);
        FileUtils.deleteAllLocalFiles(localFileTempDir, keyNamePrefix,
            "json");

        if (!oneFilePerIteration) {
          this.outputStream = FileUtils.createLocalFileAsOutputStream(
              localFileTempDir + keyNamePrefix + ".json");
        }

      } catch (IOException e) {
        log.error(
            "Could not connect and create Volume into Ozone, due to error: ",
            e);
      }

    }
  }

  @Override
  public void terminate() {
    try {
      if (!oneFilePerIteration) {
        outputStream.close();
        // Send local file to Ozone
        pushKeyToOzone(localFileTempDir + keyNamePrefix + ".json", keyNamePrefix + ".json");
      }
      closeOzone();
      FileUtils.deleteAllLocalFiles(localFileTempDir, keyNamePrefix, "json");
    } catch (IOException e) {
      log.warn("Could not close properly Ozone connection, due to error: ", e);
    }
  }

  @Override
  public void sendOneBatchOfRows(List<Row> rows) {
    // Let's create a temp local file and then pushes it to ozone ?
    String keyName =
        keyNamePrefix + "-" + String.format("%010d", counter) + ".json";
    // Write to local file
    if (oneFilePerIteration) {
      this.outputStream = FileUtils.createLocalFileAsOutputStream(localFileTempDir + keyName);
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

    if (oneFilePerIteration) {
      try {
        outputStream.close();
      } catch (IOException e) {
        log.error(" Unable to close local file with error :", e);
      }

      // Send local file to Ozone
      pushKeyToOzone(localFileTempDir + keyName, keyName);
    }

  }

  @Override
  public Model generateModel(Boolean deepAnalysis) {
    LinkedHashMap<String, Field> fields = new LinkedHashMap<String, Field>();
    Map<String, List<String>> primaryKeys = new HashMap<>();
    Map<String, String> tableNames = new HashMap<>();
    Map<String, String> options = new HashMap<>();
    // TODO : Implement logic to create a model with at least names, pk, options and column names/types
    return new Model(fields, primaryKeys, tableNames, options, null);
  }

  private void createLocalFileWithOverwrite(String path) {
    try {
      File file = new File(path);
      file.getParentFile().mkdirs();
      if (!file.createNewFile()) {
        log.warn("Could not create file: {}", path);
      }
      outputStream = new FileOutputStream(path, false);
      log.debug("Successfully created local file : " + path);

    } catch (IOException e) {
      log.error(
          "Tried to create Parquet local file : " + path + " with no success :",
          e);
    }
  }


}
