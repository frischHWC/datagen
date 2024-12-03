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
import com.datagen.connector.storage.utils.CSVUtils;
import com.datagen.connector.storage.utils.FileUtils;
import com.datagen.model.Model;
import com.datagen.model.OptionsConverter;
import com.datagen.model.Row;
import com.datagen.model.type.Field;
import com.datagen.model.type.StringField;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;


@Slf4j
public class OzoneCSVConnector extends OzoneUtils implements ConnectorInterface {

  private FileOutputStream outputStream;
  private final String lineSeparator;
  private final Boolean oneFilePerIteration;
  private final Model model;
  private int counter;

  public OzoneCSVConnector(Model model,
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
        FileUtils.deleteAllLocalFiles(localFileTempDir, keyNamePrefix, "csv");

        if (!oneFilePerIteration) {
          this.outputStream = FileUtils.createLocalFileAsOutputStream(
              localFileTempDir + keyNamePrefix + ".csv");
          CSVUtils.appendCSVHeader(model, outputStream, lineSeparator);
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
        pushKeyToOzone(localFileTempDir + keyNamePrefix + ".csv", keyNamePrefix + ".csv");
      }
      closeOzone();
      FileUtils.deleteAllLocalFiles(localFileTempDir, keyNamePrefix, "csv");
    } catch (IOException e) {
      log.warn("Could not close properly Ozone connection, due to error: ", e);
    }
  }

  @Override
  public void sendOneBatchOfRows(List<Row> rows) {
    // Let's create a temp local file and then pushes it to ozone ?
    String keyName =
        keyNamePrefix + "-" + String.format("%010d", counter) + ".csv";
    // Write to local file
    if (oneFilePerIteration) {
      this.outputStream = FileUtils.createLocalFileAsOutputStream(localFileTempDir + keyName);
      CSVUtils.appendCSVHeader(model, outputStream, lineSeparator);
      counter++;
    }

    rows.stream().map(Row::toCSV).forEach(r -> {
      try {
        outputStream.write(r.getBytes());
        outputStream.write(lineSeparator.getBytes());
      } catch (IOException e) {
        log.error("Could not write row: " + r + " to file: " +
            outputStream.getChannel());
      }
    });
    try {
      outputStream.write(lineSeparator.getBytes());
    } catch (IOException e) {
      log.error("Can not write data to the local file due to error: ", e);
    }

    if (oneFilePerIteration) {
      try {
        outputStream.close();
      } catch (IOException e) {
        log.error(" Unable to close local file with error :", e);
      }

      pushKeyToOzone(localFileTempDir + keyName, keyName);
    }

  }

  @Override
  public Model generateModel(Boolean deepAnalysis) {
    LinkedHashMap<String, Field> fields = new LinkedHashMap<String, Field>();
    Map<String, List<String>> primaryKeys = new HashMap<>();
    Map<String, String> tableNames = new HashMap<>();
    Map<String, String> options = new HashMap<>();
    byte[] readBuffer = new byte[(int) 104857600];
    try {
      OzoneInputStream ozoneInputStream = this.bucket.readFile(this.keyNamePrefix);
      BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(ozoneInputStream));
      String csvHeader = bufferedReader.readLine();
      Arrays.stream(csvHeader.split(","))
          .forEach(f -> fields.put(f,
              new StringField(f, null, new HashMap<>())));
      bufferedReader.close();
      ozoneInputStream.close();
      ozClient.close();
    } catch (IOException e) {
      log.error(
          "Could not connect and read key: {} into Ozone, due to error: ",
          keyNamePrefix, e);
    }

    return new Model("",fields, primaryKeys, tableNames, options, null);
  }

}
