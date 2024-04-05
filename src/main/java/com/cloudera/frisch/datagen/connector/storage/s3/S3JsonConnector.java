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
package com.cloudera.frisch.datagen.connector.storage.s3;


import com.cloudera.frisch.datagen.config.ApplicationConfigs;
import com.cloudera.frisch.datagen.connector.ConnectorInterface;
import com.cloudera.frisch.datagen.connector.storage.utils.FileUtils;
import com.cloudera.frisch.datagen.model.Model;
import com.cloudera.frisch.datagen.model.OptionsConverter;
import com.cloudera.frisch.datagen.model.Row;
import com.cloudera.frisch.datagen.model.type.Field;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.cloudera.frisch.datagen.config.ApplicationConfigs.DATA_HOME_DIRECTORY;

/**
 * This is a JSON connector to write to one or multiple JSON files to S3
 */
@Slf4j
public class S3JsonConnector extends S3Utils implements ConnectorInterface  {

  private final Model model;
  private FileOutputStream outputStream;
  private final String lineSeparator;
  private final Boolean oneFilePerIteration;
  private final String localFilePathForModelGeneration;

  private int counter;
  private String currentLocalFileName;
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
    this.localFilePathForModelGeneration = properties.get(DATA_HOME_DIRECTORY) + "/model-gen/s3/";
    this.lineSeparator = System.getProperty("line.separator");
  }

  @Override
  public void init(Model model, boolean writer) {
    if (writer) {
      if ((Boolean) model.getOptionsOrDefault(
          OptionsConverter.Options.DELETE_PREVIOUS)) {
        s3Client.listObjects(
                ListObjectsRequest.builder().bucket(bucketName)
                    .prefix(localDirectoryName)
                    .build())
            .contents()
            .forEach(k -> s3Client.deleteObject(
                DeleteObjectRequest.builder().bucket(bucketName).key(k.key())
                    .build()));
      }

      // Will use a local directory before pushing data to S3
      FileUtils.createLocalDirectory(localFileTempDir);
      FileUtils.deleteAllLocalFiles(localFileTempDir, keyNamePrefix, "json");

      createBucketIfNotExists();

      if (!oneFilePerIteration) {
        this.currentLocalFileName = localFileNamePrefix + ".json";
        this.currentKeyName = localDirectoryName + currentLocalFileName;
        this.outputStream = FileUtils.createLocalFileAsOutputStream(localFileTempDir +
            currentLocalFileName);
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
        pushLocalFileToS3(localFileTempDir + currentLocalFileName, currentKeyName);
      }
    } catch (IOException e) {
      log.error(" Unable to close local file with error :", e);
    } finally {
      FileUtils.deleteAllLocalFiles(localFileTempDir, localFileNamePrefix, "json");
    }
  }

  @Override
  public void sendOneBatchOfRows(List<Row> rows) {
    try {
      if (oneFilePerIteration) {
        this.currentLocalFileName = localFileNamePrefix + "-" + String.format("%010d", counter) + ".json";
        this.currentKeyName = localDirectoryName + currentLocalFileName;
        this.outputStream = FileUtils.createLocalFileAsOutputStream(
            localFileTempDir + currentLocalFileName);
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
        pushLocalFileToS3(localFileTempDir + currentLocalFileName, currentKeyName);
        FileUtils.deleteLocalFile(localFileTempDir + currentLocalFileName);
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
      String localFile = this.localFilePathForModelGeneration + this.localFileNamePrefix;
      readFileFromS3(localFile, this.keyNamePrefix);
      File file = new File(localFile);
      if (file.exists() && file.isFile()) {
        // TODO : Implement logic to create a model with at least names, pk, options and column names/types

      }
    } catch (Exception e) {
      log.error("Tried to read file : {} with no success :", this.localDirectoryName,
          e);
    }

    return new Model(fields, primaryKeys, tableNames, options);
  }


}