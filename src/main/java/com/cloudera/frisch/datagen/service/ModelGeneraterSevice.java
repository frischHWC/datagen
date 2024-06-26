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
package com.cloudera.frisch.datagen.service;


import com.cloudera.frisch.datagen.config.ApplicationConfigs;
import com.cloudera.frisch.datagen.config.ConnectorParser;
import com.cloudera.frisch.datagen.config.PropertiesLoader;
import com.cloudera.frisch.datagen.connector.ConnectorInterface;
import com.cloudera.frisch.datagen.connector.ConnectorsUtils;
import com.cloudera.frisch.datagen.connector.storage.utils.FileUtils;
import com.cloudera.frisch.datagen.model.Model;
import com.cloudera.frisch.datagen.model.OptionsConverter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;
import java.util.Random;


@Service
@Slf4j
public class ModelGeneraterSevice {

  private PropertiesLoader propertiesLoader;

  @Autowired
  public ModelGeneraterSevice(PropertiesLoader propertiesLoader) {
    this.propertiesLoader = propertiesLoader;

    FileUtils.createLocalDirectory(propertiesLoader.getPropertiesCopy()
        .get(ApplicationConfigs.DATA_MODEL_GENERATED_PATH));
    FileUtils.createLocalDirectory(propertiesLoader.getPropertiesCopy()
        .get(ApplicationConfigs.DATA_MODEL_PATH_DEFAULT));
  }

  public String generateModel(
      String source,
      Boolean deepAnalysis,
      @Nullable String filepath,
      @Nullable String database,
      @Nullable String table,
      @Nullable String volume,
      @Nullable String bucket,
      @Nullable String key
  ) {
    Map<ApplicationConfigs, String> properties =
        propertiesLoader.getPropertiesCopy();

    Model modelEmpty = new Model();
    Map<OptionsConverter.TableNames, String> tableNames = modelEmpty.getTableNames();
    if(source.contains("hdfs")) {
      tableNames.put(OptionsConverter.TableNames.HDFS_FILE_PATH, filepath);
    } else if(source.contains("hive")) {
      tableNames.put(OptionsConverter.TableNames.HIVE_DATABASE, database);
      tableNames.put(OptionsConverter.TableNames.HIVE_TABLE_NAME, table);
    } else if(source.contains("ozone")) {
      tableNames.put(OptionsConverter.TableNames.OZONE_VOLUME, volume);
      tableNames.put(OptionsConverter.TableNames.OZONE_BUCKET, bucket);
      tableNames.put(OptionsConverter.TableNames.OZONE_KEY_NAME, key);
    } else if(source.contains("s3")) {
      tableNames.put(OptionsConverter.TableNames.S3_BUCKET, bucket);
      tableNames.put(OptionsConverter.TableNames.S3_KEY_NAME, key);
    } else if(source.contains("adls")) {
      tableNames.put(OptionsConverter.TableNames.ADLS_CONTAINER, bucket);
      tableNames.put(OptionsConverter.TableNames.ADLS_FILE_NAME, key);
    } else if(source.contains("gcs")) {
      tableNames.put(OptionsConverter.TableNames.GCS_BUCKET, bucket);
      tableNames.put(OptionsConverter.TableNames.GCS_OBJECT_NAME, key);
    }
    else {
      tableNames.put(OptionsConverter.TableNames.LOCAL_FILE_PATH, filepath);
    }

    String outputPath = properties.get(ApplicationConfigs.DATA_MODEL_GENERATED_PATH) +
            "/model-generated-" + new Random().nextInt() + ".json";

    ConnectorInterface connector = ConnectorsUtils
        .connectorInit(modelEmpty, properties,
            Collections.singletonList(ConnectorParser.stringToConnector(source)), false)
        .get(0);

    return connector.generateModel(deepAnalysis).toJsonSchema(outputPath);
  }
}
