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
package com.datagen.service.api;


import com.datagen.config.ApplicationConfigs;
import com.datagen.config.PropertiesLoader;
import com.datagen.model.Row;
import com.datagen.service.model.ModelStoreService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;


@Service
@Slf4j
public class APIService {

  @Autowired
  private ModelStoreService modelStoreService;

  private PropertiesLoader propertiesLoader;
  private Map<ApplicationConfigs, String> properties;

  @Autowired
  public APIService(PropertiesLoader propertiesLoader) {
    this.propertiesLoader = propertiesLoader;
    this.properties = propertiesLoader.getPropertiesCopy();
  }

  public String saveModel(@Nullable MultipartFile modelFileAsFile,
                          @Nullable String modelFilePath) {
    log.info("Starting Generation");
    long start = System.currentTimeMillis();

    String modelFile = modelFilePath;
    if (modelFilePath == null) {
      return "{ \"commandUuid\": \"\" , \"error\": \"Error: No model file passed\" }";
    }
    if (!modelFilePath.contains("/")) {
      log.info(
          "Model file passed is identified as one of the one provided, so will look for it in data model path: {} ",
          properties.get(ApplicationConfigs.DATAGEN_MODEL_PATH));
      modelFile = properties.get(ApplicationConfigs.DATAGEN_MODEL_PATH) +
          modelFilePath;
    }
    if (modelFileAsFile != null && !modelFileAsFile.isEmpty()) {
      log.info("Model passed is an uploaded file");
      modelFile = properties.get(ApplicationConfigs.DATAGEN_MODEL_RECEIVED_PATH)+ "/model-test-" + new Random().nextInt() + ".json";
    log.debug("Transferring model to model store");
    try {
      modelFileAsFile.transferTo(new File(modelFile));
    } catch (IOException e) {
      log.error(
          "Could not save model file passed in request locally, due to error:",
          e);
      return "{ \"commandUuid\": \"\" , \"error\": \"Error with Model File - Cannot save it locally\" }";
    }
  }
    return modelStoreService.addModel(modelFile, false).getName();
  }

  public String generateData(String modelId) {
    List<Row> randomDataList = modelStoreService.getModel(modelId).generateRandomRows(1, 1);
    return randomDataList.get(0).toJSON();
  }


}
