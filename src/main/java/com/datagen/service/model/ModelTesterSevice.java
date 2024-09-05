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
package com.datagen.service.model;


import com.datagen.config.ApplicationConfigs;
import com.datagen.config.PropertiesLoader;
import com.datagen.model.Model;
import com.datagen.model.Row;
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
public class ModelTesterSevice {

  @Autowired
  private ModelStoreService modelStoreService;

  @Autowired
  private PropertiesLoader propertiesLoader;

  public String generateData(@Nullable MultipartFile modelFileAsFile,
                             @Nullable String modelName) {
    log.info("Starting Generation");
    long start = System.currentTimeMillis();

    // Get default values if some are not set
    Map<ApplicationConfigs, String> properties =
        propertiesLoader.getPropertiesCopy();

    var modelFile = "";
    Model model;
    var tempModel = false;

    if (modelName != null) {
      log.info(
          "Model file passed is identified as one of the one provided, so will look for in registered models");
      model = modelStoreService.getModel(modelName);
    }else if (modelFileAsFile != null && !modelFileAsFile.isEmpty()) {
      log.info("Model passed is an uploaded file (and so declared as temporary)");
      tempModel = true;
      modelFile = properties.get(ApplicationConfigs.DATAGEN_MODEL_RECEIVED_PATH) +
          "/model-test-" + new Random().nextInt() + ".json";
      try {
        modelFileAsFile.transferTo(new File(modelFile));
      } catch (IOException e) {
        log.error(
            "Could not save model file passed in request locally, due to error:",
            e);
        return "{ \"commandUuid\": \"\" , \"error\": \"Error with Model File - Cannot save it locally\" }";
      }
      // Saving model and rendering one row
      model = modelStoreService.addModel(modelFile, false);
    } else {
      return "{ \"commandUuid\": \"\" , \"error\": \"Error Provide a model file or an existing model named\" }";
    }
    List<Row> rows = model.generateRandomRows(1, 1);

    if(tempModel) {
      modelStoreService.deleteModel(model.getName());
    }

    return rows.get(0).toPrettyJSONAllFields();
  }
}
