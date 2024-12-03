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
package com.datagen.parsers;


import com.datagen.service.model.ModelStoreService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;

/**
 * Goal of this class is to be able to parse a codified file JSON and render a model meta based on it
 */
@SuppressWarnings("unchecked")
@Slf4j
public class JsonModelMetaUnparser {

  // Release 1.0.0 introduced possibility to have an owner and allowed users/groups to see and administer a model

  /**
   * Render a model as a JSON file
   *
   * @return
   */
  public static String renderFileFromModelMeta(ModelStoreService.ModelMetaStored modelMetaStored, String pathToWriteModelMeta, String modelName) {
    String json = "NOT ABLE TO RENDER MODEL METADATA";
    ObjectMapper mapper = new ObjectMapper();
    mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    ObjectNode root = mapper.createObjectNode();

    root.put("model_name", modelName);

    root.put("owner", modelMetaStored.getOwner()!=null?modelMetaStored.getOwner():"anonymous");

    var allowedUserArray = root.putArray("allowed_users");
    if(modelMetaStored.getAllowedUsers()!=null) {
      modelMetaStored.getAllowedUsers().forEach(allowedUserArray::add);
    }
    var allowedGroupArray = root.putArray("allowed_groups");
    if(modelMetaStored.getAllowedGroups()!=null) {
      modelMetaStored.getAllowedGroups().forEach(allowedGroupArray::add);
    }
    var allowedUserAdminArray = root.putArray("allowed_admin_users");
    if(modelMetaStored.getAllowedAdminUsers()!=null) {
      modelMetaStored.getAllowedAdminUsers()
          .forEach(allowedUserAdminArray::add);
    }
    var allowedGroupAdminArray = root.putArray("allowed_admin_groups");
    if(modelMetaStored.getAllowedAdminGroups()!=null) {
      modelMetaStored.getAllowedAdminGroups()
          .forEach(allowedGroupAdminArray::add);
    }

    try {
      json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(root);
      if(pathToWriteModelMeta!=null) {
        mapper.writeValue(new File(pathToWriteModelMeta), root);
      }
    } catch (JsonProcessingException e) {
      log.warn("Not able to render model with error: ", e);
    } catch (IOException e) {
      log.warn("Not able to write model with error: ", e);
    }

    return json;
  }


}
