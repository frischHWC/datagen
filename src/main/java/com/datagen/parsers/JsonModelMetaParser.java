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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

/**
 * Goal of this class is to be able to parse a codified file JSON and render a model meta based on it
 */
@SuppressWarnings("unchecked")
@Slf4j
public class JsonModelMetaParser {

  // Release 1.0.0 introduced possibility to have an owner and allowed users/groups to see and administer a model

  public static ModelStoreService.ModelMetaStored generateModelMetaFromFile(String jsonFilePath) {
    var modelMeta = new ModelStoreService.ModelMetaStored("");
    try {
      JsonNode root = new ObjectMapper().readTree(new File(jsonFilePath));
      log.debug("JSON file content is : {}", root.toPrettyString());

      modelMeta.setOwner(root.get("owner")!=null ? root.get("owner").asText("") : "");
      var allowedUsers = getNamesInJson(root, "allowed_users");
      allowedUsers.remove("");
      modelMeta.setAllowedUsers(allowedUsers);

      var allowedGroups = getNamesInJson(root, "allowed_groups");
      allowedGroups.remove("");
      modelMeta.setAllowedGroups(allowedGroups);

      var allowedAdminUsers = getNamesInJson(root, "allowed_admin_users");
      allowedAdminUsers.remove("");
      modelMeta.setAllowedAdminUsers(allowedAdminUsers);

      var allowedAdminGroups = getNamesInJson(root, "allowed_admin_groups");
      allowedAdminGroups.remove("");
      modelMeta.setAllowedAdminGroups(allowedAdminGroups);

    } catch (FileNotFoundException e) {
      log.info("JSON Model Meta File not found: {} hence will return an empty one", jsonFilePath);
    } catch (IOException e) {
      log.error(
          "Could not read JSON file: {}, please verify its structure, error is : ",
          jsonFilePath, e);
    } catch (NullPointerException e) {
      log.error(
          "Model Meta file has not been found at : {} , please verify it exists, error is: ",
          jsonFilePath, e);
    }
    return modelMeta;
  }


  /**
   * For a given JsonNode and a key, read in the jsonNode the corresponding key (considered as an array inside json)
   * and return a hash set of all its values
   * @param root
   * @param key
   * @return
   */
  private static HashSet<String> getNamesInJson(JsonNode root, String key) {
    var hashSet = new HashSet<String>();
    var oNode = root.findPath(key);
    if(oNode.isArray() && !oNode.isEmpty()) {
      Iterator<JsonNode> userGroupIterator = oNode.iterator();
      while (userGroupIterator.hasNext()) {
        var userGroup = userGroupIterator.next();
        hashSet.add(userGroup.asText(""));
      }
    }
    return hashSet;
  }


}
