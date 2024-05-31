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
package com.cloudera.frisch.datagen.parsers;


import com.cloudera.frisch.datagen.model.Model;
import com.cloudera.frisch.datagen.model.type.Field;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;

/**
 * Goal of this class is to be able to parse a codified file JSON and render a model based on it
 */
@SuppressWarnings("unchecked")
@Slf4j
public class JsonUnparser implements UnParser {

  private final ObjectMapper mapper;

  public JsonUnparser() {
    this.mapper = new ObjectMapper();
    mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
  }

  /**
   * Render a model as a JSON file
   *
   * @return
   */
  public String renderFileFromModel(Model model, String pathToWriteModel) {
    String json = "NOT ABLE TO RENDER MODEL";
    ObjectNode root = mapper.createObjectNode();

    root.putPOJO("Table_Names", model.getTableNames());
    root.putPOJO("Options", model.getOptions());

    ArrayNode fieldsArray = root.putArray("Fields");
    model.getFields().forEach((name, f) -> {
      Field field = (Field) f;
      ObjectNode objectNode = fieldsArray.addObject();
      objectNode.put("name", (String) name);
      objectNode.put("type", ((Field<?>) f).getTypeForModel());
      if (field.getMax() != null && field.getMax()!=Integer.MAX_VALUE && field.getMax()!=Long.MAX_VALUE && (field.getMax()!=Long.MAX_VALUE-1)) {
        objectNode.put("max", field.getMax());
      }
      if (field.getMin() != null && field.getMin()!=Integer.MIN_VALUE && field.getMin()!=Long.MIN_VALUE) {
        objectNode.put("min", field.getMin());
      }
      if (field.isGhost()) {
        objectNode.put("ghost", "true");
      }
      if (field.getLength()!=-1) {
        objectNode.put("length", field.getLength());
      }
      if (field.getPossibleValues() != null &&
          !field.getPossibleValues().isEmpty()) {
        ArrayNode possileValuesNode = objectNode.putArray("possible_values");
        field.getPossibleValues()
            .forEach(pv -> possileValuesNode.add(pv.toString()));
      }
    });

    try {
      json = this.mapper.writeValueAsString(root);
      this.mapper.writeValue(new File(pathToWriteModel), root);
    } catch (JsonProcessingException e) {
      log.warn("Not able to render model with error: ", e);
    } catch (IOException e) {
      log.warn("Not able to write model with error: ", e);
    }

    return json;
  }


}
