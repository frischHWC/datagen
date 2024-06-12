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


import com.datagen.config.ApplicationConfigs;
import com.datagen.model.Model;
import com.datagen.model.type.Field;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Goal of this class is to be able to parse a codified file JSON and render a model based on it
 */
@SuppressWarnings("unchecked")
@Slf4j
public class JsonParser<T extends Field> implements Parser {

  @Getter
  private JsonNode root;

  public JsonParser(String jsonFilePath) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      log.info("Model used is from Json file : {} ", jsonFilePath);
      root = mapper.readTree(new File(jsonFilePath));
      log.debug("JSON file content is : {}", root.toPrettyString());
    } catch (IOException e) {
      log.error(
          "Could not read JSON file: {}, please verify its structure, error is : ",
          jsonFilePath, e);
    } catch (NullPointerException e) {
      log.error(
          "Model file has not been found at : {} , please verify it exists, error is: ",
          jsonFilePath, e);
    }
  }

  /**
   * Creates a model and populate it by reading the JSON file provided as argument of the constructor
   *
   * @return Model instantiated and populated
   */
  public Model renderModelFromFile(Map<ApplicationConfigs, String> properties) {

    // Release 0.4.15 introduced an easier format with PK, TB & Options being just one JSON node instead of an array
    // But we need to keep working wih old format for retro-compatbility.  (Fields is untouched)
    Map<String, List<String>> pks = new HashMap<>();
    JsonNode pkNode = root.findPath("Primary_Keys");
    if(pkNode.isArray() && !pkNode.isEmpty()) {
      log.debug("Detected array format (old version) for Primary Keys");
      Iterator<JsonNode> pksIterator = pkNode.elements();
      while (pksIterator.hasNext()) {
        addPrimaryKeyToHashMap(pks, pksIterator.next());
      }
    } else if(!pkNode.isEmpty()) {
      log.debug("Detected node format for Primary Keys");
      addPrimaryKeyToHashMap(pks, pkNode);
    }

    Map<String, String> tbs = new HashMap<>();
    JsonNode tbNode = root.findPath("Table_Names");
    if(tbNode.isArray() && !tbNode.isEmpty()) {
      log.debug("Detected array format (old version) for Table Names");
      Iterator<JsonNode> tablesIterator = tbNode.elements();
      while (tablesIterator.hasNext()) {
        addTableNameToHashMap(tbs, tablesIterator.next());
      }
    } else if(!tbNode.isEmpty()) {
      log.debug("Detected node format for Table Names");
      addTableNameToHashMap(tbs, tbNode);
    }

    Map<String, String> opsMap = new HashMap<>();
    JsonNode oNode = root.findPath("Options");
    if(oNode.isArray() && !oNode.isEmpty()) {
      log.debug("Detected array format (old version) for Options");
      Iterator<JsonNode> optionsIterator = oNode.elements();
      while (optionsIterator.hasNext()) {
        addOptionToHashMap(opsMap, optionsIterator.next());
      }
    } else if(!oNode.isEmpty()) {
      log.debug("Detected node format for Options");
      addOptionToHashMap(opsMap, oNode);
    }

    Iterator<JsonNode> fieldsIterator = root.findPath("Fields").elements();
    LinkedHashMap<String, T> fields = new LinkedHashMap<>();

    Map<String, String> hbaseFamilyColsMap = new HashMap<>();
    try {
      hbaseFamilyColsMap =
          mapColNameToColQual(opsMap.get("HBASE_COLUMN_FAMILIES_MAPPING"));
    } catch (Exception e) {
      log.info(
          "Could not get any column qualifier, defaulting to 'cq', check you are not using HBase ");
    }

    while (fieldsIterator.hasNext()) {
      JsonNode fieldNode = fieldsIterator.next();
      T field = getOneField(fieldNode, properties, hbaseFamilyColsMap);
      if (field != null) {
        fields.put(fieldNode.get("name").asText(), field);
      }
    }

    return new Model(fields, pks, tbs, opsMap, properties);
  }


  /**
   * Get one field from a Json node by instantiating the right type of field and return it
   * Note that if no length is precised in the JSON, it is automatically handled
   *
   * @param jsonField
   * @return
   */
  private T getOneField(JsonNode jsonField, Map<ApplicationConfigs, String> properties, Map<String, String> opsMap) {
    String name;
    try {
      name = jsonField.get("name").asText();
    } catch (NullPointerException e) {
      name = "UNDEFINED_COL_NAME";
    }

    String type;
    try {
      type = jsonField.get("type").asText();
    } catch (NullPointerException e) {
      type = "UNDEFINED_TYPE";
    }

    Integer length;
    try {
      length = jsonField.get("length").asInt();
    } catch (NullPointerException e) {
      length = null;
    }

    String min;
    try {
      min = jsonField.get("min").asText();
    } catch (NullPointerException e) {
      min = null;
    }

    String max;
    try {
      max = jsonField.get("max").asText();
    } catch (NullPointerException e) {
      max = null;
    }

    String file;
    try {
      file = jsonField.get("file").asText();
    } catch (NullPointerException e) {
      file = null;
    }

    String separator;
    try {
      separator = jsonField.get("separator").asText();
    } catch (NullPointerException e) {
      separator = ",";
    }

    boolean ghost;
    try {
      ghost = jsonField.get("ghost").asBoolean();
    } catch (NullPointerException e) {
      ghost = false;
    }

    String formula;
    try {
      formula = jsonField.get("formula").asText();
    } catch (NullPointerException e) {
      formula = null;
    }

    String injection;
    try {
      injection = jsonField.get("injection").asText();
    } catch (NullPointerException e) {
      injection = null;
    }

    String field;
    try {
      field = jsonField.get("field").asText();
    } catch (NullPointerException e) {
      field = null;
    }

    String pattern;
    try {
      pattern = jsonField.get("pattern").asText();
    } catch (NullPointerException e) {
      pattern = null;
    }

    boolean useNow;
    try {
      useNow = jsonField.get("use_now").asBoolean();
    } catch (NullPointerException e) {
      useNow = false;
    }

    String regex;
    try {
      regex = jsonField.get("regex").asText();
    } catch (NullPointerException e) {
      regex = null;
    }

    String request;
    try {
      request = jsonField.get("request").asText();
    } catch (NullPointerException e) {
      request = null;
    }

    String link;
    try {
      link = jsonField.get("link").asText();
    } catch (NullPointerException e) {
      link = null;
    }

    String url;
    try {
      url = jsonField.get("url").asText();
    } catch (NullPointerException e) {
      url = null;
    }

    String user;
    try {
      user = jsonField.get("user").asText();
    } catch (NullPointerException e) {
      user = null;
    }

    String password;
    try {
      password = jsonField.get("password").asText();
    } catch (NullPointerException e) {
      password = null;
    }

    String modelType;
    try {
      modelType = jsonField.get("model_type").asText();
    } catch (NullPointerException e) {
      modelType = null;
    }

    Float temperature;
    try {
      temperature = Float.valueOf(jsonField.get("temperature").asText());
    } catch (NullPointerException e) {
      temperature = null;
    }

    Float frequencyPenalty;
    try {
      frequencyPenalty = Float.valueOf(jsonField.get("frequency_penalty").asText());
    } catch (NullPointerException e) {
      frequencyPenalty = null;
    }

    Float presencePenalty;
    try {
      presencePenalty = Float.valueOf(jsonField.get("presence_penalty").asText());
    } catch (NullPointerException e) {
      presencePenalty = null;
    }

    Integer maxTokens;
    try {
      maxTokens = Integer.valueOf(jsonField.get("max_tokens").asText());
    } catch (NullPointerException e) {
      maxTokens = null;
    }

    Float topP;
    try {
      topP = Float.valueOf(jsonField.get("top_p").asText());
    } catch (NullPointerException e) {
      topP = null;
    }

    String context;
    try {
      context = jsonField.get("context").asText();
    } catch (NullPointerException e) {
      context = null;
    }

    JsonNode filtersArray = jsonField.get("filters");
    List<JsonNode> filters = new ArrayList<>();
    try {
      if (filtersArray.isArray()) {
        for (JsonNode possibleValue : filtersArray) {
          filters.add(possibleValue);
        }
      }
    } catch (NullPointerException e) {
      filters = Collections.emptyList();
    }

    JsonNode possibleValuesArray = jsonField.get("possible_values");
    List<JsonNode> possibleValues = new ArrayList<>();
    try {
      if (possibleValuesArray.isArray()) {
        for (JsonNode possibleValue : possibleValuesArray) {
          possibleValues.add(possibleValue);
        }
      }
    } catch (NullPointerException e) {
      possibleValues = Collections.emptyList();
    }

    JsonNode weightsObject = jsonField.get("possible_values_weighted");
    LinkedHashMap<String, Long> possible_values_weighted =
        new LinkedHashMap<>();
    if (weightsObject != null) {
      Iterator<Map.Entry<String, JsonNode>> weightsIterator =
          weightsObject.fields();
      while (weightsIterator.hasNext()) {
        Map.Entry<String, JsonNode> weight = weightsIterator.next();
        possible_values_weighted.put(weight.getKey(),
            weight.getValue().asLong());
      }
    }

    JsonNode conditionalsObject = jsonField.get("conditionals");
    LinkedHashMap<String, String> conditionals = new LinkedHashMap<>();
    if (conditionalsObject != null) {
      Iterator<Map.Entry<String, JsonNode>> conditionalsIterator =
          conditionalsObject.fields();
      while (conditionalsIterator.hasNext()) {
        Map.Entry<String, JsonNode> conditional = conditionalsIterator.next();
        conditionals.put(conditional.getKey(), conditional.getValue().asText());
      }
    }

    return (T) Field.instantiateField(
        properties,
        name,
        type,
        length,
        min,
        max,
        opsMap.get(name),
        possibleValues,
        possible_values_weighted,
        filters,
        conditionals,
        file,
        separator,
        pattern,
        useNow,
        regex,
        request,
        ghost,
        field,
        formula,
        injection,
        link,
        url,
        user,
        password,
        modelType,
        temperature,
        frequencyPenalty,
        presencePenalty,
        maxTokens,
        topP,
        context);
  }

  private Map<String, String> mapColNameToColQual(String mapping) {
    Map<String, String> hbaseFamilyColsMap = new HashMap<>();
    for (String s : mapping.split(";")) {
      String cq = s.split(":")[0];
      for (String c : s.split(":")[1].split(",")) {
        hbaseFamilyColsMap.put(c, cq);
      }
    }
    return hbaseFamilyColsMap;
  }

  private void addOptionToHashMap(Map<String, String> options,
                                  JsonNode jsonOptions) {
    Iterator<String> jsonNodeIterator = jsonOptions.fieldNames();
    while (jsonNodeIterator.hasNext()) {
      String nodeName = jsonNodeIterator.next();
      options.put(nodeName, jsonOptions.get(nodeName).asText());
    }
  }


  private void addPrimaryKeyToHashMap(Map<String, List<String>> pks,
                                      JsonNode jsonPk) {
    Iterator<String> jsonNodeIterator = jsonPk.fieldNames();
    while (jsonNodeIterator.hasNext()) {
      String nodeName = jsonNodeIterator.next();
      pks.put(nodeName,
          Arrays.asList(jsonPk.get(nodeName).asText().split("\\s*,\\s*")));
    }
  }

  private void addTableNameToHashMap(Map<String, String> tbs, JsonNode jsonTb) {
    Iterator<String> jsonNodeIterator = jsonTb.fieldNames();
    while (jsonNodeIterator.hasNext()) {
      String nodeName = jsonNodeIterator.next();
      tbs.put(nodeName, jsonTb.get(nodeName).asText());
    }
  }

}
