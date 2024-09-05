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
import com.datagen.model.type.FieldRepresentation;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Goal of this class is to be able to parse a codified file JSON and render a model based on it
 */
@SuppressWarnings("unchecked")
@Slf4j
public class JsonParser<T extends Field> implements Parser {

  DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;

  @Getter
  private JsonNode root;

  public JsonParser(String jsonFilePath) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      log.info("Model used is from Json file : {} ", jsonFilePath);
      root = mapper.readTree(new File(jsonFilePath));
      log.debug("JSON file content is : {}", root.toPrettyString());
    } catch (FileNotFoundException e) {
      log.warn("JSON Model File not found: {}", jsonFilePath);
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

  public JsonParser(InputStream inputStream) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      log.info("Model used is from Json input");
      root = mapper.readTree(inputStream);
      log.debug("JSON content is : {}", root.toPrettyString());
    } catch (IOException e) {
      log.error(
          "Could not read JSON input, please verify its structure, error is : ", e);
    }
  }

  /**
   * Creates a model and populate it by reading the JSON file provided as argument of the constructor
   *
   * @return Model instantiated and populated
   */
  public Model renderModelFromFile(Map<ApplicationConfigs, String> properties) {

    // Release 0.7.0 introduced possibility to name the model
    var modelName = root.get("model_name")!=null ? root.get("model_name").asText("") : "";

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

    return new Model(modelName, fields, pks, tbs, opsMap, properties);
  }


  /**
   * Get one field from a Json node by instantiating the right type of field and return it
   * Note that if no length is precised in the JSON, it is automatically handled
   *
   * @param jsonField
   * @return
   */
  private T getOneField(JsonNode jsonField, Map<ApplicationConfigs, String> properties, Map<String, String> hbaseFamilyColsMap) {
    String name = jsonField.get("name").asText("UNDEFINED_COL_NAME");
    String type= jsonField.get("type").asText("STRING");

    FieldRepresentation fieldRepresentation = new FieldRepresentation(name, type);
    fieldRepresentation.setColumnQualifier(hbaseFamilyColsMap.get(name));

    fieldRepresentation.setLength(jsonField.get("length")==null?null:(jsonField.get("length").asInt()));
    fieldRepresentation.setMin(jsonField.get("min")==null?null:(jsonField.get("min").asLong()));
    fieldRepresentation.setMax(jsonField.get("max")==null?null:(jsonField.get("max").asLong()));
    fieldRepresentation.setFile(jsonField.get("file")==null?null:(jsonField.get("file").asText()));
    fieldRepresentation.setSeparator(jsonField.get("separator")==null?null:(jsonField.get("separator").asText()));
    fieldRepresentation.setGhost(jsonField.get("ghost")==null?null:(jsonField.get("ghost").asBoolean()));
    fieldRepresentation.setFormula(jsonField.get("formula")==null?null:(jsonField.get("formula").asText()));
    fieldRepresentation.setInjection(jsonField.get("injection")==null?null:(jsonField.get("injection").asText()));
    fieldRepresentation.setMainField(jsonField.get("field")==null?null:(jsonField.get("field").asText()));
    fieldRepresentation.setPattern(jsonField.get("pattern")==null?null:(jsonField.get("pattern").asText()));
    fieldRepresentation.setUseNow(jsonField.get("use_now")==null?null:(jsonField.get("use_now").asBoolean()));
    fieldRepresentation.setRegex(jsonField.get("regex")==null?null:(jsonField.get("regex").asText()));
    fieldRepresentation.setRequest(jsonField.get("request")==null?null:(jsonField.get("request").asText()));
    fieldRepresentation.setLink(jsonField.get("link")==null?null:(jsonField.get("link").asText()));
    fieldRepresentation.setUrl(jsonField.get("url")==null?null:(jsonField.get("url").asText()));
    fieldRepresentation.setUser(jsonField.get("user")==null?null:(jsonField.get("user").asText()));
    fieldRepresentation.setPassword(jsonField.get("password")==null?null:(jsonField.get("password").asText()));
    fieldRepresentation.setModelType(jsonField.get("model_type")==null?null:(jsonField.get("model_type").asText()));
    fieldRepresentation.setTemperature(jsonField.get("temperature")==null?null:Float.valueOf(jsonField.get("temperature").asText()));
    fieldRepresentation.setFrequencyPenalty(jsonField.get("frequency_penalty")==null?null:Float.valueOf(jsonField.get("frequency_penalty").asText()));
    fieldRepresentation.setPresencePenalty(jsonField.get("presence_penalty")==null?null:Float.valueOf(jsonField.get("presence_penalty").asText()));
    fieldRepresentation.setMaxTokens(jsonField.get("max_tokens")==null?null:(jsonField.get("max_tokens").asInt()));
    fieldRepresentation.setTopP(jsonField.get("top_p")==null?null:Float.valueOf(jsonField.get("top_p").asText()));
    fieldRepresentation.setContext(jsonField.get("context")==null?null:(jsonField.get("context").asText()));

    if(jsonField.get("min_date")!=null) {
      var minDateJson = jsonField.get("min_date").asText();
      if(minDateJson.length()<=10) {
        log.debug("Min date provided is a date");
        String[] maxSplit = minDateJson.split("[/]");
        fieldRepresentation.setMinDate(LocalDate.of(Integer.parseInt(maxSplit[2]),
            Integer.parseInt(maxSplit[1]), Integer.parseInt(maxSplit[0])));
      } else {
        log.debug("Min date provided is a date with time");
        String minFormatted = minDateJson.substring(0,11) + "T" + minDateJson.substring(12) + "Z";
        fieldRepresentation.setMinDateTime(LocalDateTime.parse(minFormatted, formatter));
      }
    }
    if(jsonField.get("max_date")!=null) {
      var maxDateJson = jsonField.get("max_date").asText();
      if(maxDateJson.length()<=10) {
        log.debug("Max date provided is a date");
        String[] maxSplit = maxDateJson.split("[/]");
        fieldRepresentation.setMaxDate(LocalDate.of(Integer.parseInt(maxSplit[2]),
            Integer.parseInt(maxSplit[1]), Integer.parseInt(maxSplit[0])));
      } else {
        log.debug("Max date provided is a date with time");
        String minFormatted = maxDateJson.substring(0,11) + "T" + maxDateJson.substring(12) + "Z";
        fieldRepresentation.setMaxDateTime(LocalDateTime.parse(minFormatted, formatter));
      }
    }
    JsonNode filtersArray = jsonField.get("filters");
    List<String> filters = new ArrayList<>();
    try {
      if (filtersArray.isArray()) {
        for (JsonNode possibleValue : filtersArray) {
          filters.add(possibleValue.asText());
        }
      }
    } catch (NullPointerException e) {
      filters = Collections.emptyList();
    }
    fieldRepresentation.setFilters(filters);

    LinkedHashMap<String, Long> possibleValuesWeighted = new LinkedHashMap<>();
    JsonNode possibleValuesArray = jsonField.get("possible_values");
    if (possibleValuesArray!=null && possibleValuesArray.isArray()) {
      for (JsonNode possibleValue : possibleValuesArray) {
        possibleValuesWeighted.put(possibleValue.asText(), 1L);
      }
    }

    JsonNode weightsObject = jsonField.get("possible_values_weighted");

    if (weightsObject != null) {
      Iterator<Map.Entry<String, JsonNode>> weightsIterator =
          weightsObject.fields();
      while (weightsIterator.hasNext()) {
        Map.Entry<String, JsonNode> weight = weightsIterator.next();
        possibleValuesWeighted.put(weight.getKey(),
            weight.getValue().asLong());
      }
    }
    fieldRepresentation.setPossibleValuesWeighted(possibleValuesWeighted);

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
    fieldRepresentation.setConditionals(conditionals);

    return (T) Field.instantiateField(properties, fieldRepresentation);
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
