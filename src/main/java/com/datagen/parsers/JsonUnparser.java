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


import com.datagen.model.Model;
import com.datagen.model.OptionsConverter;
import com.datagen.model.type.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Goal of this class is to be able to parse a codified file JSON and render a model based on it
 */
@SuppressWarnings("unchecked")
@Slf4j
public class JsonUnparser implements UnParser {

  DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy")
      .withZone(ZoneOffset.UTC);
  DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_INSTANT
      .withZone(ZoneOffset.UTC);

  private final ObjectMapper mapper;

  public JsonUnparser() {
    this.mapper = new ObjectMapper();
    mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
  }

  /**
   * Render a model as a JSON without writing it
   * @param model
   * @return
   */
  public String renderJsonFromModel(Model model) {
    return renderFileFromModel(model, null);
  }

  /**
   * Render a model as a JSON file
   *
   * @return
   */
  public String renderFileFromModel(Model model, String pathToWriteModel) {
    String json = "NOT ABLE TO RENDER MODEL";
    ObjectNode root = mapper.createObjectNode();

    root.put("model_name", model.getName());

    ArrayNode fieldsArray = root.putArray("Fields");
    model.getFields().forEach((name, f) -> {
      Field field = (Field) f;
      if(f==null) {
        log.warn("Field with name: {} is null", name);
        return;
      }
      ObjectNode objectNode = fieldsArray.addObject();
      objectNode.put("name", (String) name);
      objectNode.put("type", field.getTypeForModel());
      if (field.isGhost()) {
        objectNode.put("ghost", "true");
      }
      if (field.getLength()!=-1) {
        objectNode.put("length", field.getLength());
      }
      if (field.getPossibleValuesProvided() != null &&
          !field.getPossibleValuesProvided().isEmpty()) {
        var possibleValAsSet = new HashSet<>(field.getPossibleValuesProvided());
        if(field.getPossibleValuesProvided().size()>possibleValAsSet.size()) {
          log.debug("These possible values are in fact weighted as some duplicates were detected");
          var possibleValuesWeightedNode = objectNode.putObject("possible_values_weighted");
          possibleValAsSet.forEach(pvFromSet -> {
            var pvOccurences = field.getPossibleValuesProvided().stream().filter(pv -> pv.toString().equalsIgnoreCase(pvFromSet.toString())).count();
            possibleValuesWeightedNode.put(pvFromSet.toString(), pvOccurences);
          });
        } else {
          ArrayNode possibleValuesNode = objectNode.putArray("possible_values");
          field.getPossibleValuesProvided()
              .forEach(pv -> possibleValuesNode.add(pv.toString()));
        }
      }
      if (field.getFilters() != null &&
          !field.getFilters().isEmpty()) {
        ArrayNode filtersNode = objectNode.putArray("filters");
        field.getFilters()
            .forEach(pv -> filtersNode.add(pv.toString()));
      }

      switch (FieldRepresentation.FieldType.valueOf(((Field<?>) f).getTypeForModel())) {
      case INTEGER, LONG, FLOAT, INCREMENT_LONG, INCREMENT_INTEGER -> {
        if (field.getMax() != null && field.getMax()!=Integer.MAX_VALUE && field.getMax()!=Long.MAX_VALUE && (field.getMax()!=Long.MAX_VALUE-1)) {
          objectNode.put("max", field.getMax());
        }
        if (field.getMin() != null && field.getMin()!=Integer.MIN_VALUE && field.getMin()!=Long.MIN_VALUE) {
          objectNode.put("min", field.getMin());
        }
      }
      case CSV -> {
        var castedField = (CsvField) field;
        objectNode.put("field", castedField.getMainField());
        objectNode.put("separator", castedField.getSeparator());
        objectNode.put("file", castedField.getFile());
      }
      case BIRTHDATE -> {
        var castedField = (BirthdateField) field;
        if(castedField.getMin()!=null) {
          objectNode.put("min_date", dateFormatter.format(LocalDate.ofEpochDay(castedField.getMin())));
        }
        if(castedField.getMax()!=null) {
          objectNode.put("max_date", dateFormatter.format(LocalDate.ofEpochDay(castedField.getMax())));
        }
      }
      case DATE -> {
        var castedField = (DateField) field;
        if(castedField.getMin()!=null) {
          objectNode.put("min_date",
              LocalDateTime.ofEpochSecond(castedField.getMin(), 0, ZoneOffset.UTC)
                  .atZone(ZoneOffset.UTC)
                  .format(dateFormatter)
          );
        }
        if(castedField.getMax()!=null) {
          objectNode.put("max_date",
              LocalDateTime.ofEpochSecond(castedField.getMax(), 0, ZoneOffset.UTC)
                  .atZone(ZoneOffset.UTC)
                  .format(dateFormatter)
          );
        }
        objectNode.put("use_now", castedField.isUseNow());
      }
      case DATE_AS_STRING -> {
        var castedField = (DateAsStringField) field;
        if(castedField.getMin()!=null) {
          objectNode.put("min_date",
              LocalDateTime.ofEpochSecond(castedField.getMin(), 0, ZoneOffset.UTC)
                  .atZone(ZoneOffset.UTC)
                  .format(dateFormatter)
          );
        }
        if(castedField.getMax()!=null) {
          objectNode.put("max_date",
              LocalDateTime.ofEpochSecond(castedField.getMax(), 0, ZoneOffset.UTC)
                  .atZone(ZoneOffset.UTC)
                  .format(dateFormatter)
          );
        }
        objectNode.put("use_now", castedField.isUseNow());
        if(castedField.getPattern()!=null) {
          objectNode.put("pattern", castedField.getPattern());
        }
      }
      case STRING_REGEX -> {
        var castedField = (StringRegexField) field;
        if(castedField.getRegex()!=null) {
          objectNode.put("regex", castedField.getRegex());
        }
      }
      case LOCAL_LLM -> {
        var castedField = (LocalLLMField) field;
        if(castedField.getRawRequest()!=null) {
          objectNode.put("request", castedField.getRawRequest());
        }
        if(castedField.getContext()!=null) {
          objectNode.put("context", castedField.getContext());
        }
        if(castedField.getFile()!=null) {
          objectNode.put("file", castedField.getFile());
        }
        if(castedField.getTemperature()!=null) {
          objectNode.put("temperature", castedField.getTemperature());
        }
        if(castedField.getFrequencyPenalty()!=null) {
          objectNode.put("frequency_penalty", castedField.getFrequencyPenalty());
        }
        if(castedField.getPresencePenalty()!=null) {
          objectNode.put("presence_penalty", castedField.getPresencePenalty());
        }
        if(castedField.getTopP()!=null) {
          objectNode.put("top_p", castedField.getTopP());
        }
      }
      case OLLAMA -> {
        var castedField = (OllamaField) field;
        if(castedField.getRawRequest()!=null) {
          objectNode.put("request", castedField.getRawRequest());
        }
        if(castedField.getContext()!=null) {
          objectNode.put("context", castedField.getContext());
        }
        if(castedField.getModelType()!=null) {
          objectNode.put("model_type", castedField.getModelType());
        }
        if(castedField.getTemperature()!=null) {
          objectNode.put("temperature", castedField.getTemperature());
        }
        if(castedField.getFrequencyPenalty()!=null) {
          objectNode.put("frequency_penalty", castedField.getFrequencyPenalty());
        }
        if(castedField.getPresencePenalty()!=null) {
          objectNode.put("presence_penalty", castedField.getPresencePenalty());
        }
        if(castedField.getTopP()!=null) {
          objectNode.put("top_p", castedField.getTopP());
        }
      }
      case BEDROCK -> {
        var castedField = (BedrockField) field;
        if(castedField.getRawRequest()!=null) {
          objectNode.put("request", castedField.getRawRequest());
        }
        if(castedField.getContext()!=null) {
          objectNode.put("context", castedField.getContext());
        }
        if(castedField.getModelType()!=null) {
          objectNode.put("model_type", castedField.getModelType());
        }
        if(castedField.getTemperature()!=null) {
          objectNode.put("temperature", castedField.getTemperature());
        }
        if(castedField.getUrl()!=null) {
          objectNode.put("url", castedField.getUrl());
        }
        if(castedField.getUser()!=null) {
          objectNode.put("user", castedField.getUser());
        }
        if(castedField.getPassword()!=null) {
          objectNode.put("password", castedField.getPassword());
        }
        if(castedField.getMaxTokens()!=null) {
          objectNode.put("max_tokens", castedField.getMaxTokens());
        }
      }
      case OPEN_AI -> {
        var castedField = (OpenAIField) field;
        if(castedField.getRawRequest()!=null) {
          objectNode.put("request", castedField.getRawRequest());
        }
        if(castedField.getContext()!=null) {
          objectNode.put("context", castedField.getContext());
        }
        if(castedField.getModelType()!=null) {
          objectNode.put("model_type", castedField.getModelType());
        }
        if(castedField.getUrl()!=null) {
          objectNode.put("url", castedField.getUrl());
        }
        if(castedField.getUser()!=null) {
          objectNode.put("user", castedField.getUser());
        }
        if(castedField.getPassword()!=null) {
          objectNode.put("password", castedField.getPassword());
        }
        if(castedField.getTemperature()!=null) {
          objectNode.put("temperature", castedField.getTemperature());
        }
        if(castedField.getFrequencyPenalty()!=null) {
          objectNode.put("frequency_penalty", castedField.getFrequencyPenalty());
        }
        if(castedField.getPresencePenalty()!=null) {
          objectNode.put("presence_penalty", castedField.getPresencePenalty());
        }
        if(castedField.getTopP()!=null) {
          objectNode.put("top_p", castedField.getTopP());
        }
        if(castedField.getMaxTokens()!=null) {
          objectNode.put("max_tokens", castedField.getMaxTokens());
        }
      }
      default -> {}
      }

      if(field.getConditional()!=null
          && field.getConditional().getConditionLines()!=null
          && !field.getConditional().getConditionLines().isEmpty())
      {
        var conditionalHashmap = new LinkedHashMap<String, String>();
        field.getConditional().getConditionLines().forEach(cl -> {
          if(cl.isLink()) {
            objectNode.put("link" , cl.getRawValueToReturn());
          } else if(cl.isFormula()) {
            objectNode.put("formula" , cl.getRawValueToReturn());
          } else if(cl.isInjection()) {
            objectNode.put("injection" , cl.getRawValueToReturn());
          } else {
            conditionalHashmap.put(cl.getRawOperatorValue(),
                cl.getValueToReturn());
          }
        });
        if(!conditionalHashmap.isEmpty()) {
          ObjectNode conditionalsNode = objectNode.putObject("conditionals");
          conditionalHashmap.forEach(conditionalsNode::put);
        }
      }

    });

    // Filter table names and options to only have not null & empty values
    var tableNamesfiltered = new HashMap<OptionsConverter.TableNames, String>(model.getTableNames())
        .entrySet().stream()
        .filter(e -> e.getValue()!=null &&
            !e.getValue().isEmpty() &&
            !e.getValue().isBlank())
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            Map.Entry::getValue
        ));

    var optionsFiltered = new HashMap<OptionsConverter.Options, Object>(model.getOptions())
        .entrySet().stream()
        .filter(e -> e.getValue()!=null &&
            !e.getValue().toString().isEmpty() &&
            !e.getValue().toString().isBlank())
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            Map.Entry::getValue
        ));

    root.putPOJO("Table_Names", tableNamesfiltered);

    // Hbase col family mapping is changed during model creation into a map of field and needs to be converted back to a string before writing it again
    if(optionsFiltered.get(OptionsConverter.Options.HBASE_COLUMN_FAMILIES_MAPPING)!=null && !optionsFiltered.get(OptionsConverter.Options.HBASE_COLUMN_FAMILIES_MAPPING).toString().isEmpty()) {
      var hbaseColFamReconverted = model.reConvertHbaseColFamilyOption(
          (Map<Object, String>) optionsFiltered.get(
              OptionsConverter.Options.HBASE_COLUMN_FAMILIES_MAPPING));
      if(!hbaseColFamReconverted.isBlank()) {optionsFiltered.put(OptionsConverter.Options.HBASE_COLUMN_FAMILIES_MAPPING, hbaseColFamReconverted);}
    }
    root.putPOJO("Options", optionsFiltered);

    try {
      json = this.mapper.writeValueAsString(root);
      if(pathToWriteModel!=null) {
        this.mapper.writeValue(new File(pathToWriteModel), root);
      }
    } catch (JsonProcessingException e) {
      log.warn("Not able to render model with error: ", e);
    } catch (IOException e) {
      log.warn("Not able to write model with error: ", e);
    }

    return json;
  }


}
