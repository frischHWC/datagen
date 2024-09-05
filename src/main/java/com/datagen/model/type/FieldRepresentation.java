package com.datagen.model.type;


import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;

/**
 * This class aims at representing in a generic way, a Field.
 * Its purpose is to only represent a field with its attributes, before/after being transformed to a "real" field which is specialized
 */
@ToString
@Getter
@Setter
public class FieldRepresentation {
  // Required attributes
  String name;
  FieldType type;

  // Optional attributes

  // For all
  Boolean ghost;
  String columnQualifier;

  // INT, LONG, FLOAT
  Long max;
  // INT, LONG, FLOAT, INCREMENT_INTEGER, INCREMENT_LONG
  Long min;

  // For STRING, STRING_AZ, BYTES, HASH_MD5, BLOB, PHONE
  Integer length;

  // For STRING, STRING_AZ, INT, BOOLEAN, FLOAT, LONG, TIMESTAMP, BYTES, HASH_MD5, BIRTHDATE, COUNTRY, EMAIL, DATE, DATE_AS_STRING
  HashMap<String, Long> possibleValuesWeighted;

  LinkedHashMap<String, String> conditionals;
  // For STRING, STRING_AZ, INT, FLOAT, LONG, BYTES, HASH_MD5, BLOB, BOOLEAN, TIMESTAMP
  String formula;
  // For STRING, STRING_AZ
  String injection;
  // For STRING, STRING_AZ, INT,
  String link;

  // For NAME, CITY, PHONE, CSV, EMAIL
  List<String> filters;

  // For CSV
  String file; // and for Local LLM
  String separator;
  String mainField;

  // For BIRTHDATE
  LocalDate minDate;
  LocalDate maxDate;

  // For DATE AS STRING, DATE
  Boolean useNow;
  LocalDateTime minDateTime;
  LocalDateTime maxDateTime;

  // For DATE AS STRING,
  String pattern;

  // For STRING REGEX
  String regex;

  // For LLM fields (OLLAMA, BEDROCK, OPENAI)
  String request;
  String context;

  // For LLM fields (BEDROCK, OPENAI)
  String url;
  String user;
  String password;

  // For LLM fields (OLLAMA, BEDROCK, OPENAI)
  String modelType;
  Float temperature;
  Float frequencyPenalty;
  Float presencePenalty;
  Integer maxTokens;
  Float topP;


  public FieldRepresentation() {
    this.name = "";
    // For binding usage, it is best and easier to initialize lists and maps
    this.possibleValuesWeighted = new LinkedHashMap<>();
    this.filters = new ArrayList<>();
    this.conditionals = new LinkedHashMap<>();
  }

  public FieldRepresentation(String name, FieldType fieldType) {
    this.name = name;
    this.type = fieldType;
  }

  public FieldRepresentation(String name, String fieldType) {
    this.name = name;
    this.type = FieldType.valueOf(fieldType);
  }

  public FieldRepresentation(Field field) {
    this.name = field.getName();
    this.type = FieldType.valueOf(field.getTypeForModel());
    this.ghost = field.isGhost();
    this.columnQualifier = field.getHbaseColumnQualifier();

    this.length = field.getLength()!=20?field.getLength():null;

    this.possibleValuesWeighted = new HashMap<>();
    if(field.getPossibleValuesProvided()!=null) {
      new HashSet<>(field.getPossibleValuesProvided()).forEach(pvFromSet -> {
        var pvOccurences = field.getPossibleValuesProvided().stream()
            .filter(pv -> pv.toString().equalsIgnoreCase(pvFromSet.toString()))
            .count();
        this.possibleValuesWeighted.put(pvFromSet.toString(), pvOccurences);
      });
    }

    this.filters = field.getFilters();

    switch (FieldType.valueOf(field.getTypeForModel())) {
    case INTEGER, LONG, FLOAT, INCREMENT_LONG, INCREMENT_INTEGER -> {
      this.min = field.getMin();
      this.max = field.getMax();
    }
      case CSV -> {
        var castedField = (CsvField) field;
        this.mainField = castedField.getMainField();
        this.separator = castedField.getSeparator();
        this.file = castedField.getFile();
      }
    case BIRTHDATE -> {
      var castedField = (BirthdateField) field;
      if(castedField.getMin()!=null) {
        this.minDate = LocalDate.ofEpochDay(castedField.getMin());
      }
      if(castedField.getMax()!=null) {
        this.maxDate = LocalDate.ofEpochDay(castedField.getMax());
      }
    }
    case DATE -> {
      var castedField = (DateField) field;
      if(castedField.getMin()!=null) {
        this.minDateTime = LocalDateTime.ofEpochSecond(castedField.getMin(), 0,
            ZoneOffset.UTC);
      }
        if(castedField.getMax()!=null) {
          this.maxDateTime =
              LocalDateTime.ofEpochSecond(castedField.getMax(), 0,
                  ZoneOffset.UTC);
        }
      this.useNow = castedField.isUseNow();
    }
    case DATE_AS_STRING -> {
      var castedField = (DateAsStringField) field;
      if(castedField.getMin()!=null) {
        this.minDateTime = LocalDateTime.ofEpochSecond(castedField.getMin(), 0,
            ZoneOffset.UTC);
      }
      if(castedField.getMax()!=null) {
        this.maxDateTime = LocalDateTime.ofEpochSecond(castedField.getMax(), 0,
            ZoneOffset.UTC);
      }
      this.useNow = castedField.isUseNow();
      this.pattern = castedField.getPattern();
    }
    case STRING_REGEX -> {
      var castedField = (StringRegexField) field;
        this.regex = castedField.getRegex();
    }
    case LOCAL_LLM -> {
      var castedField = (LocalLLMField) field;
      this.file = castedField.getFile();
      this.request = castedField.getRawRequest();
      this.context = castedField.getContext();
      this.temperature = castedField.getTemperature();
      this.frequencyPenalty = castedField.getFrequencyPenalty();
      this.presencePenalty = castedField.getPresencePenalty();
      this.topP = castedField.getTopP();
    }
    case OLLAMA -> {
      var castedField = (OllamaField) field;
      this.request = castedField.getRawRequest();
      this.context = castedField.getContext();
      this.url = castedField.getUrl();
      this.user = castedField.getUser();
      this.password = castedField.getPassword();
      this.modelType = castedField.getModelType();
      this.temperature = castedField.getTemperature();
      this.frequencyPenalty = castedField.getFrequencyPenalty();
      this.presencePenalty = castedField.getPresencePenalty();
      this.topP = castedField.getTopP();
    }
    case BEDROCK -> {
      var castedField = (BedrockField) field;
      this.request = castedField.getRawRequest();
      this.context = castedField.getContext();
      this.url = castedField.getUrl();
      this.user = castedField.getUser();
      this.password = castedField.getPassword();
      this.modelType = castedField.getModelType();
      this.temperature = castedField.getTemperature().floatValue();
      this.maxTokens = castedField.getMaxTokens();
    }
    case OPEN_AI -> {
      var castedField = (OpenAIField) field;
      this.request = castedField.getRawRequest();
      this.context = castedField.getContext();
      this.url = castedField.getUrl();
      this.user = castedField.getUser();
      this.password = castedField.getPassword();
      this.modelType = castedField.getModelType();
      this.temperature = castedField.getTemperature();
      this.frequencyPenalty = castedField.getFrequencyPenalty();
      this.presencePenalty = castedField.getPresencePenalty();
      this.topP = castedField.getTopP();
      this.maxTokens = castedField.getMaxTokens();
    }
    default -> {}
    }
    if(field.getConditional()!=null
        && field.getConditional().getConditionLines()!=null
        && !field.getConditional().getConditionLines().isEmpty())
    {
      this.conditionals = new LinkedHashMap<>();
      field.getConditional().getConditionLines().forEach(cl -> {
        if(cl.isLink()) {
          this.link = cl.getValueToReturn();
        } else if(cl.isFormula()) {
          this.formula = cl.getValueToReturn();
        } else if(cl.isInjection()) {
          this.injection = cl.getValueToReturn();
        }
        this.conditionals.put(cl.getRawOperatorValue(), cl.getValueToReturn());
      });
    }

  }

  public enum FieldType {
    BEDROCK,
    BIRTHDATE,
    BLOB,
    BOOLEAN,
    BYTES,
    CITY,
    COUNTRY,
    CSV,
    DATE_AS_STRING,
    DATE,
    EMAIL,
    FLOAT,
    HASH_MD5,
    INCREMENT_LONG,
    INCREMENT_INTEGER,
    INTEGER,
    IP,
    LINK,
    LONG,
    NAME,
    LOCAL_LLM,
    OLLAMA,
    OPEN_AI,
    PHONE,
    STRING_AZ,
    STRING,
    STRING_REGEX,
    TIMESTAMP,
    UUID
  }

}
