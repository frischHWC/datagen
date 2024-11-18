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
package com.datagen.model.type;


import com.datagen.config.ApplicationConfigs;
import com.datagen.model.Row;
import com.datagen.model.conditions.ConditionalEvaluator;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hive.jdbc.HivePreparedStatement;
import org.apache.kudu.Type;
import org.apache.kudu.client.PartialRow;
import org.apache.orc.TypeDescription;
import org.apache.solr.common.SolrInputDocument;

import java.sql.SQLException;
import java.util.*;


/**
 * This abstract class describes a field with three characteristics: Name, Type, Length (which is optional)
 * Goal is also to describe how a field is rendered according to its type
 * Every new type added should extend this abstract class in a new Java Class (and override generateRandomValue())
 */
@Slf4j
public abstract class Field<T> {

  @Getter
  @Setter
  public String name;
  @Getter
  @Setter
  public Boolean computed = false;
  @Getter
  @Setter
  public List<T> possibleValuesInternal;
  @Getter
  @Setter
  public List<T> possibleValuesProvided;
  @Getter
  @Setter
  public Integer possibleValueSize;
  @Getter
  @Setter
  public List<String> filters;
  @Getter
  @Setter
  public String file;
  // This is a conditional evaluator holding all complexity (parsing, preparing comparison, evaluating it)
  @Getter
  @Setter
  public ConditionalEvaluator conditional;
  // Default length is -1, if user does not provide a strict superior to 0 length,
  // each Extended field class should by default override it to a number strictly superior to 0
  @Getter
  @Setter
  public int length = -1;
  // Minimum possible value for Int/Long
  @Getter
  @Setter
  public Long min;
  // Maximum possible value Int/Long
  @Getter
  @Setter
  public Long max;
  @Getter
  @Setter
  public String hbaseColumnQualifier = "cq";
  @Getter
  @Setter
  public boolean ghost;
  Random random = new Random();

  public static String toString(List<Field> fieldList) {
    StringBuilder sb = new StringBuilder();
    sb.append("Fields :  [ ");
    sb.append(System.getProperty("line.separator"));
    fieldList.forEach(f -> {
      sb.append(" { ");
      sb.append(f.toString());
      sb.append(" }");
      sb.append(System.getProperty("line.separator"));
    });
    sb.append(" ] ");
    sb.append(System.getProperty("line.separator"));
    return sb.toString();
  }


  /**
   *  Create the right instance of a field (i.e. String, password etc..) according to its type
   * @param properties
   * @param f
   * @return
   */
  public static Field instantiateField(Map<ApplicationConfigs, String> properties, FieldRepresentation f) {
    if (f.name == null || f.name.isEmpty()) {
      throw new IllegalStateException(
          "Name can not be null or empty for field: " + f.name);
    }
    if (f.type == null) {
      throw new IllegalStateException(
          "Type can not be null or empty for field: " + f.name);
    }

    // If length is not accurate, it should be let as is (default is -1) and let each type handles it
    if (f.length == null || f.length < 1) {
      f.length = -1;
    }

    HashMap<String, Long> possibleValuesWeighted = f.possibleValuesWeighted !=null?f.possibleValuesWeighted:new HashMap<>();

    Field field = switch (f.type) {
      case STRING:
        yield new StringField(f.name, f.length, possibleValuesWeighted);

      case STRING_AZ:
        yield new StringAZField(f.name, f.length, possibleValuesWeighted);

      case INTEGER:
        yield new IntegerField(f.name, possibleValuesWeighted, f.min, f.max);

      case INCREMENT_INTEGER:
        yield new IncrementIntegerField(f.name, f.min);

      case BOOLEAN:
        yield new BooleanField(f.name, possibleValuesWeighted);

      case FLOAT:
        yield new FloatField(f.name,
            possibleValuesWeighted, f.min,
            f.max);

      case LONG:
        yield new LongField(f.name,
            possibleValuesWeighted, f.min,
            f.max);

      case INCREMENT_LONG:
        yield new IncrementLongField(f.name, f.min);

      case TIMESTAMP:
        yield new TimestampField(f.name, possibleValuesWeighted);

      case BYTES:
        yield new BytesField(f.name, f.length, possibleValuesWeighted);

      case HASH_MD5:
        yield new HashMd5Field(f.name, f.length, possibleValuesWeighted);

      case BIRTHDATE:
        yield new BirthdateField(f.name, possibleValuesWeighted, f.minDate, f.maxDate);

      case NAME:
        yield new NameField(f.name, f.filters);

      case COUNTRY:
        yield new CountryField(f.name, possibleValuesWeighted);

      case CITY:
        yield new CityField(f.name, f.filters);

      case EMAIL:
        yield new EmailField(f.name,
            possibleValuesWeighted,
            f.filters);

      case IP:
        yield new IpField(f.name);

      case LINK:
        yield new LinkField(f.name);

      case CSV:
        yield new CsvField(f.name, f.filters, f.file, f.separator, f.mainField);

      case PHONE:
        yield new PhoneField(f.name, f.length, f.filters);

      case UUID:
        yield new UuidField(f.name);

      case DATE:
        yield new DateField(f.name, possibleValuesWeighted, f.minDateTime, f.maxDateTime, f.useNow);

      case DATE_AS_STRING:
        yield new DateAsStringField(f.name, possibleValuesWeighted, f.minDateTime, f.maxDateTime, f.useNow, f.pattern);

      case STRING_REGEX:
        yield new StringRegexField(f.name, f.regex);

      case LOCAL_LLM:
        yield new LocalLLMField(f.name,
            f.file,  f.request,
            f.temperature == null ? Float.valueOf(
                properties.get(ApplicationConfigs.LOCAL_LLM_TEMPERATURE_DEFAULT)) :
                f.temperature,
            f.frequencyPenalty == null ? Float.valueOf(properties.get(
                ApplicationConfigs.LOCAL_LLM_FREQUENCY_PENALTY_DEFAULT)) : f.frequencyPenalty,
            f.presencePenalty == null ? Float.valueOf(properties.get(
                ApplicationConfigs.LOCAL_LLM_PRESENCE_PENALTY_DEFAULT)) : f.presencePenalty,
            f.topP == null ? Float.valueOf(properties.get(
                ApplicationConfigs.LOCAL_LLM_TOP_P_DEFAULT)) : f.topP,
            f.maxTokens == null ? Integer.valueOf(properties.get(
                ApplicationConfigs.LOCAL_LLM_MAX_TOKENS_DEFAULT)) : f.maxTokens,
            f.context
        );

      case OLLAMA:
        yield new OllamaField(f.name,
            f.url,
            f.user, f.password, f.request,
            f.modelType == null ?
                properties.get(ApplicationConfigs.OLLAMA_MODEL_DEFAULT) :
                f.modelType,
            f.temperature == null ? Float.valueOf(
                properties.get(ApplicationConfigs.OLLAMA_TEMPERATURE_DEFAULT)) :
                f.temperature,
            f.frequencyPenalty == null ? Float.valueOf(properties.get(
                ApplicationConfigs.OLLAMA_FREQUENCY_PENALTY_DEFAULT)) : f.frequencyPenalty,
            f.presencePenalty == null ? Float.valueOf(properties.get(
                ApplicationConfigs.OLLAMA_PRESENCE_PENALTY_DEFAULT)) : f.presencePenalty,
            f.topP == null ? Float.valueOf(properties.get(
                ApplicationConfigs.OLLAMA_TOP_P_DEFAULT)) : f.topP,
            f.context
        );

      case BEDROCK:
        yield new BedrockField(f.name, f.url,
            f.user == null ?
                properties.get(ApplicationConfigs.BEDROCK_ACCESS_KEY_ID) : f.user,
            f.password == null ?
                properties.get(ApplicationConfigs.BEDROCK_ACCESS_KEY_SECRET) :
                f.password,
            f.request,
            f.modelType == null ?
                properties.get(ApplicationConfigs.BEDROCK_MODEL_DEFAULT) :
                f.modelType,
            f.temperature == null ? Float.valueOf(properties.get(
                ApplicationConfigs.BEDROCK_TEMPERATURE_DEFAULT)) : f.temperature,
            properties.get(ApplicationConfigs.BEDROCK_REGION),
            f.maxTokens == null ? Integer.valueOf(properties.get(
                ApplicationConfigs.BEDROCK_MAX_TOKENS_DEFAULT)) : f.maxTokens,
            f.context
            );

      case OPEN_AI:
        yield new OpenAIField(f.name, f.url,
            f.user,
            f.password == null ?
                properties.get(ApplicationConfigs.OPENAI_API_KEY) :
                f.password,
            f.request,
            f.modelType == null ?
                properties.get(ApplicationConfigs.OPENAI_MODEL_DEFAULT) :
                f.modelType,
            f.temperature == null ? Float.valueOf(properties.get(
                ApplicationConfigs.OPENAI_TEMPERATURE_DEFAULT)) : f.temperature,
            f.frequencyPenalty == null ? Float.valueOf(properties.get(
                ApplicationConfigs.OPENAI_FREQUENCY_PENALTY_DEFAULT)) : f.frequencyPenalty,
            f.presencePenalty == null ? Float.valueOf(properties.get(
                ApplicationConfigs.OPENAI_PRESENCE_PENALTY_DEFAULT)) : f.presencePenalty,
            f.maxTokens == null ? Integer.valueOf(properties.get(
                ApplicationConfigs.OPENAI_MAX_TOKENS_DEFAULT)) : f.maxTokens,
            f.topP == null ? Float.valueOf(properties.get(
                ApplicationConfigs.OPENAI_TOP_P_DEFAULT)) : f.topP,
            f.context
        );

      default:
        log.warn("Type : " + f.type +
            " has not been recognized and hence will be ignored");
        yield null;
    };

    // If hbase column qualifier is not accurate, it should be let as is (default is "cq")
    if (f.columnQualifier != null && !f.columnQualifier.isEmpty()) {
      field.setHbaseColumnQualifier(f.columnQualifier);
    }

    if(f.ghost!=null) {
      field.setGhost(f.ghost);
    }

    // If there are some conditions, we consider this field as computed (meaning it requires other fields' values to get its value)
    // and same thing for request if it contains a '$'
    if ((f.conditionals != null && !f.conditionals.isEmpty())
        || (f.request != null && f.request.contains("$"))
        || (f.formula != null && !f.formula.isEmpty())
        || (f.injection != null && !f.injection.isEmpty())
        || (f.link != null && !f.link.isEmpty() )) {
      log.debug("Field {} has been marked as computed: ", field);
      field.setComputed(true);
    }

    // Set conditionals or formula or injections for the field if there are
    if ((f.conditionals != null && !f.conditionals.isEmpty())) {
      field.setConditional(new ConditionalEvaluator(f.conditionals));
    } else if (f.formula != null && !f.formula.isEmpty()) {
      LinkedHashMap<String, String> lm = new LinkedHashMap<>();
      lm.put("formula", f.formula);
      field.setConditional(new ConditionalEvaluator(lm));
    } else if (f.injection != null && !f.injection.isEmpty()) {
      LinkedHashMap<String, String> lm = new LinkedHashMap<>();
      lm.put("injection", f.injection);
      field.setConditional(new ConditionalEvaluator(lm));
    } else if (f.link != null && !f.link.isEmpty()) {
      LinkedHashMap<String, String> lm = new LinkedHashMap<>();
      lm.put("link", f.link);
      field.setConditional(new ConditionalEvaluator(lm));
    }

    if (log.isDebugEnabled()) {
      log.debug("Field has been created: {}", field);
    }

    return field;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Class Type is " + this.getClass().getSimpleName() + " ; ");
    sb.append("name : " + name + " ; ");
    sb.append("IsGhost : " + ghost + " ; ");
    sb.append("hbase Column Qualifier : " + hbaseColumnQualifier + " ; ");
    sb.append("Length : " + length + " ; ");
    if (min != null) {
      sb.append("Min : " + min + " ; ");
    }
    if (max != null) {
      sb.append("Max : " + max + " ; ");
    }
    if(possibleValuesProvided != null) {
      sb.append("Possible Values: ");
      possibleValuesProvided.forEach(p -> {
        sb.append(p);
        sb.append(",");
      });
      sb.append(" ; ");
    }
    if(filters != null) {
      sb.append("Filters: ");
      filters.forEach(p -> {
        sb.append(p);
        sb.append(",");
      });
      sb.append(" ; ");
    }
    return sb.toString();
  }

  // To init a field when starting generation (for connections etc...)
  public void initField() {}

  // To close a field when starting generation (for connections etc...)
  public void closeField() {}

  public abstract T generateRandomValue();

  public T generateComputedValue(Row row) {
    return toCastValue(conditional.evaluateConditions(row));
  }

  public String getTypeForModel() {
    switch (this.getClass().getSimpleName().toLowerCase(Locale.ROOT)) {
    case "birthdatefield":
      return "BIRTHDATE";
    case "booleanfield":
      return "BOOLEAN";
    case "bytesfield":
      return "BYTES";
    case "countryfield":
      return "COUNTRY";
    case "cityfield":
      return "CITY";
    case "csvfield":
      return "CSV";
    case "dateasstringfield":
      return "DATE_AS_STRING";
    case "datefield":
      return "DATE";
    case "emailfield":
      return "EMAIL";
    case "floatfield":
      return "FLOAT";
    case "hashmd5field":
      return "HASH_MD5";
    case "incrementintegerfield":
      return "INCREMENT_INTEGER";
    case "incrementlongfield":
      return "INCREMENT_LONG";
    case "integerfield":
      return "INTEGER";
    case "ipfield":
      return "IP";
    case "linkfield":
      return "LINK";
    case "longfield":
      return "LONG";
    case "namefield":
      return "NAME";
    case "phonefield":
      return "PHONE";
    case "stringazfield":
      return "STRING_AZ";
    case "stringfield":
      return "STRING";
    case "stringregexfield":
      return "STRING_REGEX";
    case "timestampfield":
      return "TIMESTAMP";
    case "uuidfield":
      return "UUID";
    case "ollamafield":
      return "OLLAMA";
    case "bedrockfield":
      return "BEDROCK";
    case "openaifield":
      return "OPEN_AI";
    case "localllmfield":
      return "LOCAL_LLM";
    default:
      return "STRING";
    }
  }

    /*
    Below functions could be redefined on each Field
    They provide generic Insertions needed
    Each time a new connector is added, a new function could be created here (or in each field)
     */

  public String toStringValue(T value) {
    return value != null ? value.toString() : "null";
  }

  public T toCastValue(String value) {
    return (T) value;
  }

  public String toString(T value) {
    return " " + name + " : " + value.toString() + " ;";
  }

  public String toCSVString(T value) {
    return "\"" + value.toString() + "\",";
  }

  public String toJSONString(T value) {
    return "\"" + name + "\" : " + "\"" + value.toString() + "\", ";
  }

  // This function needs to be overrided in each field
  public Put toHbasePut(T value, Put hbasePut) {
    //hbasePut.addColumn(Bytes.toBytes(hbaseColumnQualifier), Bytes.toBytes(name), Bytes.toBytes(value));
    return hbasePut;
  }

  public SolrInputDocument toSolrDoc(T value, SolrInputDocument doc) {
    doc.addField(name, value);
    return doc;
  }

  public String toOzone(T value) {
    return toString(value);
  }

  public PartialRow toKudu(T value, PartialRow partialRow) {
    partialRow.addObject(name, value);
    return partialRow;
  }

  public Type getKuduType() {
    return Type.BINARY;
  }

  public HivePreparedStatement toHive(T value, int index,
                                      HivePreparedStatement hivePreparedStatement) {
    try {
      hivePreparedStatement.setObject(index, value);
    } catch (SQLException e) {
      log.warn("Could not set value : " + value.toString() +
          " into hive statement due to error :", e);
    }
    return hivePreparedStatement;
  }

  public String getHiveType() {
    return "BINARY";
  }

  public String getGenericRecordType() {
    return "string";
  }

  public Object toAvroValue(T value) {
    return value;
  }

  public ColumnVector getOrcColumnVector(VectorizedRowBatch batch, int cols) {
    return batch.cols[cols];
  }

  public TypeDescription getTypeDescriptionOrc() {
    return TypeDescription.createBinary();
  }

}
