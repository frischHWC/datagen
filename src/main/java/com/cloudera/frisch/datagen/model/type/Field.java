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
package com.cloudera.frisch.datagen.model.type;


import com.cloudera.frisch.datagen.model.Row;
import com.cloudera.frisch.datagen.model.conditions.ConditionalEvaluator;
import com.fasterxml.jackson.databind.JsonNode;
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
import java.util.stream.Collectors;


/**
 * This abstract class describes a field with three characteristics: Name, Type, Length (which is optional)
 * Goal is also to describe how a field is rendered according to its type
 * Every new type added should extends this abstract class in a new Java Class (and override generateRandomValue())
 */
@Slf4j
public abstract class Field<T> {

  Random random = new Random();

  @Getter
  @Setter
  public String name;

  @Getter
  @Setter
  public Boolean computed = false;

  @Getter
  @Setter
  public List<T> possibleValues;

  @Getter
  @Setter
  public Integer possibleValueSize;

  @Getter
  @Setter
  public List<T> filters;

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

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Class Type is " + this.getClass().getSimpleName() + " ; ");
    sb.append("name : " + name + " ; ");
    sb.append("hbase Column Qualifier : " + hbaseColumnQualifier + " ; ");
    sb.append("Length : " + length + " ; ");
    if (min != null) {
      sb.append("Min : " + min + " ; ");
    }
    if (max != null) {
      sb.append("Max : " + max + " ; ");
    }
    return sb.toString();
  }

  public abstract T generateRandomValue();

  public T generateComputedValue(Row row) {
    return toCastValue(conditional.evaluateConditions(row));
  }

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
   * Create the right instance of a field (i.e. String, password etc..) according to its type
   *
   * @param name            of the field that will be created
   * @param type            of the field, which is used to instantiate the right field
   * @param length          of the field, in could be null or -1, in this case, it will be ignored and default field length will be used
   * @param columnQualifier Hbase column qualifier if there is one
   * @return Field instantiated or null if type has not been recognized
   */
  public static Field instantiateField(String name,
                                       String type,
                                       Integer length,
                                       String columnQualifier,
                                       List<JsonNode> possibleValues,
                                       LinkedHashMap<String, Long> possible_values_weighted,
                                       LinkedHashMap<String, String> conditionals,
                                       String min,
                                       String max,
                                       List<JsonNode> filters,
                                       String file,
                                       String separator,
                                       String pattern,
                                       Boolean useNow,
                                       String regex,
                                       Boolean ghost,
                                       String mainField) {
    if (name == null || name.isEmpty()) {
      throw new IllegalStateException(
          "Name can not be null or empty for field: " + name);
    }
    if (type == null || type.isEmpty()) {
      throw new IllegalStateException(
          "Type can not be null or empty for field: " + name);
    }

    // If length is not precised, it should be let as is (default is -1) and let each type handles it
    if (length == null || length < 1) {
      length = -1;
    }

    Field field;

    switch (type.toUpperCase()) {
    case "STRING":
      field = new StringField(name, length,
          possibleValues.stream().map(JsonNode::asText)
              .collect(Collectors.toList()), possible_values_weighted);
      break;
    case "STRINGAZ":
      field = new StringAZField(name, length,
          possibleValues.stream().map(JsonNode::asText)
              .collect(Collectors.toList()));
      break;
    case "INTEGER":
      field = new IntegerField(name,
          possibleValues.stream().map(JsonNode::asInt)
              .collect(Collectors.toList()), possible_values_weighted, min,
          max);
      break;
    case "INCREMENT_INTEGER":
      field = new IncrementIntegerField(name, min);
      break;
    case "BOOLEAN":
      field = new BooleanField(name,
          possibleValues.stream().map(JsonNode::asBoolean)
              .collect(Collectors.toList()), possible_values_weighted);
      break;
    case "FLOAT":
      field = new FloatField(name,
          possibleValues.stream().map(j -> (float) j.asDouble())
              .collect(Collectors.toList()), possible_values_weighted, min,
          max);
      break;
    case "LONG":
      field = new LongField(name,
          possibleValues.stream().map(JsonNode::asLong)
              .collect(Collectors.toList()), possible_values_weighted, min,
          max);
      break;
    case "INCREMENT_LONG":
      field = new IncrementLongField(name, min);
      break;
    case "TIMESTAMP":
      field = new TimestampField(name,
          possibleValues.stream().map(JsonNode::asLong)
              .collect(Collectors.toList()));
      break;
    case "BYTES":
      field = new BytesField(name, length,
          possibleValues.stream().map(j -> j.asText().getBytes())
              .collect(Collectors.toList()));
      break;
    case "HASHMD5":
      field = new HashMd5Field(name, length,
          possibleValues.stream().map(j -> j.asText().getBytes())
              .collect(Collectors.toList()));
      break;
    case "BIRTHDATE":
      field = new BirthdateField(name, length,
          possibleValues.stream().map(JsonNode::asText)
              .collect(Collectors.toList()), min, max);
      break;
    case "NAME":
      field = new NameField(name, length,
          filters.stream().map(JsonNode::asText).collect(Collectors.toList()));
      break;
    case "COUNTRY":
      field = new CountryField(name, length,
          possibleValues.stream().map(JsonNode::asText)
              .collect(Collectors.toList()));
      break;
    case "CITY":
      field = new CityField(name,
          filters.stream().map(JsonNode::asText).collect(Collectors.toList()));
      break;
    case "BLOB":
      field = new BlobField(name, length,
          possibleValues.stream().map(j -> j.asText().getBytes())
              .collect(Collectors.toList()));
      break;
    case "EMAIL":
      field = new EmailField(name,
          possibleValues.stream().map(JsonNode::asText)
              .collect(Collectors.toList()),
          filters.stream().map(JsonNode::asText).collect(Collectors.toList()));
      break;
    case "IP":
      field = new IpField(name);
      break;
    case "LINK":
      field = new LinkField(name, length,
          possibleValues.stream().map(JsonNode::asText)
              .collect(Collectors.toList()));
      break;
    case "CSV":
      field = new CsvField(name, length,
          filters.stream().map(JsonNode::asText).collect(Collectors.toList()),
          file, separator, mainField);
      break;
    case "PHONE":
      field = new PhoneField(name, length,
          filters.stream().map(JsonNode::asText).collect(Collectors.toList()));
      break;
    case "UUID":
      field = new UuidField(name);
      break;
    case "DATE":
      field = new DateField(name, possibleValues.stream().map(JsonNode::asText)
          .collect(Collectors.toList()), min, max, useNow);
      break;
    case "DATE_AS_STRING":
      field = new DateAsStringField(name, possibleValues.stream().map(JsonNode::asText)
          .collect(Collectors.toList()), min, max, useNow, pattern);
      break;
    case "STRING_REGEX":
      field = new StringRegexField(name, regex);
      break;
    default:
      log.warn("Type : " + type +
          " has not been recognized and hence will be ignored");
      return null;
    }

    // If hbase column qualifier is not precised, it should be let as is (default is "cq")
    if (columnQualifier != null && !columnQualifier.isEmpty()) {
      field.setHbaseColumnQualifier(columnQualifier);
    }

    field.setGhost(ghost);

    // If there are some conditions, we consider this field as computed (meaning it requires other fields' values to get its value)
    if (conditionals != null && !conditionals.isEmpty()) {
      log.debug("Field has been marked as conditional: " + field);
      field.setComputed(true);
      field.setConditional(new ConditionalEvaluator(conditionals));
    }

    if (log.isDebugEnabled()) {
      log.debug("Field has been created: " + field);
    }

    return field;
  }

  public String getTypeForModel() {
    switch (this.getClass().getSimpleName().toLowerCase(Locale.ROOT)) {
    case "birthdatefield":
      return "BIRTHDATE";
    case "blobfield":
      return "BLOB";
    case "booleanfield":
      return "BOOLEAN";
    case "bytesfield":
      return "BYTES";
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
      return "HASHMD5";
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
      return "STRINGAZ";
    case "stringfield":
      return "STRING";
    case "stringregexfield":
      return "STRING_REGEX";
    case "timestampfield":
      return "TIMESTAMP";
    case "uuidfiel":
      return "UUID";
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
