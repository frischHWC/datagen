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

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hive.jdbc.HivePreparedStatement;
import org.apache.kudu.Type;
import org.apache.kudu.client.PartialRow;
import org.apache.orc.TypeDescription;
import org.apache.solr.common.SolrInputDocument;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class CsvField extends Field<Map<String, String>> {

  // We suppose that each row of the CSV read will fit in a map of string to string (everything is converted to a string)
  @Getter
  private final String file;
  @Getter
  private final String separator;
  @Getter
  private final String mainField;
  private final LinkedList<String> columnNames;


  public CsvField(String name, List<String> filters,
                  String file,
                  String separator, String mainField) {
    this.name = name;
    this.file = file;
    this.separator = separator;
    this.mainField = mainField;
    this.filters = filters;
    this.columnNames = new LinkedList<>();
    this.possibleValuesProvided = loadDico(filters);
  }

  // Load the CSV with filters applied if needed
  private List<Map<String, String>> loadDico(List<String> filters) {
    try {
      InputStream is = new FileInputStream(this.file);
      BufferedReader bf =
          new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
      String header = bf.readLine();
      this.columnNames.addAll(Arrays.asList(header.split(this.separator)));

      return bf.lines()
          .map(l -> {
            HashMap<String, String> map = new HashMap<>();
            String[] lineSplitted = l.split(this.separator);
            int indexOfline = 0;
            for (String colValue : lineSplitted) {
              map.put(this.columnNames.get(indexOfline), colValue);
              indexOfline++;
            }
            return map;
          })
          .filter(row -> {
            boolean toKeep = true;
            if (filters != null && !filters.isEmpty()) {
              for (String filter : filters) {
                String[] splittedFilter = filter.split("=");
                if (!row.get(splittedFilter[0])
                    .equalsIgnoreCase(splittedFilter[1])) {
                  toKeep = false;
                }
              }
            }
            return toKeep;
          })
          .collect(Collectors.toList());

    } catch (Exception e) {
      log.error("Could not load CSVs, error : " + e);
      String stackTrace = Arrays.stream(e.getStackTrace()).
          map(stackTraceElement -> stackTraceElement.toString())
          .reduce("",
              (subtot, element) -> subtot + System.lineSeparator() + element);
      log.error("Stacktrace: " + stackTrace);
      return Collections.singletonList(Map.of(this.mainField, ""));
    }
  }

  public Map<String, String> generateRandomValue() {
    return possibleValuesProvided.get(random.nextInt(possibleValuesProvided.size()));
  }

  @Override
  public String toString(Map<String, String> value) {
    return " " + name + " : " + value.get(this.mainField) + " ;";
  }

  @Override
  public String toCSVString(Map<String, String> value) {
    return "\"" + value.get(this.mainField) + "\",";
  }

  @Override
  public String toJSONString(Map<String, String> value) {
    return "\"" + name + "\" : " + "\"" + value.get(this.mainField) + "\", ";
  }

  /*
   Override if needed Field function to insert into special connectors
   */
  @Override
  public String toStringValue(Map<String, String> value) {
    return value.get(this.mainField);
  }

  @Override
  public Map<String, String> toCastValue(String value) {
    String[] valueSplitted = value.split(";");
    HashMap<String, String> map = new HashMap<>();
    int indexOfline = 0;
    for (String colValue : valueSplitted) {
      map.put(this.columnNames.get(indexOfline), colValue);
      indexOfline++;
    }
    return map;
  }

  @Override
  public Put toHbasePut(Map<String, String> value, Put hbasePut) {
    hbasePut.addColumn(Bytes.toBytes(hbaseColumnQualifier), Bytes.toBytes(name),
        Bytes.toBytes(value.get(this.mainField)));
    return hbasePut;
  }

  @Override
  public SolrInputDocument toSolrDoc(Map<String, String> value,
                                     SolrInputDocument doc) {
    doc.addField(name, value.get(this.mainField));
    return doc;
  }

  @Override
  public String toOzone(Map<String, String> value) {
    return value.get(this.mainField);
  }

  @Override
  public PartialRow toKudu(Map<String, String> value, PartialRow partialRow) {
    partialRow.addString(name, value.get(this.mainField));
    return partialRow;
  }

  @Override
  public Type getKuduType() {
    return Type.STRING;
  }

  @Override
  public HivePreparedStatement toHive(Map<String, String> value, int index,
                                      HivePreparedStatement hivePreparedStatement) {
    try {
      hivePreparedStatement.setString(index, value.get(this.mainField));
    } catch (SQLException e) {
      log.warn("Could not set value : " + value +
          " into hive statement due to error :", e);
    }
    return hivePreparedStatement;
  }

  @Override
  public String getHiveType() {
    return "STRING";
  }

  @Override
  public String getGenericRecordType() {
    return "string";
  }

  @Override
  public Object toAvroValue(Map<String, String> value) {
    return value.get(this.mainField);
  }

  @Override
  public ColumnVector getOrcColumnVector(VectorizedRowBatch batch, int cols) {
    return batch.cols[cols];
  }

  @Override
  public TypeDescription getTypeDescriptionOrc() {
    return TypeDescription.createString();
  }


}