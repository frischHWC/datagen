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

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hive.jdbc.HivePreparedStatement;
import org.apache.kudu.Type;
import org.apache.kudu.client.PartialRow;
import org.apache.orc.TypeDescription;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;


@Slf4j
public class CountryField extends Field<String> {

  private List<String> countryDico;

  public CountryField(String name,
                      HashMap<String, Long> possible_values_weighted) {
    this.name = name;
    this.possibleValuesProvided = new ArrayList<>();
    if (possible_values_weighted != null &&
        !possible_values_weighted.isEmpty()) {
      possible_values_weighted.forEach((value, probability) -> {
        for (long i = 0; i < probability; i++) {
          this.possibleValuesProvided.add(value);
        }
      });
    }
    this.possibleValueSize = this.possibleValuesProvided.size();
    this.countryDico =
        possibleValuesProvided.isEmpty() ? loadCountryDico() :
            possibleValuesProvided;
  }

  public String generateRandomValue() {
    return countryDico.get(random.nextInt(countryDico.size()));
  }

  private List<String> loadCountryDico() {
    try {
      InputStream is = this.getClass().getClassLoader().getResourceAsStream(
          "dictionaries/country-dico.txt");
      return new BufferedReader(
          new InputStreamReader(is, StandardCharsets.UTF_8))
          .lines()
          .collect(Collectors.toList());
    } catch (Exception e) {
      log.warn("Could not load country-dico with error : " + e);
      return Collections.singletonList("World");
    }
  }

    /*
     Override if needed Field function to insert into special connectors
     */

  @Override
  public Put toHbasePut(String value, Put hbasePut) {
    hbasePut.addColumn(Bytes.toBytes(hbaseColumnQualifier), Bytes.toBytes(name),
        Bytes.toBytes(value));
    return hbasePut;
  }

  @Override
  public PartialRow toKudu(String value, PartialRow partialRow) {
    partialRow.addString(name, value);
    return partialRow;
  }

  @Override
  public Type getKuduType() {
    return Type.STRING;
  }

  @Override
  public HivePreparedStatement toHive(String value, int index,
                                      HivePreparedStatement hivePreparedStatement) {
    try {
      hivePreparedStatement.setString(index, value);
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
  public ColumnVector getOrcColumnVector(VectorizedRowBatch batch, int cols) {
    return batch.cols[cols];
  }

  @Override
  public TypeDescription getTypeDescriptionOrc() {
    return TypeDescription.createString();
  }


}