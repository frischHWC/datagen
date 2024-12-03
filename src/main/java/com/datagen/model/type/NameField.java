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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class NameField extends Field<NameField.Name> {

  public class Name {
    @Getter
    String first_name;
    @Getter
    String country;
    @Getter
    Boolean unisex;
    @Getter
    Boolean female;
    @Getter
    Boolean male;

    public Name(String name, String country, String male, String female,
                String unisex) {
      this.first_name = name;
      this.country = country;
      this.unisex = unisex.equalsIgnoreCase("true");
      this.male = male.equalsIgnoreCase("true");
      this.female = female.equalsIgnoreCase("true");
    }

    @Override
    public String toString() {
      return "Name{" +
          "name='" + first_name + '\'' +
          ", country='" + country + '\'' +
          ", unisex='" + unisex.toString() + '\'' +
          ", male='" + male.toString() + '\'' +
          ", female='" + female.toString() + '\'' +
          '}';
    }
  }

  private List<Name> nameDico;

  public NameField(String name, List<String> filters) {
    this.name = name;
    this.filters = filters;
    this.nameDico = loadNameDico();

    this.possibleValuesInternal = new ArrayList<>();

    if (!filters.isEmpty()) {
      filters.forEach(filterOnCountry -> {
        this.possibleValuesInternal.addAll(
            nameDico.stream().filter(
                    n -> n.country.equalsIgnoreCase(filterOnCountry))
                .toList());
      });
    } else {
      this.possibleValuesInternal.addAll(
          nameDico.stream()
              .toList()
      );
    }
    this.possibleValueSize = possibleValuesInternal.size();
  }

  public Name generateRandomValue() {
    return possibleValuesInternal.get(random.nextInt(possibleValueSize));
  }

  private List<Name> loadNameDico() {
    try {
      InputStream is = this.getClass().getClassLoader().getResourceAsStream(
          "dictionaries/names.csv");
      return new BufferedReader(
          new InputStreamReader(is, StandardCharsets.UTF_8))
          .lines()
          .map(l -> {
            String[] lineSplitted = l.split(";");
            return new NameField.Name(lineSplitted[0], lineSplitted[1],
                lineSplitted[2], lineSplitted[3], lineSplitted[4]);
          })
          .collect(Collectors.toList());
    } catch (Exception e) {
      log.warn("Could not load names-dico with error : " + e);
      return Collections.singletonList(
          new NameField.Name("Anonymous", "", "", "", ""));
    }
  }

  @Override
  public String toString(Name value) {
    return " " + name + " : " + value.getFirst_name() + " ;";
  }

  @Override
  public String toCSVString(Name value) {
    return "\"" + value.getFirst_name() + "\",";
  }

  @Override
  public String toJSONString(Name value) {
    return "\"" + name + "\" : " + "\"" + value.getFirst_name() + "\", ";
  }

  /*
   Override if needed Field function to insert into special connectors
   */
  @Override
  public String toStringValue(Name value) {
    return value.getFirst_name();
  }

  @Override
  public Name toCastValue(String value) {
    String[] valueSplitted = value.split(";");
    return new Name(valueSplitted[0], valueSplitted[1], valueSplitted[2],
            valueSplitted[3], valueSplitted[4]);
  }

  @Override
  public Put toHbasePut(Name value, Put hbasePut) {
    hbasePut.addColumn(Bytes.toBytes(hbaseColumnQualifier), Bytes.toBytes(name),
        Bytes.toBytes(value.getFirst_name()));
    return hbasePut;
  }

  @Override
  public SolrInputDocument toSolrDoc(Name value, SolrInputDocument doc) {
    doc.addField(name, value.getFirst_name());
    return doc;
  }

  @Override
  public String toOzone(Name value) {
    return value.getFirst_name();
  }

  @Override
  public PartialRow toKudu(Name value, PartialRow partialRow) {
    partialRow.addString(name, value.getFirst_name());
    return partialRow;
  }

  @Override
  public Type getKuduType() {
    return Type.STRING;
  }

  @Override
  public HivePreparedStatement toHive(Name value, int index,
                                      HivePreparedStatement hivePreparedStatement) {
    try {
      hivePreparedStatement.setString(index, value.getFirst_name());
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
  public Object toAvroValue(Name value) {
    return value.getFirst_name();
  }

  @Override
  public ColumnVector getOrcColumnVector(VectorizedRowBatch batch, int cols) {
    return batch.cols[cols];
  }

  @Override
  public TypeDescription getTypeDescriptionOrc() {
    return TypeDescription.createString();
  }

  public static final List<String> listOfAvailableCountries =
      List.of(
          "Albania",
          "Algeria",
          "Arabia",
          "Armenia",
          "Austria",
          "Azerbaijan",
          "Belarus",
          "Belgium",
          "Bosnia",
          "Bulgaria",
          "China",
          "Croatia",
          "Czech",
          "Denmark",
          "Estonia",
          "Finland",
          "France",
          "Georgia",
          "Germany",
          "Greece",
          "Hungary",
          "Iceland",
          "India",
          "Ireland",
          "Israel",
          "Italy",
          "Japan",
          "Kazakhstan",
          "Korea",
          "Kosovo",
          "Latvia",
          "Lithuania",
          "Luxembourg",
          "Macedonia",
          "Moldova",
          "Montenegro",
          "Morocco",
          "Netherlands",
          "Norway",
          "Poland",
          "Portugal",
          "Romania",
          "Russia",
          "Serbia",
          "Slovakia",
          "Slovenia",
          "Spain",
          "Sweden",
          "Switzerland",
          "Tunisia",
          "Turkey",
          "UK",
          "USA",
          "Ukraine",
          "Vietnam"
      );

}