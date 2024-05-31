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
public class CityField extends Field<CityField.City> {

  public class City {
    @Getter
    String name;
    @Getter
    String latitude;
    @Getter
    String longitude;
    @Getter
    String country;
    @Getter
    Long population;

    public City(String name, String lat, String lon, String country,
                Long population) {
      this.name = name;
      this.latitude = lat;
      this.longitude = lon;
      this.country = country;
      this.population = population;
    }

    @Override
    public String toString() {
      return "City{" +
          "name='" + name + '\'' +
          ", latitude='" + latitude + '\'' +
          ", longitude='" + longitude + '\'' +
          ", country='" + country + '\'' +
          ", population='" + population + '\'' +
          '}';
    }
  }

  private List<City> cityDico;

  public CityField(String name, List<String> filters) {
    this.name = name;
    this.cityDico = loadCityDico();

    List<City> possibleCities = new ArrayList<>();
    filters.forEach(filterOnCountry -> {
          possibleCities.addAll(
              this.cityDico.stream()
                  .filter(c -> c.country.equalsIgnoreCase(filterOnCountry))
                  .collect(Collectors.toList()));
        }
    );
    List<City> possibleCitiesAfterLambda = possibleCities;
    if (possibleCities.isEmpty()) {
      this.possibleValues.addAll(this.cityDico);
    } else {

      this.possibleValues = new ArrayList<>();
      City minPop = possibleCitiesAfterLambda.stream()
          .min((c1, c2) -> (int) (c1.population - c2.population))
          .orElse(new City("", "", "", "", 1L));

      possibleCitiesAfterLambda.forEach(city -> {
        long occurencesToCreate = city.population / minPop.population;
        for (long i = 0; i <= occurencesToCreate; i++) {
          this.possibleValues.add(city);
        }
      });

    }

    this.possibleValueSize = this.possibleValues.size();

  }

  private List<City> loadCityDico() {
    try {
      InputStream is = this.getClass().getClassLoader().getResourceAsStream(
          "dictionaries/worldcities.csv");
      return new BufferedReader(
          new InputStreamReader(is, StandardCharsets.UTF_8))
          .lines()
          .filter(l -> !l.startsWith("name"))
          .map(l -> {
            String[] lineSplitted = l.split(";");
            return new City(lineSplitted[0], lineSplitted[1], lineSplitted[2],
                lineSplitted[3], Long.valueOf(lineSplitted[4]));
          })
          .collect(Collectors.toList());
    } catch (Exception e) {
      log.warn("Could not load world cities, error : ", e);
      return Collections.singletonList(
          new City("world", "0", "0", "world", 8000000000L));
    }
  }

  public City generateRandomValue() {
    return this.possibleValues.get(random.nextInt(this.possibleValueSize));
  }

  @Override
  public String toString(City value) {
    return " " + name + " : " + value.getName() + " ;";
  }

  @Override
  public String toCSVString(City value) {
    return "\"" + value.getName() + "\",";
  }

  @Override
  public String toJSONString(City value) {
    return "\"" + name + "\" : " + "\"" + value.getName() + "\", ";
  }

  /*
   Override if needed Field function to insert into special connectors
   */
  @Override
  public String toStringValue(City value) {
    return value.getName();
  }

  @Override
  public City toCastValue(String value) {
    String[] valueSplitted = value.split(";");
    return new City(valueSplitted[0], valueSplitted[1], valueSplitted[2],
        valueSplitted[3], Long.valueOf(valueSplitted[4]));
  }

  @Override
  public Put toHbasePut(City value, Put hbasePut) {
    hbasePut.addColumn(Bytes.toBytes(hbaseColumnQualifier), Bytes.toBytes(name),
        Bytes.toBytes(value.name));
    return hbasePut;
  }

  @Override
  public SolrInputDocument toSolrDoc(City value, SolrInputDocument doc) {
    doc.addField(name, value.getName());
    return doc;
  }

  @Override
  public String toOzone(City value) {
    return value.getName();
  }

  @Override
  public PartialRow toKudu(City value, PartialRow partialRow) {
    partialRow.addString(name, value.getName());
    return partialRow;
  }

  @Override
  public Type getKuduType() {
    return Type.STRING;
  }

  @Override
  public HivePreparedStatement toHive(City value, int index,
                                      HivePreparedStatement hivePreparedStatement) {
    try {
      hivePreparedStatement.setString(index, value.getName());
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
  public Object toAvroValue(City value) {
    return value.getName();
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