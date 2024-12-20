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

import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;

@Slf4j
public class BirthdateField extends Field<LocalDate> {

  DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");

  public BirthdateField(String name, HashMap<String, Long> possible_values_weighted,
                 LocalDate min, LocalDate max) {
    this.name = name;
    this.possibleValuesProvided = new ArrayList<>();
    if (possible_values_weighted != null &&
        !possible_values_weighted.isEmpty()) {
      possible_values_weighted.forEach((value, probability) -> {
        for (long i = 0; i < probability; i++) {
          this.possibleValuesProvided.add(LocalDate.parse(value, formatter));
        }
      });
    }
    this.possibleValueSize = this.possibleValuesProvided.size();

    if (min == null) {
      this.min = LocalDate.of(1920, 1, 1).toEpochDay();
    } else {
      this.min = min.toEpochDay();
    }
    if (max == null) {
      this.max = LocalDate.of(2024, 1, 1).toEpochDay();
    } else {
      this.max = max.toEpochDay();
    }
  }

  /**
   * Generates a random birth date between 1910 & 2024 (unless min & max are specified)
   * @return
   */
  public LocalDate generateRandomValue() {
    if (possibleValuesProvided.isEmpty()) {
      Long randomDay = random.longs(1, min, max + 1).findFirst().orElse(0L);
      return LocalDate.ofEpochDay(randomDay);
    } else {
      return possibleValuesProvided.get(random.nextInt(possibleValuesProvided.size()));
    }
  }

    /*
     Override if needed Field function to insert into special connectors
     */

  @Override
  public String toCSVString(LocalDate value) {
    return "\"" + formatter.format(value) + "\",";
  }

  @Override
  public String toStringValue(LocalDate value) {
    return value.toString();
  }

  @Override
  public LocalDate toCastValue(String value) {
    String[] dateSplit = value.split("[/]");
    return LocalDate.of(Integer.parseInt(dateSplit[2]),
        Integer.parseInt(dateSplit[1]), Integer.parseInt(dateSplit[0]));
  }

  @Override
  public Put toHbasePut(LocalDate value, Put hbasePut) {
    hbasePut.addColumn(Bytes.toBytes(hbaseColumnQualifier), Bytes.toBytes(name),
        Bytes.toBytes(value.toString()));
    return hbasePut;
  }

  @Override
  public PartialRow toKudu(LocalDate value, PartialRow partialRow) {
    partialRow.addString(name, value.toString());
    return partialRow;
  }

  @Override
  public Type getKuduType() {
    return Type.STRING;
  }

  @Override
  public HivePreparedStatement toHive(LocalDate value, int index,
                                      HivePreparedStatement hivePreparedStatement) {
    try {
      hivePreparedStatement.setString(index, value.toString());
    } catch (SQLException e) {
      log.warn("Could not set value : " + value.toString() +
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
  public Object toAvroValue(LocalDate value) {
    return value.toString();
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