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
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class DateField extends Field<LocalDateTime> {

  DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;
  private final boolean useNow;

  public DateField(String name, List<String> possibleValues,
                   String min, String max, boolean useNow) {
    // For reference on how to specify an input date as possible values or min/max,
    // see: https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html
    this.name = name;
    this.useNow = useNow;
    this.possibleValues = possibleValues.stream()
        .map(p -> LocalDateTime.parse(p, formatter))
        .collect(Collectors.toList());

    if (min == null) {
      this.min = 0L;
    } else {
      String minFormatted = min.substring(0,11) + "T" + min.substring(12) + "Z";
      this.min = LocalDateTime.parse(minFormatted, formatter).toEpochSecond(ZoneOffset.UTC);
    }
    if (max == null) {
      this.max = LocalDateTime.MAX.toEpochSecond(ZoneOffset.UTC);
    } else {
        String maxFormatted = max.substring(0,11) + "T" + max.substring(12) + "Z";
      this.max = LocalDateTime.parse(maxFormatted, formatter).toEpochSecond(ZoneOffset.UTC);
    }
  }

  /**
   * Generates a random date with a pattern if specified
   *
   * @return
   */
  public LocalDateTime generateRandomValue() {
    if(useNow) {
      return LocalDateTime.now();
    } else if (possibleValues.isEmpty()) {
      Long randomDate = random.longs(1, min, max + 1).findFirst().orElse(0L);
      return LocalDateTime.ofEpochSecond(randomDate, 0, ZoneOffset.UTC);
    } else {
      return possibleValues.get(random.nextInt(possibleValues.size()));
    }
  }

    /*
     Override if needed Field function to insert into special sinks
     */

  @Override
  public String toStringValue(LocalDateTime value) {
    return value.toString();
  }

  @Override
  public LocalDateTime toCastValue(String value) {
    return LocalDateTime.parse(value, formatter);
  }

  @Override
  public Put toHbasePut(LocalDateTime value, Put hbasePut) {
    hbasePut.addColumn(Bytes.toBytes(hbaseColumnQualifier), Bytes.toBytes(name),
        Bytes.toBytes(value.toString()));
    return hbasePut;
  }

  @Override
  public PartialRow toKudu(LocalDateTime value, PartialRow partialRow) {
    partialRow.addDate(name, java.sql.Date.valueOf(value.toLocalDate()));
    return partialRow;
  }

  @Override
  public Type getKuduType() {
    return Type.DATE;
  }

  @Override
  public HivePreparedStatement toHive(LocalDateTime value, int index,
                                      HivePreparedStatement hivePreparedStatement) {
    try {
      hivePreparedStatement.setDate(index, java.sql.Date.valueOf(value.toLocalDate()));
    } catch (SQLException e) {
      log.warn("Could not set value : " + value.toString() +
          " into hive statement due to error :", e);
    }
    return hivePreparedStatement;
  }

  @Override
  public String getHiveType() {
    return "DATE";
  }

  @Override
  public String getGenericRecordType() {
    return "string";
  }

  @Override
  public Object toAvroValue(LocalDateTime value) {
    return value.toString();
  }

  @Override
  public ColumnVector getOrcColumnVector(VectorizedRowBatch batch, int cols) {
    return batch.cols[cols];
  }

  @Override
  public TypeDescription getTypeDescriptionOrc() {
    return TypeDescription.createDate();
  }

}