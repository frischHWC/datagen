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
import java.util.LinkedHashMap;
import java.util.List;

@Slf4j
public class LongField extends Field<Long> {

  public LongField(String name, List<Long> possibleValues,
                   LinkedHashMap<String, Long> possible_values_weighted,
                   String min,
                   String max) {
    if (max == null) {
      this.max = Long.MAX_VALUE - 1;
    } else {
      this.max = Long.parseLong(max);
    }
    if (min == null) {
      this.min = Long.MIN_VALUE;
    } else {
      this.min = Long.parseLong(min);
    }
    this.name = name;
    this.possibleValues = possibleValues;
    if (possible_values_weighted != null &&
        !possible_values_weighted.isEmpty()) {
      possible_values_weighted.forEach((value, probability) -> {
        for (long i = 0; i < probability; i++) {
          this.possibleValues.add(Long.valueOf(value));
        }
      });
    }
    this.possibleValueSize = this.possibleValues.size();
  }

  public Long generateRandomValue() {
    if (!possibleValues.isEmpty()) {
      return possibleValues.get(random.nextInt(possibleValues.size()));
    } else {
      return random.longs(1, min, max + 1).findFirst().orElse(0L);
    }
  }

  /*
   Override if needed Field function to insert into special connectors
   */
  @Override
  public String toStringValue(Long value) {
    return value.toString();
  }

  @Override
  public Long toCastValue(String value) {
    return Long.valueOf(value);
  }

  @Override
  public Put toHbasePut(Long value, Put hbasePut) {
    hbasePut.addColumn(Bytes.toBytes(hbaseColumnQualifier), Bytes.toBytes(name),
        Bytes.toBytes(value));
    return hbasePut;
  }

  @Override
  public PartialRow toKudu(Long value, PartialRow partialRow) {
    partialRow.addLong(name, value);
    return partialRow;
  }

  @Override
  public Type getKuduType() {
    return Type.INT64;
  }

  @Override
  public HivePreparedStatement toHive(Long value, int index,
                                      HivePreparedStatement hivePreparedStatement) {
    try {
      hivePreparedStatement.setLong(index, value);
    } catch (SQLException e) {
      log.warn("Could not set value : " + value.toString() +
          " into hive statement due to error :", e);
    }
    return hivePreparedStatement;
  }

  @Override
  public String getHiveType() {
    return "BIGINT";
  }

  @Override
  public String getGenericRecordType() {
    return "long";
  }

  @Override
  public ColumnVector getOrcColumnVector(VectorizedRowBatch batch, int cols) {
    return batch.cols[cols];
  }

  @Override
  public TypeDescription getTypeDescriptionOrc() {
    return TypeDescription.createLong();
  }
}

