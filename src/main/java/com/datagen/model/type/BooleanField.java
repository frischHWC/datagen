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
public class BooleanField extends Field<Boolean> {

  public BooleanField(String name, List<Boolean> possibleValues,
                      LinkedHashMap<String, Long> possible_values_weighted) {
    this.name = name;
    this.possibleValues = possibleValues;
    if (possible_values_weighted != null &&
        !possible_values_weighted.isEmpty()) {
      possible_values_weighted.forEach((value, probability) -> {
        for (long i = 0; i < probability; i++) {
          this.possibleValues.add(Boolean.valueOf(value));
        }
      });
    }
    this.possibleValueSize = this.possibleValues.size();
  }

  public Boolean generateRandomValue() {
    if (!possibleValues.isEmpty()) {
      return possibleValues.get(random.nextInt(possibleValues.size()));
    } else {
      return random.nextBoolean();
    }
  }

    /*
     Override if needed Field function to insert into special connectors
     */

  @Override
  public String toStringValue(Boolean value) {
    return value.toString();
  }

  @Override
  public Boolean toCastValue(String value) {
    return Boolean.valueOf(value);
  }

  @Override
  public Put toHbasePut(Boolean value, Put hbasePut) {
    hbasePut.addColumn(Bytes.toBytes(hbaseColumnQualifier), Bytes.toBytes(name),
        Bytes.toBytes(value));
    return hbasePut;
  }

  @Override
  public PartialRow toKudu(Boolean value, PartialRow partialRow) {
    partialRow.addBoolean(name, value);
    return partialRow;
  }

  @Override
  public Type getKuduType() {
    return Type.BOOL;
  }

  @Override
  public HivePreparedStatement toHive(Boolean value, int index,
                                      HivePreparedStatement hivePreparedStatement) {
    try {
      hivePreparedStatement.setBoolean(index, value);
    } catch (SQLException e) {
      log.warn("Could not set value : " + value.toString() +
          " into hive statement due to error :", e);
    }
    return hivePreparedStatement;
  }

  @Override
  public String getHiveType() {
    return "BOOLEAN";
  }

  @Override
  public String getGenericRecordType() {
    return "boolean";
  }

  @Override
  public ColumnVector getOrcColumnVector(VectorizedRowBatch batch, int cols) {
    return batch.cols[cols];
  }

  @Override
  public TypeDescription getTypeDescriptionOrc() {
    return TypeDescription.createBoolean();
  }

}
