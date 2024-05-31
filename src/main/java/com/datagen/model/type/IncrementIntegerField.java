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

@Slf4j
public class IncrementIntegerField extends Field<Integer> {

  private Integer counter = 0;

  public IncrementIntegerField(String name,
                               String min) {
    this.name = name;
    if (min == null) {
      this.min = 0L;
    } else {
      this.min = Long.parseLong(min);
    }
    counter = this.min.intValue();
  }

  public synchronized Integer generateRandomValue() {
    counter++;
    return counter;
  }

    /*
    Override if needed Field function to insert into special connectors
    */

  @Override
  public String toStringValue(Integer value) {
    return value.toString();
  }

  @Override
  public Integer toCastValue(String value) {
    return Integer.valueOf(value);
  }

  @Override
  public Put toHbasePut(Integer value, Put hbasePut) {
    hbasePut.addColumn(Bytes.toBytes(hbaseColumnQualifier), Bytes.toBytes(name),
        Bytes.toBytes(value));
    return hbasePut;
  }

  @Override
  public PartialRow toKudu(Integer value, PartialRow partialRow) {
    partialRow.addInt(name, value);
    return partialRow;
  }

  @Override
  public Type getKuduType() {
    return Type.INT32;
  }

  @Override
  public HivePreparedStatement toHive(Integer value, int index,
                                      HivePreparedStatement hivePreparedStatement) {
    try {
      hivePreparedStatement.setInt(index, value);
    } catch (SQLException e) {
      log.warn("Could not set value : " + value.toString() +
          " into hive statement due to error :", e);
    }
    return hivePreparedStatement;
  }

  @Override
  public String getHiveType() {
    return "INT";
  }

  @Override
  public String getGenericRecordType() {
    return "int";
  }

  @Override
  public ColumnVector getOrcColumnVector(VectorizedRowBatch batch, int cols) {
    return batch.cols[cols];
  }

  @Override
  public TypeDescription getTypeDescriptionOrc() {
    return TypeDescription.createInt();
  }

}
