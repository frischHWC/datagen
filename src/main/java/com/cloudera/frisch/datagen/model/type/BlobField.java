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

import javax.xml.bind.DatatypeConverter;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

@Slf4j
public class BlobField extends Field<byte[]> {

  public BlobField(String name, Integer length, List<byte[]> possibleValues) {
    this.name = name;
    if (length == null || length < 1) {
      this.length = 1_000_000;
    } else {
      this.length = length;
    }
    this.possibleValues = possibleValues;
  }

  public byte[] generateRandomValue() {
    if (possibleValues.isEmpty()) {
      byte[] bytesArray = new byte[length];
      random.nextBytes(bytesArray);
      return bytesArray;
    } else {
      return possibleValues.get(random.nextInt(possibleValues.size()));
    }
  }

  @Override
  public String toString(byte[] value) {
    return " " + name + " : " +
        DatatypeConverter.printHexBinary(value).toUpperCase() + " ;";
  }

  @Override
  public String toCSVString(byte[] value) {
    return "\"" + DatatypeConverter.printHexBinary(value).toUpperCase() + "\",";
  }


  /*
   Override if needed Field function to insert into special connectors
   */
  @Override
  public String toStringValue(byte[] value) {
    return DatatypeConverter.printHexBinary(value);
  }

  @Override
  public byte[] toCastValue(String value) {
    return value.getBytes();
  }

  @Override
  public Put toHbasePut(byte[] value, Put hbasePut) {
    hbasePut.addColumn(Bytes.toBytes(hbaseColumnQualifier), Bytes.toBytes(name),
        value);
    return hbasePut;
  }

  @Override
  public PartialRow toKudu(byte[] value, PartialRow partialRow) {
    partialRow.addBinary(name, value);
    return partialRow;
  }

  @Override
  public Type getKuduType() {
    return Type.BINARY;
  }

  @Override
  public HivePreparedStatement toHive(byte[] value, int index,
                                      HivePreparedStatement hivePreparedStatement) {
    try {
      hivePreparedStatement.setBytes(index, value);
    } catch (SQLException e) {
      log.warn("Could not set value : " + Arrays.toString(value) +
          " into hive statement due to error :", e);
    }
    return hivePreparedStatement;
  }

  @Override
  public String getHiveType() {
    return "BINARY";
  }

  @Override
  public String getGenericRecordType() {
    return "bytes";
  }

  @Override
  public Object toAvroValue(byte[] value) {
    return ByteBuffer.wrap(value);
  }

  @Override
  public ColumnVector getOrcColumnVector(VectorizedRowBatch batch, int cols) {
    return batch.cols[cols];
  }

  @Override
  public TypeDescription getTypeDescriptionOrc() {
    return TypeDescription.createBinary();
  }

}