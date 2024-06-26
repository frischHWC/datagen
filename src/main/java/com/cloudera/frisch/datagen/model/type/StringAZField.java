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
import java.util.List;
import java.util.Random;

@Slf4j
public class StringAZField extends Field<String> {

  public StringAZField(String name, Integer length,
                       List<String> possibleValues) {
    this.name = name;
    if (length == null || length < 1) {
      this.length = 20;
    } else {
      this.length = length;
    }
    this.possibleValues = possibleValues;
  }

  public String generateRandomValue() {
    return possibleValues.isEmpty() ?
        getAlphaString(this.length, random) :
        possibleValues.get(random.nextInt(possibleValues.size()));
  }

  /**
   * Generates a random Alpha string [A-Z] of specified length
   *
   * @param n      equals length of the string to generate
   * @param random Random object used to generate random string
   * @return
   */
  String getAlphaString(int n, Random random) {
    // chose a Character random from this String
    String alphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        + "abcdefghijklmnopqrstuvxyz";
    // create StringBuffer size of alphaNumericString
    StringBuilder sb = new StringBuilder(n);
    for (int i = 0; i < n; i++) {
      // generate a random number between
      // 0 to alphaNumericString variable length
      int index
          = (int) (alphaNumericString.length()
          * random.nextDouble());
      // add Character one by one in end of sb
      sb.append(alphaNumericString
          .charAt(index));
    }
    return sb.toString();
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
  public ColumnVector getOrcColumnVector(VectorizedRowBatch batch, int cols) {
    return batch.cols[cols];
  }

  @Override
  public TypeDescription getTypeDescriptionOrc() {
    return TypeDescription.createString();
  }
}
