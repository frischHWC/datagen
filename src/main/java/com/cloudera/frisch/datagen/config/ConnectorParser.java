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
package com.cloudera.frisch.datagen.config;


import java.util.Comparator;


public class ConnectorParser {

  private ConnectorParser() {
    throw new IllegalStateException("Could not initialize this class");
  }

  public static Connector stringToConnector(String connector) {
    switch (connector.toUpperCase()) {
    case "HDFS-CSV":
      return Connector.HDFS_CSV;
    case "HDFS-JSON":
      return Connector.HDFS_JSON;
    case "HDFS-PARQUET":
      return Connector.HDFS_PARQUET;
    case "HDFS-ORC":
      return Connector.HDFS_ORC;
    case "HDFS-AVRO":
      return Connector.HDFS_AVRO;
    case "HBASE":
      return Connector.HBASE;
    case "HIVE":
      return Connector.HIVE;
    case "KAFKA":
      return Connector.KAFKA;
    case "OZONE-PARQUET":
      return Connector.OZONE_PARQUET;
    case "OZONE-CSV":
      return Connector.OZONE_CSV;
    case "OZONE-JSON":
      return Connector.OZONE_JSON;
    case "OZONE-ORC":
      return Connector.OZONE_ORC;
    case "OZONE-AVRO":
      return Connector.OZONE_AVRO;
    case "SOLR":
      return Connector.SOLR;
    case "KUDU":
      return Connector.KUDU;
    case "CSV":
      return Connector.CSV;
    case "JSON":
      return Connector.JSON;
    case "AVRO":
      return Connector.AVRO;
    case "PARQUET":
      return Connector.PARQUET;
    case "ORC":
      return Connector.ORC;
    case "S3-PARQUET":
      return Connector.S3_PARQUET;
    case "S3-CSV":
      return Connector.S3_CSV;
    case "S3-JSON":
      return Connector.S3_JSON;
    case "S3-ORC":
      return Connector.S3_ORC;
    case "S3-AVRO":
      return Connector.S3_AVRO;
    default:
      return null;
    }
  }

  public enum Connector {
    HDFS_CSV,
    HDFS_JSON,
    HDFS_PARQUET,
    HDFS_ORC,
    HDFS_AVRO,
    HBASE,
    HIVE,
    KAFKA,
    OZONE_PARQUET,
    OZONE_CSV,
    OZONE_AVRO,
    OZONE_JSON,
    OZONE_ORC,
    SOLR,
    KUDU,
    CSV,
    JSON,
    AVRO,
    PARQUET,
    ORC,
    S3_PARQUET,
    S3_CSV,
    S3_AVRO,
    S3_JSON,
    S3_ORC;

    public static Comparator<Connector> conenctorInitPrecedence = new Comparator<>() {
      @Override
      public int compare(Connector s1, Connector s2) {
        if (s1.equals(Connector.OZONE_AVRO) ||
            s1.equals(Connector.OZONE_JSON) ||
            s1.equals(Connector.OZONE_CSV) ||
            s1.equals(Connector.OZONE_PARQUET) ||
            s1.equals(Connector.OZONE_ORC)) {
          if (s2.equals(Connector.OZONE_AVRO) ||
              s2.equals(Connector.OZONE_JSON) ||
              s2.equals(Connector.OZONE_CSV) ||
              s2.equals(Connector.OZONE_PARQUET) ||
              s2.equals(Connector.OZONE_ORC)) {
            return 0;
          } else {
            return -1;
          }
        } else if (s1.equals(Connector.HIVE) || s1.equals(Connector.KUDU)) {
          if (s2.equals(Connector.OZONE_AVRO) ||
              s2.equals(Connector.OZONE_JSON) ||
              s2.equals(Connector.OZONE_CSV) ||
              s2.equals(Connector.OZONE_PARQUET) ||
              s2.equals(Connector.OZONE_ORC)) {
            return 1;
          } else if (s2.equals(Connector.HIVE) || s2.equals(Connector.KUDU)) {
            return 0;
          } else {
            return -1;
          }
        } else {
          return 0;
        }
      }
    };
  }
}
