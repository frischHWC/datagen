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
package com.datagen.config;


import java.util.Comparator;


public class ConnectorParser {

  private ConnectorParser() {
    throw new IllegalStateException("Could not initialize this class");
  }

  public static Connector stringToConnector(String connector) {
    return Connector.valueOf(connector);
  }

  public enum Connector {
    CSV,
    JSON,
    AVRO,
    PARQUET,
    ORC,

    HDFS_CSV,
    HDFS_JSON,
    HDFS_PARQUET,
    HDFS_ORC,
    HDFS_AVRO,

    OZONE_PARQUET,
    OZONE_CSV,
    OZONE_AVRO,
    OZONE_JSON,
    OZONE_ORC,

    S3_PARQUET,
    S3_CSV,
    S3_AVRO,
    S3_JSON,
    S3_ORC,

    ADLS_PARQUET,
    ADLS_CSV,
    ADLS_AVRO,
    ADLS_JSON,
    ADLS_ORC,

    GCS_PARQUET,
    GCS_CSV,
    GCS_AVRO,
    GCS_JSON,
    GCS_ORC,

    HIVE,
    HBASE,
    KAFKA,
    KUDU,
    SOLR
    ;

    public static Comparator<Connector> connectorInitPrecedence = new Comparator<>() {
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
