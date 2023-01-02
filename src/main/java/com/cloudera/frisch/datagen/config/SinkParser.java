/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.frisch.datagen.config;


import java.util.Comparator;


public class SinkParser {

    private SinkParser() { throw new IllegalStateException("Could not initialize this class"); }

    public static Sink stringToSink(String sink) {
        switch (sink.toUpperCase()) {
            case "HDFS-CSV": return Sink.HDFS_CSV;
            case "HDFS-JSON": return Sink.HDFS_JSON;
            case "HDFS-PARQUET": return Sink.HDFS_PARQUET;
            case "HDFS-ORC": return Sink.HDFS_ORC;
            case "HDFS-AVRO": return Sink.HDFS_AVRO;
            case "HBASE": return Sink.HBASE;
            case "HIVE": return Sink.HIVE;
            case "KAFKA": return Sink.KAFKA;
            case "OZONE": return Sink.OZONE;
            case "OZONE-PARQUET": return Sink.OZONE_PARQUET;
            case "OZONE-CSV": return Sink.OZONE_CSV;
            case "OZONE-JSON": return Sink.OZONE_JSON;
            case "OZONE-ORC": return Sink.OZONE_ORC;
            case "OZONE-AVRO": return Sink.OZONE_AVRO;
            case "SOLR": return Sink.SOLR;
            case "KUDU": return Sink.KUDU;
            case "CSV": return Sink.CSV;
            case "JSON": return Sink.JSON;
            case "AVRO": return Sink.AVRO;
            case "PARQUET": return Sink.PARQUET;
            case "ORC": return Sink.ORC;
            default: return null;
        }
    }

    public enum Sink {
        HDFS_CSV,
        HDFS_JSON,
        HDFS_PARQUET,
        HDFS_ORC,
        HDFS_AVRO,
        HBASE,
        HIVE,
        KAFKA,
        OZONE,
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
        ORC;

        // TODO: Add Kudu first
        public static Comparator<Sink> sinkInitPrecedence = new Comparator<>() {
            @Override
            public int compare(Sink s1, Sink s2) {
                if(s1.equals(SinkParser.Sink.OZONE_AVRO) ||
                    s1.equals(SinkParser.Sink.OZONE_JSON) ||
                    s1.equals(SinkParser.Sink.OZONE_CSV) ||
                    s1.equals(SinkParser.Sink.OZONE_PARQUET) ||
                    s1.equals(SinkParser.Sink.OZONE_ORC)) {
                    if(s2.equals(SinkParser.Sink.OZONE_AVRO) ||
                        s2.equals(SinkParser.Sink.OZONE_JSON) ||
                        s2.equals(SinkParser.Sink.OZONE_CSV) ||
                        s2.equals(SinkParser.Sink.OZONE_PARQUET) ||
                        s2.equals(SinkParser.Sink.OZONE_ORC)) {
                        return 0;
                    } else {
                        return -1;
                    }
                } else if(s1.equals(SinkParser.Sink.HIVE) || s1.equals(Sink.KUDU)) {
                    if(s2.equals(SinkParser.Sink.OZONE_AVRO) ||
                        s2.equals(SinkParser.Sink.OZONE_JSON) ||
                        s2.equals(SinkParser.Sink.OZONE_CSV) ||
                        s2.equals(SinkParser.Sink.OZONE_PARQUET) ||
                        s2.equals(SinkParser.Sink.OZONE_ORC)) {
                        return 1;
                    } else if(s2.equals(SinkParser.Sink.HIVE) || s2.equals(Sink.KUDU)) {
                        return 0;
                    } else {
                        return -1;
                    }
                } else {
                    return 0;
                }
            }
        } ;
    }
}
