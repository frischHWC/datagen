package com.cloudera.frisch.randomdatagen.config;


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
        ORC
    }
}
