package com.cloudera.frisch.randomdatagen.model;


import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OptionsConverter {

    public enum PrimaryKeys {
        KAFKA_MSG_KEY,
        HBASE_PRIMARY_KEY,
        OZONE_BUCKET,
        OZONE_KEY,
        KUDU_HASH_KEYS,
        KUDU_RANGE_KEYS,
        KUDU_PRIMARY_KEYS
    }

    static PrimaryKeys convertOptionToPrimaryKey(String option) {
        switch (option.toUpperCase()) {
            case "KAFKA_MSG_KEY":
                return PrimaryKeys.KAFKA_MSG_KEY;
            case "HBASE_PRIMARY_KEY":
                return PrimaryKeys.HBASE_PRIMARY_KEY;
            case "OZONE_BUCKET":
                return PrimaryKeys.OZONE_BUCKET;
            case "OZONE_KEY":
                return PrimaryKeys.OZONE_KEY;
            case "KUDU_PRIMARY_KEYS":
                return PrimaryKeys.KUDU_PRIMARY_KEYS;
            case "KUDU_HASH_KEYS":
                return PrimaryKeys.KUDU_HASH_KEYS;
            case "KUDU_RANGE_KEYS":
                return PrimaryKeys.KUDU_RANGE_KEYS;
            default:
                log.warn("Option was not recognized: " + option + " , please verify your JSON");
                return null;
        }
    }

    public enum TableNames {
        HDFS_FILE_PATH,
        HDFS_FILE_NAME,
        HIVE_HDFS_FILE_PATH,
        HBASE_TABLE_NAME,
        HBASE_NAMESPACE,
        KAFKA_TOPIC,
        OZONE_VOLUME,
        OZONE_BUCKET,
        OZONE_KEY_NAME,
        OZONE_LOCAL_FILE_PATH,
        SOLR_COLLECTION,
        HIVE_DATABASE,
        HIVE_TABLE_NAME,
        HIVE_TEMPORARY_TABLE_NAME,
        KUDU_TABLE_NAME,
        LOCAL_FILE_PATH,
        LOCAL_FILE_NAME,
        AVRO_NAME
    }

    static TableNames convertOptionToTableNames(String option) {
        switch (option.toUpperCase()) {
            case "HDFS_FILE_PATH":
                return TableNames.HDFS_FILE_PATH;
            case "HDFS_FILE_NAME":
                return TableNames.HDFS_FILE_NAME;
            case "HIVE_HDFS_FILE_PATH":
                return TableNames.HIVE_HDFS_FILE_PATH;
            case "HBASE_TABLE_NAME":
                return TableNames.HBASE_TABLE_NAME;
            case "HBASE_NAMESPACE":
                return TableNames.HBASE_NAMESPACE;
            case "KAFKA_TOPIC":
                return TableNames.KAFKA_TOPIC;
            case "OZONE_VOLUME":
                return TableNames.OZONE_VOLUME;
            case "OZONE_BUCKET":
                return TableNames.OZONE_BUCKET;
            case "OZONE_KEY_NAME":
                return TableNames.OZONE_KEY_NAME;
            case "OZONE_LOCAL_FILE_PATH":
                return TableNames.OZONE_LOCAL_FILE_PATH;
            case "SOLR_COLLECTION":
                return TableNames.SOLR_COLLECTION;
            case "HIVE_DATABASE":
                return TableNames.HIVE_DATABASE;
            case "HIVE_TABLE_NAME":
                return TableNames.HIVE_TABLE_NAME;
            case "HIVE_TEMPORARY_TABLE_NAME":
                return TableNames.HIVE_TEMPORARY_TABLE_NAME;
            case "KUDU_TABLE_NAME":
                return TableNames.KUDU_TABLE_NAME;
            case "LOCAL_FILE_PATH":
                return TableNames.LOCAL_FILE_PATH;
            case "LOCAL_FILE_NAME":
                return TableNames.LOCAL_FILE_NAME;
            case "AVRO_NAME":
                return TableNames.AVRO_NAME;
            default:
                log.warn("Option was not recognized: " + option + " , please verify your JSON");
                return null;
        }
    }

    public enum Options {
        HBASE_COLUMN_FAMILIES_MAPPING,
        SOLR_SHARDS,
        SOLR_REPLICAS,
        SOLR_JAAS_FILE_PATH,
        KUDU_REPLICAS,
        ONE_FILE_PER_ITERATION,
        KAFKA_MESSAGE_TYPE,
        HIVE_THREAD_NUMBER,
        HIVE_ON_HDFS,
        HIVE_TEZ_QUEUE_NAME,
        CSV_HEADER,
        DELETE_PREVIOUS,
        PARQUET_PAGE_SIZE,
        PARQUET_ROW_GROUP_SIZE,
        PARQUET_DICTIONARY_PAGE_SIZE,
        PARQUET_DICTIONARY_ENCODING,
        KAFKA_ACKS_CONFIG,
        KAFKA_RETRIES_CONFIG,
        KAFKA_JAAS_FILE_PATH,
        KUDU_BUCKETS,
        KUDU_BUFFER,
        KUDU_FLUSH,
        OZONE_REPLICATION_FACTOR,
        HDFS_REPLICATION_FACTOR
    }

    static Options convertOptionToOption(String option) {
        switch (option.toUpperCase()) {
            case "HBASE_COLUMN_FAMILIES_MAPPING":
                return Options.HBASE_COLUMN_FAMILIES_MAPPING;
            case "SOLR_SHARDS":
                return Options.SOLR_SHARDS;
            case "SOLR_REPLICAS":
                return Options.SOLR_REPLICAS;
            case "KUDU_REPLICAS":
                return Options.KUDU_REPLICAS;
            case "ONE_FILE_PER_ITERATION":
                return Options.ONE_FILE_PER_ITERATION;
            case "KAFKA_MESSAGE_TYPE":
                return Options.KAFKA_MESSAGE_TYPE;
            case "HIVE_THREAD_NUMBER":
                return Options.HIVE_THREAD_NUMBER;
            case "HIVE_ON_HDFS":
                return Options.HIVE_ON_HDFS;
            case "HIVE_TEZ_QUEUE_NAME":
                return Options.HIVE_TEZ_QUEUE_NAME;
            case "CSV_HEADER":
                return Options.CSV_HEADER;
            case "DELETE_PREVIOUS":
                return Options.DELETE_PREVIOUS;
            case "PARQUET_PAGE_SIZE":
                return Options.PARQUET_PAGE_SIZE;
            case "PARQUET_ROW_GROUP_SIZE":
                return Options.PARQUET_ROW_GROUP_SIZE;
            case "PARQUET_DICTIONARY_PAGE_SIZE":
                return Options.PARQUET_DICTIONARY_PAGE_SIZE;
            case "PARQUET_DICTIONARY_ENCODING":
                return Options.PARQUET_DICTIONARY_ENCODING;
            case "KAFKA_ACKS_CONFIG":
                return Options.KAFKA_ACKS_CONFIG;
            case "KAFKA_RETRIES_CONFIG":
                return Options.KAFKA_RETRIES_CONFIG;
            case "KUDU_BUCKETS":
                return Options.KUDU_BUCKETS;
            case "KUDU_BUFFER":
                return Options.KUDU_BUFFER;
            case "KUDU_FLUSH":
                return Options.KUDU_FLUSH;
            case "OZONE_REPLICATION_FACTOR":
                return Options.OZONE_REPLICATION_FACTOR;
            case "HDFS_REPLICATION_FACTOR":
                return Options.HDFS_REPLICATION_FACTOR;
            default:
                log.warn("Option was not recognized: " + option + " , please verify your JSON");
                return null;
        }
    }
}