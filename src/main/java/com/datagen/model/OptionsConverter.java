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
package com.datagen.model;


import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OptionsConverter {

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
    S3_BUCKET,
    S3_DIRECTORY,
    S3_KEY_NAME,
    S3_LOCAL_FILE_PATH,
    ADLS_CONTAINER,
    ADLS_DIRECTORY,
    ADLS_FILE_NAME,
    ADLS_LOCAL_FILE_PATH,
    GCS_BUCKET,
    GCS_DIRECTORY,
    GCS_OBJECT_NAME,
    GCS_LOCAL_FILE_PATH,
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
    case "S3_BUCKET":
      return TableNames.S3_BUCKET;
    case "S3_DIRECTORY":
      return TableNames.S3_DIRECTORY;
    case "S3_KEY_NAME":
      return TableNames.S3_KEY_NAME;
    case "S3_LOCAL_FILE_PATH":
      return TableNames.S3_LOCAL_FILE_PATH;
    case "ADLS_CONTAINER":
      return TableNames.ADLS_CONTAINER;
    case "ADLS_DIRECTORY":
      return TableNames.ADLS_DIRECTORY;
    case "ADLS_FILE_NAME":
      return TableNames.ADLS_FILE_NAME;
    case "ADLS_LOCAL_FILE_PATH":
      return TableNames.ADLS_LOCAL_FILE_PATH;
    case "GCS_BUCKET":
      return TableNames.GCS_BUCKET;
    case "GCS_DIRECTORY":
      return TableNames.GCS_DIRECTORY;
    case "GCS_OBJECT_NAME":
      return TableNames.GCS_OBJECT_NAME;
    case "GCS_LOCAL_FILE_PATH":
      return TableNames.GCS_LOCAL_FILE_PATH;
    case "AVRO_NAME":
      return TableNames.AVRO_NAME;
    default:
      log.warn("Option was not recognized: " + option +
          " , please verify your JSON");
      return null;
    }
  }

  public enum Options {
    KAFKA_MSG_KEY,
    HBASE_PRIMARY_KEY,
    KUDU_HASH_KEYS,
    KUDU_RANGE_KEYS,
    KUDU_PRIMARY_KEYS,
    HBASE_COLUMN_FAMILIES_MAPPING,
    SOLR_SHARDS,
    SOLR_REPLICAS,
    SOLR_JAAS_FILE_PATH,
    KUDU_REPLICAS,
    ONE_FILE_PER_ITERATION,
    HIVE_THREAD_NUMBER,
    HIVE_TABLE_TYPE,
    HIVE_TABLE_FORMAT,
    HIVE_ON_HDFS,
    HIVE_TEZ_QUEUE_NAME,
    HIVE_TABLE_PARTITIONS_COLS,
    HIVE_TABLE_BUCKETS_COLS,
    HIVE_TABLE_BUCKETS_NUMBER,
    CSV_HEADER,
    DELETE_PREVIOUS,
    PARQUET_PAGE_SIZE,
    PARQUET_ROW_GROUP_SIZE,
    PARQUET_DICTIONARY_PAGE_SIZE,
    PARQUET_DICTIONARY_ENCODING,
    KAFKA_ACKS_CONFIG,
    KAFKA_RETRIES_CONFIG,
    KAFKA_JAAS_FILE_PATH,
    KAFKA_REPLICATION_FACTOR,
    KAFKA_PARTITIONS_NUMBER,
    KAFKA_MESSAGE_TYPE,
    KUDU_BUCKETS,
    KUDU_BUFFER,
    KUDU_FLUSH,
    OZONE_REPLICATION_FACTOR,
    HDFS_REPLICATION_FACTOR,
    ADLS_BLOCK_SIZE,
    ADLS_MAX_UPLOAD_SIZE,
    ADLS_MAX_CONCURRENCY
  }

  static Options convertOptionToOption(String option) {
    switch (option.toUpperCase()) {
    case "KAFKA_MSG_KEY":
      return Options.KAFKA_MSG_KEY;
    case "HBASE_PRIMARY_KEY":
      return Options.HBASE_PRIMARY_KEY;
    case "KUDU_PRIMARY_KEYS":
      return Options.KUDU_PRIMARY_KEYS;
    case "KUDU_HASH_KEYS":
      return Options.KUDU_HASH_KEYS;
    case "KUDU_RANGE_KEYS":
      return Options.KUDU_RANGE_KEYS;
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
    case "KAFKA_JAAS_FILE_PATH":
      return Options.KAFKA_JAAS_FILE_PATH;
    case "SOLR_JAAS_FILE_PATH":
      return Options.SOLR_JAAS_FILE_PATH;
    case "HIVE_THREAD_NUMBER":
      return Options.HIVE_THREAD_NUMBER;
    case "HIVE_TABLE_TYPE":
      return Options.HIVE_TABLE_TYPE;
    case "HIVE_TABLE_FORMAT":
      return Options.HIVE_TABLE_FORMAT;
    case "HIVE_ON_HDFS":
      return Options.HIVE_ON_HDFS;
    case "HIVE_TEZ_QUEUE_NAME":
      return Options.HIVE_TEZ_QUEUE_NAME;
    case "HIVE_TABLE_PARTITIONS_COLS":
      return Options.HIVE_TABLE_PARTITIONS_COLS;
    case "HIVE_TABLE_BUCKETS_COLS":
      return Options.HIVE_TABLE_BUCKETS_COLS;
    case "HIVE_TABLE_BUCKETS_NUMBER":
      return Options.HIVE_TABLE_BUCKETS_NUMBER;
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
    case "KAFKA_REPLICATION_FACTOR":
      return Options.KAFKA_REPLICATION_FACTOR;
    case "KAFKA_PARTITIONS_NUMBER":
      return Options.KAFKA_PARTITIONS_NUMBER;
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
    case "ADLS_BLOCK_SIZE":
      return Options.ADLS_BLOCK_SIZE;
    case "ADLS_MAX_UPLOAD_SIZE":
      return Options.ADLS_MAX_UPLOAD_SIZE;
    case "ADLS_MAX_CONCURRENCY":
      return Options.ADLS_MAX_CONCURRENCY;
    default:
      log.warn("Option was not recognized: " + option +
          " , please verify your JSON");
      return null;
    }
  }
}