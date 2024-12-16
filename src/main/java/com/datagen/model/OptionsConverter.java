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
    LOCAL_FILE_PATH,
    LOCAL_FILE_NAME,

    HDFS_FILE_PATH,
    HDFS_FILE_NAME,
    HDFS_USE_KERBEROS,
    HDFS_USER,
    HDFS_KEYTAB,

    OZONE_VOLUME,
    OZONE_BUCKET,
    OZONE_KEY_NAME,
    OZONE_LOCAL_FILE_PATH,
    OZONE_USE_KERBEROS,
    OZONE_USER,
    OZONE_KEYTAB,

    S3_BUCKET,
    S3_DIRECTORY,
    S3_KEY_NAME,
    S3_LOCAL_FILE_PATH,
    S3_REGION,
    S3_ACCESS_KEY_ID,
    S3_ACCESS_KEY_SECRET,

    ADLS_CONTAINER,
    ADLS_DIRECTORY,
    ADLS_FILE_NAME,
    ADLS_LOCAL_FILE_PATH,
    ADLS_SAS_TOKEN,
    ADLS_ACCOUNT_NAME,
    ADLS_ACCOUNT_TYPE,

    GCS_BUCKET,
    GCS_DIRECTORY,
    GCS_OBJECT_NAME,
    GCS_LOCAL_FILE_PATH,
    GCS_PROJECT_ID,
    GCS_REGION,
    GCS_ACCOUNT_KEY_PATH,
    GCS_ACCOUNT_ACCESS_TOKEN,

    HIVE_DATABASE,
    HIVE_TABLE_NAME,
    HIVE_TEMPORARY_TABLE_NAME,
    HIVE_HDFS_FILE_PATH,
    HIVE_USE_KERBEROS,
    HIVE_USER,
    HIVE_KEYTAB,

    HBASE_TABLE_NAME,
    HBASE_NAMESPACE,
    HBASE_USE_KERBEROS,
    HBASE_USER,
    HBASE_KEYTAB,

    KAFKA_TOPIC,
    KAFKA_USE_KERBEROS,
    KAFKA_USER,
    KAFKA_KEYTAB,
    KAFKA_KEYSTORE_LOCATION,
    KAFKA_TRUSTSTORE_LOCATION,
    KAFKA_KEYSTORE_PASSWORD,
    KAFKA_KEYSTORE_KEY_PASSWORD,
    KAFKA_TRUSTSTORE_PASSWORD,

    KUDU_TABLE_NAME,
    KUDU_USE_KERBEROS,
    KUDU_USER,
    KUDU_KEYTAB,

    SOLR_COLLECTION,
    SOLR_USE_KERBEROS,
    SOLR_USER,
    SOLR_KEYTAB,
    SOLR_KEYSTORE_LOCATION,
    SOLR_KEYSTORE_PASSWORD,
    SOLR_TRUSTSTORE_LOCATION,
    SOLR_TRUSTSTORE_PASSWORD,

    AVRO_NAME
  }

  static TableNames convertOptionToTableNames(String option) {
    return TableNames.valueOf(option.toUpperCase());
  }

  public enum Options {
    ONE_FILE_PER_ITERATION,
    DELETE_PREVIOUS,

    CSV_HEADER,

    PARQUET_PAGE_SIZE,
    PARQUET_ROW_GROUP_SIZE,
    PARQUET_DICTIONARY_PAGE_SIZE,
    PARQUET_DICTIONARY_ENCODING,

    HDFS_REPLICATION_FACTOR,

    OZONE_REPLICATION_FACTOR,

    ADLS_BLOCK_SIZE,
    ADLS_MAX_UPLOAD_SIZE,
    ADLS_MAX_CONCURRENCY,

    HIVE_THREAD_NUMBER,
    HIVE_TABLE_TYPE,
    HIVE_TABLE_FORMAT,
    HIVE_ON_HDFS,
    HIVE_TEZ_QUEUE_NAME,
    HIVE_TABLE_PARTITIONS_COLS,
    HIVE_TABLE_BUCKETS_COLS,
    HIVE_TABLE_BUCKETS_NUMBER,
    HIVE_TABLE_ICEBERG_V2,

    HBASE_PRIMARY_KEY,
    HBASE_COLUMN_FAMILIES_MAPPING,

    KAFKA_MSG_KEY,
    KAFKA_ACKS_CONFIG,
    KAFKA_RETRIES_CONFIG,
    KAFKA_JAAS_FILE_PATH,
    KAFKA_REPLICATION_FACTOR,
    KAFKA_PARTITIONS_NUMBER,
    KAFKA_MESSAGE_TYPE,

    KUDU_BUCKETS,
    KUDU_BUFFER,
    KUDU_FLUSH,
    KUDU_REPLICAS,
    KUDU_HASH_KEYS,
    KUDU_RANGE_KEYS,
    KUDU_PRIMARY_KEYS,

    SOLR_SHARDS,
    SOLR_REPLICAS,
    SOLR_JAAS_FILE_PATH
  }

  static Options convertOptionToOption(String option) {
    return Options.valueOf(option.toUpperCase());
  }
}