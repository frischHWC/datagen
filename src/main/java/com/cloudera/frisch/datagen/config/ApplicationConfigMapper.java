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


import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Locale;


@Slf4j
@Component
public class ApplicationConfigMapper {

  public static ApplicationConfigs getApplicationConfigFromProperty(
      String propertyName) {
    switch (propertyName.toLowerCase(Locale.ROOT)) {
    case "app.name":
      return ApplicationConfigs.APP_NAME;
    case "app.port":
      return ApplicationConfigs.APP_PORT;
    case "hadoop.user":
      return ApplicationConfigs.HADOOP_USER;
    case "hadoop.home":
      return ApplicationConfigs.HADOOP_HOME;
    case "threads":
      return ApplicationConfigs.THREADS;
    case "number.batches.default":
      return ApplicationConfigs.NUMBER_OF_BATCHES_DEFAULT;
    case "number.rows.default":
      return ApplicationConfigs.NUMBER_OF_ROWS_DEFAULT;
    case "datagen.home.directory":
      return ApplicationConfigs.DATA_HOME_DIRECTORY;
    case "datagen.model.path":
      return ApplicationConfigs.DATA_MODEL_PATH_DEFAULT;
    case "datagen.model.received.path":
      return ApplicationConfigs.DATA_MODEL_RECEIVED_PATH;
    case "datagen.model.generated.path":
      return ApplicationConfigs.DATA_MODEL_GENERATED_PATH;
    case "datagen.model.default":
      return ApplicationConfigs.DATA_MODEL_DEFAULT;
    case "datagen.custom.model":
      return ApplicationConfigs.CUSTOM_DATA_MODEL_DEFAULT;
    case "datagen.scheduler.file.path":
      return ApplicationConfigs.SCHEDULER_FILE_PATH;
    case "kerberos.enabled":
      return ApplicationConfigs.KERBEROS_ENABLED;
    case "kerberos.user":
      return ApplicationConfigs.KERBEROS_USER;
    case "kerberos.keytab":
      return ApplicationConfigs.KERBEROS_KEYTAB;
    case "tls.enabled":
      return ApplicationConfigs.TLS_ENABLED;
    case "truststore.location":
      return ApplicationConfigs.TRUSTSTORE_LOCATION;
    case "truststore.password":
      return ApplicationConfigs.TRUSTSTORE_PASSWORD;
    case "keystore.location":
      return ApplicationConfigs.KEYSTORE_LOCATION;
    case "keystore.password":
      return ApplicationConfigs.KEYSTORE_PASSWORD;
    case "keystore.keypassword":
      return ApplicationConfigs.KEYSTORE_KEYPASSWORD;
    case "admin.user":
      return ApplicationConfigs.ADMIN_USER;
    case "admin.password":
      return ApplicationConfigs.ADMIN_PASSWORD;
    case "hadoop.core.site.path":
      return ApplicationConfigs.HADOOP_CORE_SITE_PATH;
    case "hadoop.hdfs.site.path":
      return ApplicationConfigs.HADOOP_HDFS_SITE_PATH;
    case "hadoop.ozone.site.path":
      return ApplicationConfigs.HADOOP_OZONE_SITE_PATH;
    case "hadoop.hbase.site.path":
      return ApplicationConfigs.HADOOP_HBASE_SITE_PATH;
    case "hadoop.hive.site.path":
      return ApplicationConfigs.HADOOP_HIVE_SITE_PATH;
    case "cm.autodiscovery":
      return ApplicationConfigs.CM_AUTO_DISCOVERY;
    case "cm.url":
      return ApplicationConfigs.CM_URL;
    case "cm.user":
      return ApplicationConfigs.CM_USER;
    case "cm.password":
      return ApplicationConfigs.CM_PASSWORD;
    case "cm.cluster.name":
      return ApplicationConfigs.CM_CLUSTER_NAME;
    case "solr.env.path":
      return ApplicationConfigs.SOLR_ENV_PATH;
    case "kafka.conf.client.path":
      return ApplicationConfigs.KAFKA_CONF_CLIENT_PATH;
    case "kafka.conf.cluster.path":
      return ApplicationConfigs.KAFKA_CONF_CLUSTER_PATH;
    case "schema.registry.conf.path":
      return ApplicationConfigs.SCHEMA_REGISTRY_CONF_PATH;
    case "kudu.conf.path":
      return ApplicationConfigs.KUDU_CONF_PATH;
    case "hdfs.uri":
      return ApplicationConfigs.HDFS_URI;
    case "hdfs.auth.kerberos":
      return ApplicationConfigs.HDFS_AUTH_KERBEROS;
    case "hdfs.auth.kerberos.user":
      return ApplicationConfigs.HDFS_AUTH_KERBEROS_USER;
    case "hdfs.auth.kerberos.keytab":
      return ApplicationConfigs.HDFS_AUTH_KERBEROS_KEYTAB;
    case "hbase.zookeeper.quorum":
      return ApplicationConfigs.HBASE_ZK_QUORUM;
    case "hbase.zookeeper.port":
      return ApplicationConfigs.HBASE_ZK_QUORUM_PORT;
    case "hbase.zookeeper.znode":
      return ApplicationConfigs.HBASE_ZK_ZNODE;
    case "hbase.auth.kerberos":
      return ApplicationConfigs.HBASE_AUTH_KERBEROS;
    case "hbase.security.user":
      return ApplicationConfigs.HBASE_AUTH_KERBEROS_USER;
    case "hbase.security.keytab":
      return ApplicationConfigs.HBASE_AUTH_KERBEROS_KEYTAB;
    case "ozone.service.id":
      return ApplicationConfigs.OZONE_SERVICE_ID;
    case "ozone.auth.kerberos":
      return ApplicationConfigs.OZONE_AUTH_KERBEROS;
    case "ozone.auth.kerberos.user":
      return ApplicationConfigs.OZONE_AUTH_KERBEROS_USER;
    case "ozone.auth.kerberos.keytab":
      return ApplicationConfigs.OZONE_AUTH_KERBEROS_KEYTAB;
    case "hive.zookeeper.quorum":
      return ApplicationConfigs.HIVE_ZK_QUORUM;
    case "hive.zookeeper.znode":
      return ApplicationConfigs.HIVE_ZK_ZNODE;
    case "hive.auth.kerberos":
      return ApplicationConfigs.HIVE_AUTH_KERBEROS;
    case "hive.security.user":
      return ApplicationConfigs.HIVE_AUTH_KERBEROS_USER;
    case "hive.security.keytab":
      return ApplicationConfigs.HIVE_AUTH_KERBEROS_KEYTAB;
    case "hive.truststore.location":
      return ApplicationConfigs.HIVE_TRUSTSTORE_LOCATION;
    case "hive.truststore.password":
      return ApplicationConfigs.HIVE_TRUSTSTORE_PASSWORD;
    case "solr.zookeeper.quorum":
      return ApplicationConfigs.SOLR_ZK_QUORUM;
    case "solr.zookeeper.znode":
      return ApplicationConfigs.SOLR_ZK_NODE;
    case "solr.tls.enabled":
      return ApplicationConfigs.SOLR_TLS_ENABLED;
    case "solr.auth.kerberos":
      return ApplicationConfigs.SOLR_AUTH_KERBEROS;
    case "solr.auth.kerberos.keytab":
      return ApplicationConfigs.SOLR_AUTH_KERBEROS_KEYTAB;
    case "solr.auth.kerberos.user":
      return ApplicationConfigs.SOLR_AUTH_KERBEROS_USER;
    case "solr.truststore.location":
      return ApplicationConfigs.SOLR_TRUSTSTORE_LOCATION;
    case "solr.truststore.password":
      return ApplicationConfigs.SOLR_TRUSTSTORE_PASSWORD;
    case "solr.keystore.location":
      return ApplicationConfigs.SOLR_KEYSTORE_LOCATION;
    case "solr.keystore.password":
      return ApplicationConfigs.SOLR_KEYSTORE_PASSWORD;
    case "kafka.brokers":
      return ApplicationConfigs.KAFKA_BROKERS;
    case "kafka.security.protocol":
      return ApplicationConfigs.KAFKA_SECURITY_PROTOCOL;
    case "schema.registry.url":
      return ApplicationConfigs.SCHEMA_REGISTRY_URL;
    case "schema.registry.tls.enabled":
      return ApplicationConfigs.SCHEMA_REGISTRY_TLS_ENABLED;
    case "kafka.keystore.location":
      return ApplicationConfigs.KAFKA_KEYSTORE_LOCATION;
    case "kafka.truststore.location":
      return ApplicationConfigs.KAFKA_TRUSTSTORE_LOCATION;
    case "kafka.keystore.password":
      return ApplicationConfigs.KAFKA_KEYSTORE_PASSWORD;
    case "kafka.keystore.key.password":
      return ApplicationConfigs.KAFKA_KEYSTORE_KEYPASSWORD;
    case "kafka.truststore.password":
      return ApplicationConfigs.KAFKA_TRUSTSTORE_PASSWORD;
    case "kafka.sasl.mechanism":
      return ApplicationConfigs.KAFKA_SASL_MECHANISM;
    case "kafka.sasl.kerberos.service.name":
      return ApplicationConfigs.KAFKA_SASL_KERBEROS_SERVICE_NAME;
    case "kafka.auth.kerberos.keytab":
      return ApplicationConfigs.KAFKA_AUTH_KERBEROS_KEYTAB;
    case "kafka.auth.kerberos.user":
      return ApplicationConfigs.KAFKA_AUTH_KERBEROS_USER;
    case "kudu.master.server":
      return ApplicationConfigs.KUDU_URL;
    case "kudu.auth.kerberos":
      return ApplicationConfigs.KUDU_AUTH_KERBEROS;
    case "kudu.security.user":
      return ApplicationConfigs.KUDU_AUTH_KERBEROS_USER;
    case "kudu.security.keytab":
      return ApplicationConfigs.KUDU_AUTH_KERBEROS_KEYTAB;
    case "kudu.truststore.location":
      return ApplicationConfigs.KUDU_TRUSTSTORE_LOCATION;
    case "kudu.truststore.password":
      return ApplicationConfigs.KUDU_TRUSTSTORE_PASSWORD;
    case "s3.access_key.id":
      return ApplicationConfigs.S3_ACCESS_KEY_ID;
    case "s3.access_key.secret":
      return ApplicationConfigs.S3_ACCESS_KEY_SECRET;
    case "s3.region":
      return ApplicationConfigs.S3_REGION;
    case "adls.account.name":
      return ApplicationConfigs.ADLS_ACCOUNT_NAME;
    case "adls.account.type":
      return ApplicationConfigs.ADLS_ACCOUNT_TYPE;
    case "adls.sas.token":
      return ApplicationConfigs.ADLS_SAS_TOKEN;
    case "gcs.project.id":
      return ApplicationConfigs.GCS_PROJECT_ID;
    case "gcs.accountkey.path":
      return ApplicationConfigs.GCS_ACCOUNT_KEY_PATH;
    case "gcs.region":
      return ApplicationConfigs.GCS_REGION;

    default:
      log.warn("Could not guess property: {} , check it is well written",
          propertyName);
      return null;

    }
  }
}
