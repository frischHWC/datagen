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
package com.datagen.connector.queues;

import com.datagen.config.ApplicationConfigs;
import com.datagen.connector.ConnectorInterface;
import com.datagen.model.Model;
import com.datagen.model.OptionsConverter;
import com.datagen.model.Row;
import com.datagen.model.type.Field;
import com.datagen.utils.KerberosUtils;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;

import static com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotSerializer.SERDES_PROTOCOL_VERSION;
import static com.hortonworks.registries.schemaregistry.serdes.avro.SerDesProtocolHandlerRegistry.METADATA_ID_VERSION_PROTOCOL;

/**
 * This is a Kafka connector
 */
@Slf4j
public class KafkaConnector implements ConnectorInterface {

  private Producer<String, GenericRecord> producer;
  private Producer<String, String> producerString;
  private AdminClient kafkaAdminClient;
  private Properties props;
  private final String user;
  private final String keytab;
  private final String topic;
  private final int partitions;
  private final short replicationFactor;
  private final Schema schema;
  private final MessageType messagetype;
  private Boolean useKerberos;

  public KafkaConnector(Model model,
                        Map<ApplicationConfigs, String> properties) {
    this.topic = (String) model.getTableNames()
        .get(OptionsConverter.TableNames.KAFKA_TOPIC);
    this.partitions = (int) model.getOptionsOrDefault(
        OptionsConverter.Options.KAFKA_PARTITIONS_NUMBER);
    //TODO: Maybe check if it is a short and otherwise, convert it to a short???
    this.replicationFactor = (short) model.getOptionsOrDefault(
        OptionsConverter.Options.KAFKA_REPLICATION_FACTOR);
    this.schema = model.getAvroSchema();
    this.messagetype = convertStringToMessageType(
        (String) model.getOptionsOrDefault(
            OptionsConverter.Options.KAFKA_MESSAGE_TYPE));
    this.useKerberos = Boolean.FALSE;

    this.user = model.getTableNames().get(OptionsConverter.TableNames.KAFKA_USER)==null ?
        properties.get(ApplicationConfigs.KAFKA_AUTH_KERBEROS_USER) :
        model.getTableNames().get(OptionsConverter.TableNames.KAFKA_USER).toString();
    this.keytab = model.getTableNames().get(OptionsConverter.TableNames.KAFKA_KEYTAB)==null ?
        properties.get(ApplicationConfigs.KAFKA_AUTH_KERBEROS_KEYTAB) :
        model.getTableNames().get(OptionsConverter.TableNames.KAFKA_KEYTAB).toString();


    props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        properties.get(ApplicationConfigs.KAFKA_BROKERS));
    props.put(ProducerConfig.ACKS_CONFIG, model.getOptionsOrDefault(
        OptionsConverter.Options.KAFKA_ACKS_CONFIG));
    props.put(ProducerConfig.RETRIES_CONFIG, model.getOptionsOrDefault(
        OptionsConverter.Options.KAFKA_RETRIES_CONFIG));
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");

    if (messagetype == MessageType.AVRO) {
      String schemaRegistryProtocol = "http";
      // SSL configs
      if (Boolean.parseBoolean(properties.get(
          ApplicationConfigs.SCHEMA_REGISTRY_TLS_ENABLED))) {
        System.setProperty("javax.net.ssl.trustStore",
            model.getTableNames().get(OptionsConverter.TableNames.KAFKA_TRUSTSTORE_LOCATION)==null ?
                properties.get(ApplicationConfigs.KAFKA_TRUSTSTORE_LOCATION):
                model.getTableNames().get(OptionsConverter.TableNames.KAFKA_TRUSTSTORE_LOCATION).toString()
            );
        System.setProperty("javax.net.ssl.trustStorePassword",
            model.getTableNames().get(OptionsConverter.TableNames.KAFKA_TRUSTSTORE_PASSWORD)==null ?
                properties.get(ApplicationConfigs.KAFKA_TRUSTSTORE_PASSWORD):
                model.getTableNames().get(OptionsConverter.TableNames.KAFKA_TRUSTSTORE_PASSWORD).toString()
            );
        System.setProperty("javax.net.ssl.keyStore",
            model.getTableNames().get(OptionsConverter.TableNames.KAFKA_KEYSTORE_LOCATION)==null ?
                properties.get(ApplicationConfigs.KAFKA_KEYSTORE_LOCATION):
                model.getTableNames().get(OptionsConverter.TableNames.KAFKA_KEYSTORE_LOCATION).toString()
            );
        System.setProperty("javax.net.ssl.keyStorePassword",
            model.getTableNames().get(OptionsConverter.TableNames.KAFKA_KEYSTORE_PASSWORD)==null ?
                properties.get(ApplicationConfigs.KAFKA_KEYSTORE_PASSWORD):
                model.getTableNames().get(OptionsConverter.TableNames.KAFKA_KEYSTORE_PASSWORD).toString()
            );
        schemaRegistryProtocol = "https";
      }

      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
          "com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerializer");
      props.put(
          SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(),
          schemaRegistryProtocol + "://" +
              properties.get(ApplicationConfigs.SCHEMA_REGISTRY_URL) +
              "/api/v1");
      props.put(SERDES_PROTOCOL_VERSION, METADATA_ID_VERSION_PROTOCOL);

    } else {
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringSerializer");
    }

    String securityProtocol =
        properties.get(ApplicationConfigs.KAFKA_SECURITY_PROTOCOL);
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
        securityProtocol);

    //Kerberos config
    if (securityProtocol.equalsIgnoreCase("SASL_PLAINTEXT") ||
        securityProtocol.equalsIgnoreCase("SASL_SSL")) {
      this.useKerberos = Boolean.TRUE;
      String jaasFilePath = (String) model.getOptionsOrDefault(
          OptionsConverter.Options.KAFKA_JAAS_FILE_PATH);
      KerberosUtils.createJaasConfigFile(jaasFilePath, "KafkaClient",
          this.keytab,
          this.user,
          true, true, false);
      KerberosUtils.createJaasConfigFile(jaasFilePath, "RegistryClient",
          this.keytab,
          this.user,
          true, true, true);
      System.setProperty("java.security.auth.login.config", jaasFilePath);

      props.put(SaslConfigs.SASL_MECHANISM,
          properties.get(ApplicationConfigs.KAFKA_SASL_MECHANISM));
      props.put(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, properties.get(
          ApplicationConfigs.KAFKA_SASL_KERBEROS_SERVICE_NAME));
      props.put(SaslConfigs.SASL_JAAS_CONFIG,
          "com.sun.security.auth.module.Krb5LoginModule required " +
              "useKeyTab=true " +
              "storeKey=true " +
              "keyTab=\"" + this.keytab + "\" " +
              "principal=\"" + this.user + "\";");

      KerberosUtils.loginUserWithKerberos(
          this.user,
          this.keytab,
          new Configuration());
    }

    // SSL configs
    if (securityProtocol.equalsIgnoreCase("SASL_SSL") ||
        securityProtocol.equalsIgnoreCase("SSL")) {
      props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
          model.getTableNames().get(OptionsConverter.TableNames.KAFKA_KEYSTORE_LOCATION)==null ?
              properties.get(ApplicationConfigs.KAFKA_KEYSTORE_LOCATION):
              model.getTableNames().get(OptionsConverter.TableNames.KAFKA_KEYSTORE_LOCATION).toString()
          );
      props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
          model.getTableNames().get(OptionsConverter.TableNames.KAFKA_TRUSTSTORE_LOCATION)==null ?
              properties.get(ApplicationConfigs.KAFKA_TRUSTSTORE_LOCATION):
              model.getTableNames().get(OptionsConverter.TableNames.KAFKA_TRUSTSTORE_LOCATION).toString()
          );
      props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG,
          model.getTableNames().get(OptionsConverter.TableNames.KAFKA_KEYSTORE_PASSWORD)==null ?
              properties.get(ApplicationConfigs.KAFKA_KEYSTORE_PASSWORD):
              model.getTableNames().get(OptionsConverter.TableNames.KAFKA_KEYSTORE_PASSWORD).toString()
      );
      props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
          model.getTableNames().get(OptionsConverter.TableNames.KAFKA_KEYSTORE_KEY_PASSWORD)==null ?
              properties.get(ApplicationConfigs.KAFKA_KEYSTORE_KEY_PASSWORD):
              model.getTableNames().get(OptionsConverter.TableNames.KAFKA_KEYSTORE_KEY_PASSWORD).toString()
          );
      props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
          model.getTableNames().get(OptionsConverter.TableNames.KAFKA_TRUSTSTORE_PASSWORD)==null ?
              properties.get(ApplicationConfigs.KAFKA_TRUSTSTORE_PASSWORD):
              model.getTableNames().get(OptionsConverter.TableNames.KAFKA_TRUSTSTORE_PASSWORD).toString()
          );
    }
  }

  @Override
  public void init(Model model, boolean writer) {
    if (writer) {
      // Topic Creation
      try {
        this.kafkaAdminClient = KafkaAdminClient.create(props);
        if ((Boolean) model.getOptionsOrDefault(
            OptionsConverter.Options.DELETE_PREVIOUS)) {
          this.kafkaAdminClient.deleteTopics(List.of(topic));
        }
        this.kafkaAdminClient.createTopics(
                Collections.singleton(
                    new NewTopic(this.topic, this.partitions,
                        this.replicationFactor)))
            .values().get(this.topic).get();
      } catch (Exception e) {
        log.warn("Cannot create Kafka topic, due to error: ", e);
      }

      if (messagetype == MessageType.AVRO) {
        this.producer = new KafkaProducer<>(props);
      } else {
        this.producerString = new KafkaProducer<>(props);
      }
    }
  }

  @Override
  public void terminate() {
    try {
      if (messagetype == MessageType.AVRO) {
        producer.close();
      } else {
        producerString.close();
      }
      if (useKerberos) {
        KerberosUtils.logoutUserWithKerberos();
      }
      this.kafkaAdminClient.close();
    } catch (Exception e) {
      log.warn("Could not close Kafka Client");
    }
  }

  @Override
  public void sendOneBatchOfRows(List<Row> rows) {
    ConcurrentLinkedQueue<Future<RecordMetadata>> queue =
        new ConcurrentLinkedQueue<>();
    if (messagetype == MessageType.AVRO) {
      rows.parallelStream()
          .map(row -> row.toKafkaMessage(schema))
          .forEach(keyValue ->
              queue.add(
                  producer.send(
                      new ProducerRecord<>(
                          topic,
                          (String) keyValue.getKey(),
                          (GenericRecord) keyValue.getValue()
                      )
                  ))
          );
    } else {
      rows.parallelStream()
          .map(row -> row.toKafkaMessageString(messagetype))
          .forEach(keyValue ->
              queue.add(
                  producerString.send(
                      new ProducerRecord<>(
                          topic,
                          (String) keyValue.getKey(),
                          (String) keyValue.getValue()
                      )
                  ))
          );
    }
    checkMessagesHaveBeenSent(queue);
  }

  @Override
  public Model generateModel(Boolean deepAnalysis) {
    LinkedHashMap<String, Field> fields = new LinkedHashMap<String, Field>();
    Map<String, List<String>> primaryKeys = new HashMap<>();
    Map<String, String> tableNames = new HashMap<>();
    Map<String, String> options = new HashMap<>();
    // TODO : Implement logic to create a model with at least names, pk, options and column names/types
    return new Model("",fields, primaryKeys, tableNames, options, null);
  }

  /**
   * Goal is to not overflow kafka brokers and verify they well received data sent before going further
   *
   * @param queue
   * @return
   */
  private void checkMessagesHaveBeenSent(
      ConcurrentLinkedQueue<Future<RecordMetadata>> queue) {
    while (!queue.isEmpty()) {
      Future<RecordMetadata> metadata = queue.poll();
      if (!metadata.isDone()) {
        queue.add(metadata);
      }
    }
  }

  public enum MessageType {
    AVRO,
    CSV,
    JSON
  }

  private MessageType convertStringToMessageType(String messageType) {
    switch (messageType.toUpperCase()) {
    case "AVRO":
      return MessageType.AVRO;
    case "CSV":
      return MessageType.CSV;
    case "JSON":
    default:
      return MessageType.JSON;
    }
  }
}
