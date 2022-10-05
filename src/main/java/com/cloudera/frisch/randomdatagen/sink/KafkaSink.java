package com.cloudera.frisch.randomdatagen.sink;

import com.cloudera.frisch.randomdatagen.Utils;
import com.cloudera.frisch.randomdatagen.config.ApplicationConfigs;
import com.cloudera.frisch.randomdatagen.config.PropertiesLoader;
import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.OptionsConverter;
import com.cloudera.frisch.randomdatagen.model.Row;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import static com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotSerializer.SERDES_PROTOCOL_VERSION;
import static com.hortonworks.registries.schemaregistry.serdes.avro.SerDesProtocolHandlerRegistry.METADATA_ID_VERSION_PROTOCOL;

/**
 * This is a Kafka Sink
 */
@Slf4j
public class KafkaSink implements SinkInterface {

    private Producer<String, GenericRecord> producer;
    private Producer<String, String> producerString;
    private final String topic;
    private final Schema schema;
    private final MessageType messagetype;

    KafkaSink(Model model, Map<ApplicationConfigs, String> properties) {
        this.topic =  (String) model.getTableNames().get(OptionsConverter.TableNames.KAFKA_TOPIC);
        this.schema = model.getAvroSchema();
        this.messagetype = convertStringToMessageType((String) model.getOptionsOrDefault(OptionsConverter.Options.KAFKA_MESSAGE_TYPE));

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.get(ApplicationConfigs.KAFKA_BROKERS));
        props.put(ProducerConfig.ACKS_CONFIG, model.getOptionsOrDefault(
            OptionsConverter.Options.KAFKA_ACKS_CONFIG));
        props.put(ProducerConfig.RETRIES_CONFIG, model.getOptionsOrDefault(
            OptionsConverter.Options.KAFKA_RETRIES_CONFIG));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        if(messagetype==MessageType.AVRO) {
            String schemaRegistryProtocol = "http";
          // SSL configs
            if (Boolean.parseBoolean(properties.get(ApplicationConfigs.SCHEMA_REGISTRY_TLS_ENABLED))) {
                System.setProperty("javax.net.ssl.trustStore",
                    properties.get(ApplicationConfigs.KAFKA_TRUSTSTORE_LOCATION));
                System.setProperty("javax.net.ssl.trustStorePassword",
                    properties.get(ApplicationConfigs.KAFKA_TRUSTSTORE_PASSWORD));
                System.setProperty("javax.net.ssl.keyStore",
                    properties.get(ApplicationConfigs.KAFKA_KEYSTORE_LOCATION));
                System.setProperty("javax.net.ssl.keyStorePassword",
                    properties.get(ApplicationConfigs.KAFKA_KEYSTORE_PASSWORD));
                schemaRegistryProtocol = "https";
            }

            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerializer");
            props.put(
                SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(),
                schemaRegistryProtocol + "://" + properties.get(ApplicationConfigs.SCHEMA_REGISTRY_URL) + "/api/v1");
            props.put(SERDES_PROTOCOL_VERSION, METADATA_ID_VERSION_PROTOCOL);

        } else {
          props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
              "org.apache.kafka.common.serialization.StringSerializer");
        }

        String securityProtocol = properties.get(ApplicationConfigs.KAFKA_SECURITY_PROTOCOL);
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);

        //Kerberos config
        if (securityProtocol.equalsIgnoreCase("SASL_PLAINTEXT") || securityProtocol.equalsIgnoreCase("SASL_SSL")) {
            String jaasFilePath = (String) model.getOptionsOrDefault(OptionsConverter.Options.KAFKA_JAAS_FILE_PATH);
            Utils.createJaasConfigFile(jaasFilePath, "KafkaClient",
                properties.get(ApplicationConfigs.KAFKA_AUTH_KERBEROS_KEYTAB), properties.get(ApplicationConfigs.KAFKA_AUTH_KERBEROS_USER),
                    true, true, false);
            Utils.createJaasConfigFile(jaasFilePath, "RegistryClient",
                properties.get(ApplicationConfigs.KAFKA_AUTH_KERBEROS_KEYTAB), properties.get(ApplicationConfigs.KAFKA_AUTH_KERBEROS_USER),
                    true, true, true);
            System.setProperty("java.security.auth.login.config", jaasFilePath);

            props.put(SaslConfigs.SASL_MECHANISM, properties.get(ApplicationConfigs.KAFKA_SASL_MECHANISM));
            props.put(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, properties.get(ApplicationConfigs.KAFKA_SASL_KERBEROS_SERVICE_NAME));
            props.put(SaslConfigs.SASL_JAAS_CONFIG, "com.sun.security.auth.module.Krb5LoginModule required " +
                "useKeyTab=true " +
                "storeKey=true " +
                "keyTab=\""+ properties.get(ApplicationConfigs.KAFKA_AUTH_KERBEROS_KEYTAB) + "\" " +
                "principal=\"" + properties.get(ApplicationConfigs.KAFKA_AUTH_KERBEROS_USER) + "\";");

            Utils.loginUserWithKerberos(properties.get(ApplicationConfigs.KAFKA_AUTH_KERBEROS_USER),
                properties.get(ApplicationConfigs.KAFKA_AUTH_KERBEROS_KEYTAB), new Configuration());
        }

        // SSL configs
        if (securityProtocol.equalsIgnoreCase("SASL_SSL") || securityProtocol.equalsIgnoreCase("SSL")) {
            props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, properties.get(ApplicationConfigs.KAFKA_KEYSTORE_LOCATION));
            props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, properties.get(ApplicationConfigs.KAFKA_TRUSTSTORE_LOCATION));
            props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, properties.get(ApplicationConfigs.KAFKA_KEYSTORE_PASSWORD));
            props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, properties.get(ApplicationConfigs.KAFKA_KEYSTORE_KEYPASSWORD));
            props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, properties.get(ApplicationConfigs.KAFKA_TRUSTSTORE_PASSWORD));
        }

        if ((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.DELETE_PREVIOUS)) {
            KafkaAdminClient.create(props).deleteTopics(List.of(topic));
        }

        if(messagetype==MessageType.AVRO) {
            this.producer = new KafkaProducer<>(props);
        } else {
            this.producerString = new KafkaProducer<>(props);
        }
    }

    @Override
    public void terminate() {
        if(messagetype==MessageType.AVRO) {
            producer.close();
        } else {
            producerString.close();
        }
    }

    @Override
    public void sendOneBatchOfRows(List<Row> rows) {
        ConcurrentLinkedQueue<Future<RecordMetadata>> queue = new ConcurrentLinkedQueue<>();
        if(messagetype==MessageType.AVRO) {
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

    /**
     * Goal is to not overflow kafka brokers and verify they well received data sent before going further
     *
     * @param queue
     * @return
     */
    private void checkMessagesHaveBeenSent(ConcurrentLinkedQueue<Future<RecordMetadata>> queue) {
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
            case "AVRO": return MessageType.AVRO;
            case "CSV": return MessageType.CSV;
            case "JSON":
            default: return MessageType.JSON;
        }
    }
}
