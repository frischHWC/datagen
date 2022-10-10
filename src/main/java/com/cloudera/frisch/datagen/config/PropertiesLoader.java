package com.cloudera.frisch.datagen.config;

import com.cloudera.frisch.datagen.service.CmApiService;
import com.cloudera.frisch.datagen.utils.Utils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;


@Slf4j
@Component
public class PropertiesLoader {

    private SpringConfig springConfig;

    private Map<ApplicationConfigs, String> properties;

    private CmApiService cmApiService;

    @Autowired
    public PropertiesLoader(SpringConfig springConfig,
                            CmApiService cmApiService) {
        this.springConfig = springConfig;
        this.cmApiService = cmApiService;
        properties = new HashMap<>();

        // Load config file
        java.util.Properties propertiesAsProperties =
            new java.util.Properties();

        String pathToApplicationProperties =
            "src/main/resources/application.properties";
        if (springConfig.getActiveProfile()
            .equalsIgnoreCase("cdp")) {
            log.info(
                "Detected to be in cdp profile, so will load service.properties file");
            pathToApplicationProperties = "service.properties";
        } else if (!springConfig.getActiveProfile().equalsIgnoreCase("dev")) {
            log.info("Detected another profile to be loaded, will use it");
            pathToApplicationProperties = "src/main/resources/application-" +
                springConfig.getActiveProfile() + ".properties";
        }

        try {
            log.info("Reading properties from file : {}",
                pathToApplicationProperties);
            FileInputStream fileInputStream =
                new FileInputStream(pathToApplicationProperties);
            propertiesAsProperties.load(fileInputStream);
        } catch (IOException e) {
            log.error("Property file not found !", e);
        }

        propertiesAsProperties.forEach((propertyKey, propertyValue) -> {
            // Keys starting with spring or server are considered as internal spring-boot used and should not be taken
            if (!propertyKey.toString().startsWith("spring.")
                && !propertyKey.toString().startsWith("server.")
                && !propertyKey.toString().startsWith("security.")) {
                ApplicationConfigs propAsAppConfig =
                    ApplicationConfigMapper.getApplicationConfigFromProperty(
                        propertyKey.toString());
                if (propAsAppConfig != null) {
                    String propValue =
                        getPropertyResolvingPlaceholder(propertyKey.toString(),
                            propertiesAsProperties);

                    if (propValue != null && !propValue.isEmpty()) {
                        properties.put(propAsAppConfig, propValue);
                    }
                }
            }
        });

        printAllProperties();

        autoDiscover();

        printAllProperties();
    }

    private String getPropertyResolvingPlaceholder(String key,
                                                   Properties propertiesAsProperties) {
        String property = "null";
        try {
            property = propertiesAsProperties.getProperty(key);
            if (property.length() > 1 &&
                property.substring(0, 2).equalsIgnoreCase("#{")) {
                property = getPropertyResolvingPlaceholder(
                    property.substring(2, property.length() - 1),
                    propertiesAsProperties);
            }
        } catch (Exception e) {
            log.warn("Could not get property : {} due to following error: ",
                key, e);
        }
        log.debug("For key: {} returning property: {}", key, property);
        return property;
    }

    private void printAllProperties() {
        log.debug("Printing all properties:  ");
        properties.entrySet().stream().sorted(
                (entry1, entry2) -> entry1.getKey().toString()
                    .compareTo(entry2.getKey().toString()))
            .collect(Collectors.toList())
            .forEach(entry -> log.debug("{} = {}", entry.getKey().toString(),
                entry.getValue()));
    }


    public Map<ApplicationConfigs, String> getPropertiesCopy() {
        return new HashMap<>(properties);
    }

    /*
    - If properties for a service are not set (ex: hdfs.uri) => Look for config file's location property
    - If this is empty or file does not exists => WARNING: You should provide info on API call to use this sink
    - Otherwise, load the file and set the required property (hdfs.uri for example)
     */
    private void autoDiscover() {
        log.info(
            "Starting auto-discover of properties after load of properties file");

        if (Boolean.parseBoolean(
            properties.get(ApplicationConfigs.CM_AUTO_DISCOVERY))) {
            autoDiscoverWithCMApi(properties.get(ApplicationConfigs.CM_URL),
                properties.get(ApplicationConfigs.CM_USER),
                properties.get(ApplicationConfigs.CM_PASSWORD),
                properties.get(ApplicationConfigs.CM_CLUSTER_NAME));
        }

        if (properties.get(ApplicationConfigs.HDFS_URI) == null
            &&
            properties.get(ApplicationConfigs.HADOOP_CORE_SITE_PATH) != null) {
            log.info("Going to auto-discover hdfs.uri");

            String hdfsUri = Utils.getPropertyFromXMLFile(
                properties.get(ApplicationConfigs.HADOOP_CORE_SITE_PATH),
                "fs.defaultFS") + "/";

            properties.put(ApplicationConfigs.HDFS_URI, hdfsUri);
        }

        if (properties.get(ApplicationConfigs.HBASE_ZK_QUORUM) == null
            &&
            properties.get(ApplicationConfigs.HADOOP_HBASE_SITE_PATH) != null) {
            log.info("Going to auto-discover hbase.zookeeper.quorum");

            properties.put(ApplicationConfigs.HBASE_ZK_QUORUM,
                Utils.getPropertyFromXMLFile(
                    properties.get(ApplicationConfigs.HADOOP_HBASE_SITE_PATH),
                    "hbase.zookeeper.quorum"));
        }

        if (properties.get(ApplicationConfigs.HBASE_ZK_QUORUM_PORT) == null
            &&
            properties.get(ApplicationConfigs.HADOOP_HBASE_SITE_PATH) != null) {
            log.info("Going to auto-discover hbase.zookeeper.port");

            properties.put(ApplicationConfigs.HBASE_ZK_QUORUM_PORT,
                Utils.getPropertyFromXMLFile(
                    properties.get(ApplicationConfigs.HADOOP_HBASE_SITE_PATH),
                    "hbase.zookeeper.property.clientPort"));
        }

        if (properties.get(ApplicationConfigs.HBASE_ZK_ZNODE) == null
            &&
            properties.get(ApplicationConfigs.HADOOP_HBASE_SITE_PATH) != null) {
            log.info("Going to auto-discover hbase.zookeeper.znode");

            properties.put(ApplicationConfigs.HBASE_ZK_ZNODE,
                Utils.getPropertyFromXMLFile(
                    properties.get(ApplicationConfigs.HADOOP_HBASE_SITE_PATH),
                    "zookeeper.znode.parent"));
        }

        if (properties.get(ApplicationConfigs.OZONE_SERVICE_ID) == null
            &&
            properties.get(ApplicationConfigs.HADOOP_OZONE_SITE_PATH) != null) {
            log.info("Going to auto-discover ozone.service.id");

            properties.put(ApplicationConfigs.OZONE_SERVICE_ID,
                Utils.getPropertyFromXMLFile(
                    properties.get(ApplicationConfigs.HADOOP_OZONE_SITE_PATH),
                    "ozone.service.id"));
        }

        if (properties.get(ApplicationConfigs.HIVE_ZK_QUORUM) == null
            &&
            properties.get(ApplicationConfigs.HADOOP_HIVE_SITE_PATH) != null) {
            log.info("Going to auto-discover hive.zookeeper.quorum");

            String zookeeperPort = Utils.getPropertyFromXMLFile(
                properties.get(ApplicationConfigs.HADOOP_HIVE_SITE_PATH),
                "hive.zookeeper.client.port");
            String zookeeperPortSuffix = ":" + zookeeperPort + ",";
            String zookeeperUriWithPort = Utils.getPropertyFromXMLFile(
                properties.get(ApplicationConfigs.HADOOP_HIVE_SITE_PATH),
                "hive.zookeeper.quorum").replaceAll(",", zookeeperPortSuffix) +
                ":" + zookeeperPort;

            properties.put(ApplicationConfigs.HIVE_ZK_QUORUM,
                zookeeperUriWithPort);
        }

        if (properties.get(ApplicationConfigs.HIVE_ZK_ZNODE) == null
            &&
            properties.get(ApplicationConfigs.HADOOP_HIVE_SITE_PATH) != null) {
            log.info("Going to auto-discover hive.zookeeper.znode");

            properties.put(ApplicationConfigs.HIVE_ZK_ZNODE,
                Utils.getPropertyFromXMLFile(
                    properties.get(ApplicationConfigs.HADOOP_HIVE_SITE_PATH),
                    "hive.server2.zookeeper.namespace"));
        }

        if (properties.get(ApplicationConfigs.SOLR_ZK_QUORUM) == null
            && properties.get(ApplicationConfigs.SOLR_ENV_PATH) != null) {
            log.info("Going to auto-discover solr.zookeeper.quorum");
            properties.put(ApplicationConfigs.SOLR_ZK_QUORUM,
                Utils.getSolrZKQuorumFromEnvsh(
                    properties.get(ApplicationConfigs.SOLR_ENV_PATH)));
        }

        if (properties.get(ApplicationConfigs.SOLR_ZK_NODE) == null
            && properties.get(ApplicationConfigs.SOLR_ENV_PATH) != null) {
            log.info("Going to auto-discover solr.zookeeper.znode");
            properties.put(ApplicationConfigs.SOLR_ZK_NODE,
                Utils.getSolrZKznodeFromEnvsh(
                    properties.get(ApplicationConfigs.SOLR_ENV_PATH)));
        }

        if (properties.get(ApplicationConfigs.KAFKA_BROKERS) == null
            &&
            properties.get(ApplicationConfigs.KAFKA_CONF_CLIENT_PATH) != null) {
            log.info("Going to auto-discover kafka.brokers");
            List<String> brokers = Utils.getAllPropertiesKeyFromPropertiesFile(
                properties.get(ApplicationConfigs.KAFKA_CONF_CLIENT_PATH),
                "broker.id");
            String tlsEnabled = Utils.getOnePropertyValueFromPropertiesFile(
                properties.get(ApplicationConfigs.KAFKA_CONF_CLIENT_PATH),
                "ssl_enabled");

            if (!brokers.isEmpty() && !tlsEnabled.isEmpty()) {
                boolean isTls = Boolean.valueOf(tlsEnabled);
                String port = "9092";
                if (isTls) {
                    port = "9093";
                }
                StringBuffer sb = new StringBuffer();
                for (String broker : brokers) {
                    sb.append(broker);
                    sb.append(":");
                    sb.append(port);
                    sb.append(",");
                }
                sb.deleteCharAt(sb.lastIndexOf(","));
                properties.put(ApplicationConfigs.KAFKA_BROKERS, sb.toString());
            }
        }

        if (properties.get(ApplicationConfigs.KAFKA_SECURITY_PROTOCOL) ==
            null) {
            log.info("Going to auto-discover kafka.security.protocol");
            String tlsEnabled = Utils.getOnePropertyValueFromPropertiesFile(
                properties.get(ApplicationConfigs.KAFKA_CONF_CLIENT_PATH),
                "ssl_enabled");
            if (!tlsEnabled.isEmpty()) {
                boolean isTls = Boolean.valueOf(tlsEnabled);
                String securityProtocol = "SASL";
                if (isTls) {
                    securityProtocol = "SASL_SSL";
                }
                properties.put(ApplicationConfigs.KAFKA_SECURITY_PROTOCOL,
                    securityProtocol);
            }

        }

        if (properties.get(ApplicationConfigs.SCHEMA_REGISTRY_URL) == null
            && properties.get(ApplicationConfigs.SCHEMA_REGISTRY_CONF_PATH) !=
            null) {
            log.info("Going to auto-discover schema.registry.url");
            List<String> servers = Utils.getAllPropertiesKeyFromPropertiesFile(
                properties.get(ApplicationConfigs.SCHEMA_REGISTRY_CONF_PATH),
                "schema.registry.port");
            String tlsEnabled = Utils.getOnePropertyValueFromPropertiesFile(
                properties.get(ApplicationConfigs.SCHEMA_REGISTRY_CONF_PATH),
                "ssl.enable");

            if (!servers.isEmpty() && !tlsEnabled.isEmpty()) {
                boolean isTls = Boolean.valueOf(tlsEnabled);
                String port = Utils.getOnePropertyValueFromPropertiesFile(
                    properties.get(
                        ApplicationConfigs.SCHEMA_REGISTRY_CONF_PATH),
                    "schema.registry.port");
                if (isTls) {
                    port = Utils.getOnePropertyValueFromPropertiesFile(
                        properties.get(
                            ApplicationConfigs.SCHEMA_REGISTRY_CONF_PATH),
                        "schema.registry.ssl.port");
                    ;
                }
                StringBuffer sb = new StringBuffer();
                for (String broker : servers) {
                    sb.append(broker);
                    sb.append(":");
                    sb.append(port);
                    sb.append(",");
                }
                sb.deleteCharAt(sb.lastIndexOf(","));
                properties.put(ApplicationConfigs.SCHEMA_REGISTRY_URL,
                    sb.toString());
            }
        }

        if (properties.get(ApplicationConfigs.SCHEMA_REGISTRY_TLS_ENABLED) ==
            null
            && properties.get(ApplicationConfigs.SCHEMA_REGISTRY_CONF_PATH) !=
            null) {
            log.info("Going to auto-discover schema.registry.tls.enabled");
            properties.put(ApplicationConfigs.SCHEMA_REGISTRY_TLS_ENABLED,
                Utils.getOnePropertyValueFromPropertiesFile(
                    properties.get(
                        ApplicationConfigs.SCHEMA_REGISTRY_CONF_PATH),
                    "ssl.enable"));
        }

        // AFAIK, it is not possible to get a kudu config file
        if (properties.get(ApplicationConfigs.KUDU_URL) == null
            && properties.get(ApplicationConfigs.KUDU_CONF_PATH) != null) {
            log.info("Going to auto-discover kudu.master.server");

        }

    }


    private void autoDiscoverWithCMApi(String cmURL, String cmUser,
                                       String cmPassword, String clusterName) {
        log.debug("Starting auto-discover using CM API");
        log.debug("CM user: {} ; CM Url: {} ; CM Password: {} ; Cluster: {}",
            cmUser, cmURL, cmPassword, clusterName);

        if (cmUser.isEmpty() || cmPassword.isEmpty() || cmURL.isEmpty() ||
            clusterName.isEmpty()) {
            log.warn(
                "One of the required property to make auto-discovery with CM is missing, will not do CM auto-discovery so");
        } else {

            // Prepare API Calls
            cmApiService.configureApiService(cmURL, cmUser, cmPassword,
                clusterName);

            // Check if services exist
            Map<String, String> servicesExisting =
                cmApiService.getServicesNames();

            servicesExisting.entrySet().forEach(
                (k) -> log.info("Found service: {} with name: {}", k.getKey(),
                    k.getValue()));

            if (properties.get(ApplicationConfigs.HDFS_URI) == null) {
                log.info("Going to auto-discover hdfs.uri with CM API");

                String hdfsUri = cmApiService.getHdfsuri(servicesExisting.get("HDFS"));
                if (!hdfsUri.isEmpty()) {
                    properties.put(ApplicationConfigs.HDFS_URI,
                        cmApiService.getHdfsuri(servicesExisting.get("HDFS")));
                }
            }

            if (properties.get(ApplicationConfigs.HBASE_ZK_QUORUM) == null) {
                log.info(
                    "Going to auto-discover hbase.zookeeper.quorum with CM API");

                String zkQuorum =
                    cmApiService.getZkQuorum(servicesExisting.get("ZOOKEEPER"));
                if (!zkQuorum.isEmpty()) {
                    properties.put(ApplicationConfigs.HBASE_ZK_QUORUM,
                        zkQuorum);
                }

            }

            if (properties.get(ApplicationConfigs.HBASE_ZK_QUORUM_PORT) ==
                null) {
                log.info(
                    "Going to auto-discover hbase.zookeeper.port with CM API");

                String zkPort =
                    cmApiService.getZkPort(servicesExisting.get("ZOOKEEPER"));
                if (!zkPort.isEmpty()) {
                    properties.put(ApplicationConfigs.HBASE_ZK_QUORUM_PORT,
                        zkPort);
                }

            }

            if (properties.get(ApplicationConfigs.HBASE_ZK_ZNODE) == null) {
                log.info(
                    "Going to auto-discover hbase.zookeeper.znode with CM API");
                String zknode =
                    cmApiService.getHbaseZkZnode(servicesExisting.get("HBASE"));
                if (!zknode.isEmpty()) {
                    properties.put(ApplicationConfigs.HBASE_ZK_ZNODE, zknode);
                }

            }

                if (properties.get(ApplicationConfigs.OZONE_SERVICE_ID) ==
                    null) {
                    log.info(
                        "Going to auto-discover ozone.service.id with CM API");
                    String ozoneService =
                        cmApiService.getOzoneServiceId(servicesExisting.get("OZONE"));
                    if (!ozoneService.isEmpty()) {
                        properties.put(ApplicationConfigs.OZONE_SERVICE_ID, ozoneService);
                    }

                }

                if (properties.get(ApplicationConfigs.HIVE_ZK_QUORUM) == null) {
                    log.info(
                        "Going to auto-discover hive.zookeeper.quorum with CM API");

                    String zkQuorum = cmApiService.getZkQuorumWithPort(
                        servicesExisting.get("ZOOKEEPER"));
                    if (!zkQuorum.isEmpty()) {
                        properties.put(ApplicationConfigs.HIVE_ZK_QUORUM,
                            zkQuorum);
                    }

                }

                if (properties.get(ApplicationConfigs.HIVE_ZK_ZNODE) == null) {
                    log.info(
                        "Going to auto-discover hive.zookeeper.znode with CM API");
                    String hiveZnode =
                        cmApiService.getHiveZnode(servicesExisting.get("HIVE_ON_TEZ"));
                    if (!hiveZnode.isEmpty()) {
                        properties.put(ApplicationConfigs.HIVE_ZK_ZNODE, hiveZnode);
                    }

                }

                if (properties.get(ApplicationConfigs.SOLR_ZK_QUORUM) == null) {
                    log.info(
                        "Going to auto-discover solr.zookeeper.quorum with CM API");

                    String zkQuorum = cmApiService.getZkQuorumWithPort(
                        servicesExisting.get("ZOOKEEPER"));
                    if (!zkQuorum.isEmpty()) {
                        properties.put(ApplicationConfigs.SOLR_ZK_QUORUM,
                            zkQuorum);
                    }

                }

                if (properties.get(ApplicationConfigs.SOLR_ZK_NODE) == null) {
                    log.info(
                        "Going to auto-discover solr.zookeeper.znode with CM API");

                    String solrZnode =
                        cmApiService.getSolRZnode(servicesExisting.get("SOLR"));
                    if (!solrZnode.isEmpty()) {
                        properties.put(ApplicationConfigs.SOLR_ZK_NODE, solrZnode);
                    }

                }

                if (properties.get(ApplicationConfigs.KAFKA_BROKERS) == null) {
                    log.info(
                        "Going to auto-discover kafka.brokers with CM API");
                    String kafkaBrokers =
                        cmApiService.getKafkaBrokers(servicesExisting.get("KAFKA"));
                    if (!kafkaBrokers.isEmpty()) {
                        properties.put(ApplicationConfigs.KAFKA_BROKERS, kafkaBrokers);
                    }

                }


                if (properties.get(
                    ApplicationConfigs.KAFKA_SECURITY_PROTOCOL) ==
                    null) {
                    log.info(
                        "Going to auto-discover kafka.security.protocol with CM API");
                    String kafkaProtocol =
                        cmApiService.getKafkaProtocol(servicesExisting.get("KAFKA"));
                    if (!kafkaProtocol.isEmpty()) {
                        properties.put(ApplicationConfigs.KAFKA_SECURITY_PROTOCOL, kafkaProtocol);
                    }

                }

                if (properties.get(ApplicationConfigs.SCHEMA_REGISTRY_URL) ==
                    null) {
                    log.info(
                        "Going to auto-discover schema.registry.url with CM API");
                    String srUrl =
                        cmApiService.getSchemaRegistryUrl(servicesExisting.get("SCHEMAREGISTRY"));
                    if (!srUrl.isEmpty()) {
                        properties.put(ApplicationConfigs.SCHEMA_REGISTRY_URL, srUrl);
                    }
                }

                if (properties.get(
                    ApplicationConfigs.SCHEMA_REGISTRY_TLS_ENABLED) == null) {
                    log.info(
                        "Going to auto-discover schema.registry.tls.enabled with CM API");
                    String srTls =
                        cmApiService.getSchemaregistryTls(servicesExisting.get("SCHEMAREGISTRY"));
                    if (!srTls.isEmpty()) {
                        properties.put(ApplicationConfigs.SCHEMA_REGISTRY_TLS_ENABLED, srTls);
                    }

                }

                if (properties.get(ApplicationConfigs.KUDU_URL) == null) {
                    log.info(
                        "Going to auto-discover kudu.master.server with CM API");
                    String kuduMasters =
                        cmApiService.getKuduServers(servicesExisting.get("KUDU"));
                    if (!kuduMasters.isEmpty()) {
                        properties.put(ApplicationConfigs.KUDU_URL, kuduMasters);
                    }

                }


        }
    }
}

