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
package com.datagen.service.api;


import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.Filter;
import com.jayway.jsonpath.JsonPath;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.net.util.Base64;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.nio.charset.Charset;
import java.util.*;
import java.util.stream.Collectors;

import static com.jayway.jsonpath.Criteria.where;
import static com.jayway.jsonpath.Filter.filter;


@Slf4j
@Service
public class CmApiService {

  @Autowired
  private RestTemplate restTemplate;

  @Getter
  @Setter
  private String cmUrl;
  @Getter
  @Setter
  private String cmUser;
  @Getter
  @Setter
  private String cmPassword;
  @Getter
  @Setter
  private String cmClusterName;
  @Getter
  @Setter
  private String cmapiUrl;

  public void configureApiService(String cmUrl, String cmUser,
                                  String cmPassword, String cmClusterName) {

    this.cmUrl = cmUrl + "/";
    this.cmUser = cmUser;
    this.cmPassword = cmPassword;
    this.cmClusterName = cmClusterName;

    this.cmapiUrl = cmUrl + "/api/" + getPlainJSON(cmUrl + "/api/version");

  }

  public String getPlainJSON(String url) {

    HttpHeaders headers = new HttpHeaders();
    headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));

    String auth = cmUser + ":" + cmPassword;
    byte[] encodedAuth = Base64.encodeBase64(
        auth.getBytes(Charset.forName("US-ASCII")));
    String authHeader = "Basic " + new String(encodedAuth);
    headers.set("Authorization", authHeader);

    HttpEntity request = new HttpEntity(headers);

    ResponseEntity<String> response =
        this.restTemplate.exchange(url, HttpMethod.GET, request, String.class,
            1);
    if (response.getStatusCode() == HttpStatus.OK) {
      return response.getBody();
    } else {
      return "";
    }
  }


  /**
   * Get All services names needed (defined in below list)
   * @return
   */
  public Map<String, String> getServicesNames() {
    Map<String, String> servicesExisting = new HashMap<>();

    try {
      String fullJson =
          getPlainJSON(cmapiUrl + "/clusters/" + cmClusterName + "/services");
      Object document =
          Configuration.defaultConfiguration().jsonProvider().parse(fullJson);

      List<String> services =
          List.of("HDFS", "HBASE", "HIVE", "HIVE_ON_TEZ", "SOLR", "KAFKA",
              "KUDU", "OZONE", "ZOOKEEPER", "SCHEMAREGISTRY");
      for (String service : services) {

        Filter filterServiceName = filter(where("type").is(service));

        List<String> servicesFound = (List<String>) JsonPath.read(document,
            "$.items[?].name"
            , filterServiceName);

        if (!servicesFound.isEmpty()) {
          List<String> servicesFoundFiltered =
              servicesFound.stream().filter(s -> !s.contains("infra")).collect(
                  Collectors.toList());
          if (!servicesFoundFiltered.isEmpty()) {
            servicesExisting.put(service, servicesFoundFiltered.get(0));
          }
        }
      }

    } catch (Exception e) {
      log.warn("Could not parse json response from CM due to error: ", e);
    }

    return servicesExisting;
  }

  /**
   * Get configuration
   * @param document
   * @param configName
   * @return
   */
  private String getConfigFromCMJson(Object document, String configName) {
    String value = "";
    try {
      Filter filterForConfig = filter(where("name").is(configName));
      List<String> values =
          (List<String>) JsonPath.read(document, "$.items[?].value",
              filterForConfig);
      if (values.isEmpty() || values.get(0) == "") {
        values =
            (List<String>) JsonPath.read(document, "$.items[?].default",
                filterForConfig);
      }
      if (!values.isEmpty()) {
        value = values.get(0);
      }
    } catch (Exception e) {
      log.warn("Cannot get configuration for {} due to error:", configName, e);
    }
    return value;
  }

  private String getConfigFromCM(String endpoint, String configName) {
    String configValue = "";
    try {
      String fullJson = getPlainJSON(
          cmapiUrl + "/clusters/" + cmClusterName + "/services/" + endpoint);
      Object document =
          Configuration.defaultConfiguration().jsonProvider().parse(fullJson);

      String value = getConfigFromCMJson(document, configName);

      if (!value.isEmpty()) {
        configValue = value;
      }

    } catch (Exception e) {
      log.info("Could not call CM API: {} - Activate debug to get full error",
          cmapiUrl + "/clusters/" + cmClusterName + "/services/" + endpoint);
      log.debug("Error: ", e);
    }

    return configValue;
  }


  public String getHdfsuri(String serviceName) {
    String hdfsUri = "";
    try {
      String fullJson = getPlainJSON(
          cmapiUrl + "/clusters/" + cmClusterName + "/services/" + serviceName
              + "/roleConfigGroups/" + serviceName +
              "-NAMENODE-BASE/config?view=full");
      Object document =
          Configuration.defaultConfiguration().jsonProvider().parse(fullJson);

      String nameservice =
          getConfigFromCMJson(document, "dfs_federation_namenode_nameservice");
      String port = getConfigFromCMJson(document, "namenode_port");

      if (!nameservice.isEmpty() && !port.isEmpty()) {
        hdfsUri = "hdfs://" + nameservice + ":" + port + "/";
      }

    } catch (Exception e) {
      log.info(
          "Could not get hdfs uri - Check HDFS exists or activate debug to get full error");
      log.debug("Error: ", e);
    }

    return hdfsUri;
  }

  public String getZkQuorum(String zkServiceName) {
    String zkHosts = "";
    try {
      String fullJson = getPlainJSON(
          cmapiUrl + "/clusters/" + cmClusterName + "/services/" +
              zkServiceName + "/roles");
      Object document =
          Configuration.defaultConfiguration().jsonProvider().parse(fullJson);

      List<String> hosts =
          (List<String>) JsonPath.read(document,
              "$.items[?(@.type == 'SERVER')].hostRef.hostname");

      if (!hosts.isEmpty()) {
        zkHosts = hosts.stream().collect(Collectors.joining(","));
      }

    } catch (Exception e) {
      log.info(
          "Could not get zookeeper quorum - Check ZK exists or activate debug to get full error");
      log.debug("Error: ", e);
    }

    return zkHosts;
  }

  public String getKafkaBrokers(String kafkaServiceName) {
    String brokers = "";
    try {
      String fullJson = getPlainJSON(
          cmapiUrl + "/clusters/" + cmClusterName + "/services/" +
              kafkaServiceName + "/roles");
      Object document =
          Configuration.defaultConfiguration().jsonProvider().parse(fullJson);

      List<String> hosts =
          (List<String>) JsonPath.read(document,
              "$.items[?(@.type == 'KAFKA_BROKER')].hostRef.hostname");

      String tlsEnabled = getConfigFromCM(
          kafkaServiceName + "/roleConfigGroups/" + kafkaServiceName +
              "-KAFKA_BROKER-BASE/config?view=full", "ssl_enabled");
      String port = "";
      if (!tlsEnabled.isEmpty()) {
        if (Boolean.valueOf(tlsEnabled)) {
          port = getConfigFromCM(kafkaServiceName +
                  "/roleConfigGroups/" + kafkaServiceName +
                  "-KAFKA_BROKER-BASE/config?view=full",
              "ssl_port");
        } else {
          port = getConfigFromCM(kafkaServiceName +
                  "/roleConfigGroups/" + kafkaServiceName +
                  "-KAFKA_BROKER-BASE/config?view=full",
              "port");
        }
      }

      if (!hosts.isEmpty()) {
        brokers =
            hosts.stream().collect(Collectors.joining(":" + port + ",")) + ":" +
                port;
      }

    } catch (Exception e) {
      log.info(
          "Could not get kafka brokers - Check Kafka exists or activate debug to get full error");
      log.debug("Error: ", e);
    }

    return brokers;
  }

  public String getKafkaProtocol(String kafkaServiceName) {
    String protocol = "";
    try {
      String fullJson = getPlainJSON(
          cmapiUrl + "/clusters/" + cmClusterName + "/services/" +
              kafkaServiceName + "/roles");
      Object document =
          Configuration.defaultConfiguration().jsonProvider().parse(fullJson);

      String tlsEnabled = getConfigFromCM(
          kafkaServiceName + "/roleConfigGroups/" + kafkaServiceName +
              "-KAFKA_BROKER-BASE/config?view=full", "ssl_enabled");
      String kerberosEnabled =
          getConfigFromCM(kafkaServiceName + "/config?view=full",
              "kerberos.auth.enable");

      if (!tlsEnabled.isEmpty() && !kerberosEnabled.isEmpty()) {
        if (Boolean.valueOf(tlsEnabled) && Boolean.valueOf(kerberosEnabled)) {
          protocol = "SASL_SSL";
        } else if (!Boolean.valueOf(tlsEnabled) &&
            Boolean.valueOf(kerberosEnabled)) {
          protocol = "SASL_PLAINTEXT";
        } else if (Boolean.valueOf(tlsEnabled) &&
            !Boolean.valueOf(kerberosEnabled)) {
          protocol = "SSL";
        } else {
          protocol = "PLAINTEXT";
        }
      }

    } catch (Exception e) {
      log.info(
          "Could not get kafka Protocol - Check Kafka exists or activate debug to get full error");
      log.debug("Error: ", e);
    }

    return protocol;

  }

  public String getSchemaRegistryUrl(String srServiceName) {
    String servers = "";
    try {
      String fullJson = getPlainJSON(
          cmapiUrl + "/clusters/" + cmClusterName + "/services/" +
              srServiceName + "/roles");
      Object document =
          Configuration.defaultConfiguration().jsonProvider().parse(fullJson);

      List<String> hosts =
          (List<String>) JsonPath.read(document,
              "$.items[?(@.type == 'SCHEMA_REGISTRY_SERVER')].hostRef.hostname");

      String tlsEnabled = getConfigFromCM(
          srServiceName + "/roleConfigGroups/" + srServiceName +
              "-SCHEMA_REGISTRY_SERVER-BASE/config?view=full", "ssl_enabled");
      String port = "";
      if (!tlsEnabled.isEmpty()) {
        if (Boolean.valueOf(tlsEnabled)) {
          port = getConfigFromCM(srServiceName +
                  "/roleConfigGroups/" + srServiceName +
                  "-SCHEMA_REGISTRY_SERVER-BASE/config?view=full",
              "schema.registry.ssl.port");
        } else {
          port = getConfigFromCM(srServiceName +
                  "/roleConfigGroups/" + srServiceName +
                  "-SCHEMA_REGISTRY_SERVER-BASE/config?view=full",
              "schema.registry.port");
        }
      }

      if (!hosts.isEmpty()) {
        servers =
            hosts.stream().collect(Collectors.joining(":" + port + ",")) + ":" +
                port;
      }

    } catch (Exception e) {
      log.info(
          "Could not get Schema Registry URL - Check SR exists or activate debug to get full error");
      log.debug("Error: ", e);
    }

    return servers;
  }

  public String getSchemaregistryTls(String srServiceName) {
    return getConfigFromCM(
        srServiceName + "/roleConfigGroups/" + srServiceName +
            "-SCHEMA_REGISTRY_SERVER-BASE/config?view=full", "ssl_enabled");
  }

  public String getKuduServers(String kuduServiceName) {
    String servers = "";
    try {
      String fullJson = getPlainJSON(
          cmapiUrl + "/clusters/" + cmClusterName + "/services/" +
              kuduServiceName + "/roles");
      Object document =
          Configuration.defaultConfiguration().jsonProvider().parse(fullJson);

      List<String> hosts =
          (List<String>) JsonPath.read(document,
              "$.items[?(@.type == 'KUDU_MASTER')].hostRef.hostname");

      String tlsEnabled = getConfigFromCM(
          kuduServiceName + "/roleConfigGroups/" + kuduServiceName +
              "-KUDU_MASTER-BASE/config?view=full", "ssl_enabled");
      String port = "";
      if (!tlsEnabled.isEmpty()) {
        if (Boolean.valueOf(tlsEnabled)) {
          port = "7051";
        } else {
          port = "7050";
        }
      }

      if (!hosts.isEmpty()) {
        servers =
            hosts.stream().collect(Collectors.joining(":" + port + ",")) + ":" +
                port;
      }

    } catch (Exception e) {
      log.info(
          "Could not get kudu servers - Check Kudu exists or activate debug to get full error");
      log.debug("Error: ", e);
    }

    return servers;
  }


  public String getZkQuorumWithPort(String zkServiceName) {
    String zkHosts = getZkQuorum(zkServiceName);
    String zkPort = getConfigFromCM(
        zkServiceName + "/roleConfigGroups/" + zkServiceName +
            "-SERVER-BASE/config?view=full", "clientPort");
    return Arrays.stream(zkHosts.split(","))
        .collect(Collectors.joining(":" + zkPort + ",")) + ":" + zkPort;
  }

  public String getZkPort(String zkServiceName) {
    return getConfigFromCM(
        zkServiceName + "/roleConfigGroups/" + zkServiceName +
            "-SERVER-BASE/config?view=full", "clientPort");
  }

  public String getHbaseZkZnode(String serviceName) {
    return getConfigFromCM(serviceName + "/config?view=full",
        "zookeeper_znode_parent");
  }

  public String getOzoneServiceId(String serviceName) {
    return getConfigFromCM(serviceName + "/config?view=full",
        "ozone.service.id");
  }

  public String getHiveZnode(String serviceName) {
    return getConfigFromCM(serviceName + "/config?view=full",
        "hive_server2_zookeeper_namespace");
  }

  public String getSolRZnode(String serviceName) {
    return getConfigFromCM(serviceName + "/config?view=full",
        "zookeeper_znode");
  }


}
