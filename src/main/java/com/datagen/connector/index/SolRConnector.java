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
package com.datagen.connector.index;

import com.datagen.config.ApplicationConfigs;
import com.datagen.connector.ConnectorInterface;
import com.datagen.model.Model;
import com.datagen.model.OptionsConverter;
import com.datagen.model.Row;
import com.datagen.model.type.Field;
import com.datagen.utils.KerberosUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.Krb5HttpClientBuilder;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;

import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.stream.Collectors;


/**
 * This is a SolR connector using 8.4.0 API
 * Each instance is linked to a unique SolR collection
 */
@Slf4j
public class SolRConnector implements ConnectorInterface {

  private CloudSolrClient cloudSolrClient;
  private final String collection;
  private final Model model;
  private final Boolean useKerberos;

  public SolRConnector(Model model,
                       Map<ApplicationConfigs, String> properties) {
    this.collection = (String) model.getTableNames()
        .get(OptionsConverter.TableNames.SOLR_COLLECTION);
    this.model = model;
    this.useKerberos = model.getTableNames().get(OptionsConverter.TableNames.SOLR_USE_KERBEROS)==null ?
        Boolean.parseBoolean(properties.get(ApplicationConfigs.SOLR_AUTH_KERBEROS)) :
        Boolean.parseBoolean(model.getTableNames().get(OptionsConverter.TableNames.SOLR_USE_KERBEROS).toString());

    List<String> zkHosts = Arrays.stream(
            properties.get(ApplicationConfigs.SOLR_ZOOKEEPER_QUORUM).split(","))
        .collect(Collectors.toList());
    String znode = properties.get(ApplicationConfigs.SOLR_ZOOKEEPER_ZNODE);

    if (Boolean.parseBoolean(
        properties.get(ApplicationConfigs.SOLR_TLS_ENABLED))) {
      System.setProperty("javax.net.ssl.keyStore",
          model.getTableNames().get(OptionsConverter.TableNames.SOLR_KEYSTORE_LOCATION)==null ?
          properties.get(ApplicationConfigs.SOLR_KEYSTORE_LOCATION) :
              model.getTableNames().get(OptionsConverter.TableNames.SOLR_KEYSTORE_LOCATION).toString());
      System.setProperty("javax.net.ssl.keyStorePassword",
          model.getTableNames().get(OptionsConverter.TableNames.SOLR_KEYSTORE_PASSWORD)==null ?
              properties.get(ApplicationConfigs.SOLR_KEYSTORE_PASSWORD) :
              model.getTableNames().get(OptionsConverter.TableNames.SOLR_KEYSTORE_PASSWORD).toString()
          );
      System.setProperty("javax.net.ssl.trustStore",
          model.getTableNames().get(OptionsConverter.TableNames.SOLR_TRUSTSTORE_LOCATION)==null ?
              properties.get(ApplicationConfigs.SOLR_TRUSTSTORE_LOCATION) :
              model.getTableNames().get(OptionsConverter.TableNames.SOLR_TRUSTSTORE_LOCATION).toString());
      System.setProperty("javax.net.ssl.trustStorePassword",
          model.getTableNames().get(OptionsConverter.TableNames.SOLR_TRUSTSTORE_PASSWORD)==null ?
              properties.get(ApplicationConfigs.SOLR_TRUSTSTORE_PASSWORD) :
              model.getTableNames().get(OptionsConverter.TableNames.SOLR_TRUSTSTORE_PASSWORD).toString());
    }


    if (useKerberos) {
      try {
        String jaasFilePath = (String) model.getOptionsOrDefault(
            OptionsConverter.Options.SOLR_JAAS_FILE_PATH);
        KerberosUtils.createJaasConfigFile(jaasFilePath, "SolrJClient",
            model.getTableNames().get(OptionsConverter.TableNames.SOLR_KEYTAB)==null ?
                properties.get(ApplicationConfigs.SOLR_AUTH_KERBEROS_KEYTAB) :
                model.getTableNames().get(OptionsConverter.TableNames.SOLR_KEYTAB).toString(),
            model.getTableNames().get(OptionsConverter.TableNames.SOLR_USER)==null ?
                properties.get(ApplicationConfigs.SOLR_AUTH_KERBEROS_USER) :
                model.getTableNames().get(OptionsConverter.TableNames.SOLR_USER).toString(),
            true, true, false);
        KerberosUtils.createJaasConfigFile(jaasFilePath, "Client",
            model.getTableNames().get(OptionsConverter.TableNames.SOLR_KEYTAB)==null ?
                properties.get(ApplicationConfigs.SOLR_AUTH_KERBEROS_KEYTAB) :
                model.getTableNames().get(OptionsConverter.TableNames.SOLR_KEYTAB).toString(),
            model.getTableNames().get(OptionsConverter.TableNames.SOLR_USER)==null ?
                properties.get(ApplicationConfigs.SOLR_AUTH_KERBEROS_USER) :
                model.getTableNames().get(OptionsConverter.TableNames.SOLR_USER).toString(),
            true, true, true);
        System.setProperty("java.security.auth.login.config", jaasFilePath);
        System.setProperty("solr.kerberos.jaas.appname", "SolrJClient");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");

        Krb5HttpClientBuilder krb5HttpClientBuilder =
            new Krb5HttpClientBuilder();
        HttpClientUtil.setHttpClientBuilder(krb5HttpClientBuilder.getBuilder());

        KerberosUtils.loginUserWithKerberos(
            model.getTableNames().get(OptionsConverter.TableNames.SOLR_USER)==null ?
                properties.get(ApplicationConfigs.SOLR_AUTH_KERBEROS_USER) :
                model.getTableNames().get(OptionsConverter.TableNames.SOLR_USER).toString(),
            model.getTableNames().get(OptionsConverter.TableNames.SOLR_KEYTAB)==null ?
                properties.get(ApplicationConfigs.SOLR_AUTH_KERBEROS_KEYTAB) :
                model.getTableNames().get(OptionsConverter.TableNames.SOLR_KEYTAB).toString(),
            new Configuration());

        UserGroupInformation.getLoginUser().doAs(
            new PrivilegedExceptionAction<CloudSolrClient>() {
              @Override
              public CloudSolrClient run() throws Exception {
                cloudSolrClient = new CloudSolrClient.Builder(zkHosts,
                    Optional.of(znode)).build();
                return cloudSolrClient;
              }
            });
      } catch (Exception e) {
        log.error("Could not connect to Solr due to error: ", e);
      }


    } else {
      this.cloudSolrClient =
          new CloudSolrClient.Builder(zkHosts, Optional.of(znode)).build();
    }

  }

  @Override
  public void init(Model model, boolean writer) {
    if (writer) {
      if ((Boolean) model.getOptionsOrDefault(
          OptionsConverter.Options.DELETE_PREVIOUS)) {
        deleteSolrCollection();
      }
      createSolRCollectionIfNotExists();
    }
    // Set base URL directly to the collection, note that this is required
    cloudSolrClient.setDefaultCollection(collection);
  }


  @Override
  public void terminate() {
    try {
      cloudSolrClient.close();
    } catch (Exception e) {
      log.error("Could not close connection to SolR due to error: ", e);
    }
    if (useKerberos) {
      KerberosUtils.logoutUserWithKerberos();
    }
  }

  @Override
  public void sendOneBatchOfRows(List<Row> rows) {
    try {
      cloudSolrClient.add(
          rows.parallelStream().map(Row::toSolRDoc).collect(Collectors.toList())
      );
      cloudSolrClient.commit();
    } catch (Exception e) {
      log.error(
          "An unexpected error occurred while adding documents to SolR collection : " +
              collection + " due to error:", e);
    }
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

  private void createSolRCollectionIfNotExists() {
    try {
      log.debug("Creating collection : " + collection + " in SolR");
      cloudSolrClient.request(
          CollectionAdminRequest.createCollection(collection,
              (Integer) model.getOptionsOrDefault(
                  OptionsConverter.Options.SOLR_SHARDS),
              (Integer) model.getOptionsOrDefault(
                  OptionsConverter.Options.SOLR_REPLICAS))
      );
      log.debug("Finished to create collection : " + collection + " in SolR");
    } catch (BaseHttpSolrClient.RemoteSolrException e) {
      if (e.getMessage().contains("collection already exists")) {
        log.warn("Collection already exists so it has not been created");
      } else {
        log.error("Could not create SolR collection : " + collection +
            " due to error: ", e);
      }
    } catch (Exception e) {
      log.error("Could not create SolR collection : " + collection +
          " due to error: ", e);
    }
  }

  private void deleteSolrCollection() {
    try {
      cloudSolrClient.request(
          CollectionAdminRequest.deleteCollection(collection));
    } catch (Exception e) {
      log.warn("Could not delete previous collection: {} due to error: ", collection, e);
    }
  }
}
