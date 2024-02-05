/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.frisch.datagen.connector.index;

import com.cloudera.frisch.datagen.connector.ConnectorInterface;
import com.cloudera.frisch.datagen.model.type.Field;
import com.cloudera.frisch.datagen.utils.Utils;
import com.cloudera.frisch.datagen.config.ApplicationConfigs;
import com.cloudera.frisch.datagen.model.Model;
import com.cloudera.frisch.datagen.model.OptionsConverter;
import com.cloudera.frisch.datagen.model.Row;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.*;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.stream.Collectors;


/**
 * This is a SolR sink using 8.4.0 API
 * Each instance is linked to a unique SolR collection
 */
@Slf4j
public class SolRConnector implements ConnectorInterface {

    private CloudSolrClient cloudSolrClient;
    private final String collection;
    private final Model model;
    private final Boolean useKerberos;


    public SolRConnector(Model model, Map<ApplicationConfigs, String> properties) {
        this.collection = (String) model.getTableNames().get(OptionsConverter.TableNames.SOLR_COLLECTION);
        this.model = model;
        this.useKerberos = Boolean.parseBoolean(properties.get(ApplicationConfigs.SOLR_AUTH_KERBEROS));

        List<String> zkHosts = Arrays.stream(properties.get(ApplicationConfigs.SOLR_ZK_QUORUM).split(",")).collect(Collectors.toList());
        String znode = properties.get(ApplicationConfigs.SOLR_ZK_NODE);

        if(Boolean.parseBoolean(properties.get(ApplicationConfigs.SOLR_TLS_ENABLED))) {
            System.setProperty("javax.net.ssl.keyStore", properties.get(ApplicationConfigs.SOLR_KEYSTORE_LOCATION));
            System.setProperty("javax.net.ssl.keyStorePassword", properties.get(ApplicationConfigs.SOLR_KEYSTORE_PASSWORD));
            System.setProperty("javax.net.ssl.trustStore", properties.get(ApplicationConfigs.SOLR_TRUSTSTORE_LOCATION));
            System.setProperty("javax.net.ssl.trustStorePassword", properties.get(ApplicationConfigs.SOLR_TRUSTSTORE_PASSWORD));
        }


        if (useKerberos) {
            try {
                String jaasFilePath = (String) model.getOptionsOrDefault(OptionsConverter.Options.SOLR_JAAS_FILE_PATH);
                Utils.createJaasConfigFile(jaasFilePath, "SolrJClient",
                    properties.get(ApplicationConfigs.SOLR_AUTH_KERBEROS_KEYTAB),
                    properties.get(ApplicationConfigs.SOLR_AUTH_KERBEROS_USER),
                    true, true, false);
                Utils.createJaasConfigFile(jaasFilePath, "Client",
                    properties.get(ApplicationConfigs.SOLR_AUTH_KERBEROS_KEYTAB),
                    properties.get(ApplicationConfigs.SOLR_AUTH_KERBEROS_USER),
                    true, true, true);
                System.setProperty("java.security.auth.login.config", jaasFilePath);
                System.setProperty("solr.kerberos.jaas.appname", "SolrJClient");
                System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");

                Krb5HttpClientBuilder krb5HttpClientBuilder = new Krb5HttpClientBuilder();
                HttpClientUtil.setHttpClientBuilder(krb5HttpClientBuilder.getBuilder());

                Utils.loginUserWithKerberos(properties.get(ApplicationConfigs.SOLR_AUTH_KERBEROS_USER),
                    properties.get(ApplicationConfigs.SOLR_AUTH_KERBEROS_KEYTAB), new Configuration());

                UserGroupInformation.getLoginUser().doAs(
                    new PrivilegedExceptionAction<CloudSolrClient>() {
                        @Override
                        public CloudSolrClient run() throws Exception {
                            cloudSolrClient = new CloudSolrClient.Builder(zkHosts, Optional.of(znode)).build();
                            return cloudSolrClient;
                        }
                    });
            } catch (Exception e) {
                log.error("Could not connect to Solr due to error: ", e);
            }


        } else {
            this.cloudSolrClient = new CloudSolrClient.Builder(zkHosts, Optional.of(znode)).build();
        }

    }

        @Override
        public void init(Model model, boolean writer){
            if(writer) {
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
        if(useKerberos) {
            Utils.logoutUserWithKerberos();
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
            log.error("An unexpected error occurred while adding documents to SolR collection : " +
                    collection + " due to error:", e);
        }
    }

    @Override
    public Model generateModel() {
        LinkedHashMap<String, Field> fields = new LinkedHashMap<String, Field>();
        Map<String, List<String>> primaryKeys = new HashMap<>();
        Map<String, String> tableNames = new HashMap<>();
        Map<String, String> options = new HashMap<>();
        // TODO : Implement logic to create a model with at least names, pk, options and column names/types
        return new Model(fields, primaryKeys, tableNames, options);
    }

    private void createSolRCollectionIfNotExists() {
        try {
            log.debug("Creating collection : " + collection + " in SolR");
            cloudSolrClient.request(
                    CollectionAdminRequest.createCollection(collection,
                            (Integer) model.getOptionsOrDefault(OptionsConverter.Options.SOLR_SHARDS),
                            (Integer) model.getOptionsOrDefault(OptionsConverter.Options.SOLR_REPLICAS))
            );
            log.debug("Finished to create collection : " + collection + " in SolR");
        } catch (BaseHttpSolrClient.RemoteSolrException e) {
            if (e.getMessage().contains("collection already exists")) {
                log.warn("Collection already exists so it has not been created");
            } else {
                log.error("Could not create SolR collection : " + collection + " due to error: ", e);
            }
        } catch (Exception e) {
            log.error("Could not create SolR collection : " + collection + " due to error: ", e);
        }
    }

    private void deleteSolrCollection() {
        try {
            cloudSolrClient.request(CollectionAdminRequest.deleteCollection(collection));
        } catch (SolrServerException| IOException e) {
            log.error("Could not delete previous collection: " + collection + " due to error: ", e);
        }
    }
}
