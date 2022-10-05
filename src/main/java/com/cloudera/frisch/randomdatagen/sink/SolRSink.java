package com.cloudera.frisch.randomdatagen.sink;

import com.cloudera.frisch.randomdatagen.Utils;
import com.cloudera.frisch.randomdatagen.config.ApplicationConfigs;
import com.cloudera.frisch.randomdatagen.config.PropertiesLoader;
import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.OptionsConverter;
import com.cloudera.frisch.randomdatagen.model.Row;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kudu.client.KuduClient;
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
public class SolRSink implements SinkInterface {

    private CloudSolrClient cloudSolrClient;
    private final String collection;
    private final Model model;


    SolRSink(Model model, Map<ApplicationConfigs, String> properties) {
        this.collection = (String) model.getTableNames().get(OptionsConverter.TableNames.SOLR_COLLECTION);
        this.model = model;

        List<String> zkHosts = Arrays.stream(properties.get(ApplicationConfigs.SOLR_ZK_QUORUM).split(",")).collect(Collectors.toList());
        String znode = properties.get(ApplicationConfigs.SOLR_ZK_NODE);

        if(Boolean.parseBoolean(properties.get(ApplicationConfigs.SOLR_TLS_ENABLED))) {
            System.setProperty("javax.net.ssl.keyStore", properties.get(ApplicationConfigs.SOLR_KEYSTORE_LOCATION));
            System.setProperty("javax.net.ssl.keyStorePassword", properties.get(ApplicationConfigs.SOLR_KEYSTORE_PASSWORD));
            System.setProperty("javax.net.ssl.trustStore", properties.get(ApplicationConfigs.SOLR_TRUSTSTORE_LOCATION));
            System.setProperty("javax.net.ssl.trustStorePassword", properties.get(ApplicationConfigs.SOLR_TRUSTSTORE_PASSWORD));
        }


        if (Boolean.parseBoolean(properties.get(ApplicationConfigs.SOLR_AUTH_KERBEROS))) {
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


        if ((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.DELETE_PREVIOUS)) {
            deleteSolrCollection();
        }

        createSolRCollectionIfNotExists();

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
