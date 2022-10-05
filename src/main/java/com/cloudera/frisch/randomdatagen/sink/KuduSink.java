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
import org.apache.kudu.client.*;

import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Map;

/**
 * This is a kudu sink based on Kudu 1.11.0 API
 */
@SuppressWarnings("unchecked")
@Slf4j
public class KuduSink implements SinkInterface {

    private KuduTable table;
    private KuduSession session;
    private KuduClient client;
    private final String tableName;
    private final Model model;


    KuduSink(Model model, Map<ApplicationConfigs, String> properties) {
        this.tableName = (String) model.getTableNames().get(OptionsConverter.TableNames.KUDU_TABLE_NAME);
        this.model = model;

        try {

            System.setProperty("javax.net.ssl.trustStore", properties.get(ApplicationConfigs.KUDU_TRUSTSTORE_LOCATION));
            System.setProperty("javax.net.ssl.trustStorePassword", properties.get(ApplicationConfigs.KUDU_TRUSTSTORE_PASSWORD));

            if (Boolean.parseBoolean(properties.get(ApplicationConfigs.KUDU_AUTH_KERBEROS))) {
                Utils.loginUserWithKerberos(properties.get(ApplicationConfigs.KUDU_AUTH_KERBEROS_USER),
                    properties.get(ApplicationConfigs.KUDU_AUTH_KERBEROS_KEYTAB), new Configuration());

                UserGroupInformation.getLoginUser().doAs(
                        new PrivilegedExceptionAction<KuduClient>() {
                            @Override
                            public KuduClient run() throws Exception {
                                client = new KuduClient.KuduClientBuilder(properties.get(ApplicationConfigs.KUDU_URL)).build();
                                return client;
                            }
                        });
            } else {
                this.client = new KuduClient.KuduClientBuilder(properties.get(ApplicationConfigs.KUDU_URL)).build();
            }

            createTableIfNotExists();

            this.session = client.newSession();

            switch ((String) model.getOptionsOrDefault(OptionsConverter.Options.KUDU_FLUSH)) {
                case "AUTO_FLUSH_SYNC": session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC); break;
                case "AUTO_FLUSH_BACKGROUND": session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND); break;
                case "MANUAL_FLUSH": session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH); break;
            }
            session.setMutationBufferSpace((int) model.getOptionsOrDefault(OptionsConverter.Options.KUDU_BUFFER));

            if ((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.DELETE_PREVIOUS)) {
               client.deleteTable(tableName);
            }

            this.table = client.openTable(tableName);

        } catch (Exception e) {
            log.error("Could not connect to Kudu due to error: ", e);
        }
    }

    @Override
    public void terminate() {
        try {
            session.close();
            client.shutdown();
        } catch (Exception e) {
            log.error("Could not close connection to Kudu due to error: ", e);
        }
    }

    @Override
    public void sendOneBatchOfRows(List<Row> rows) {
        try {
            rows.parallelStream().map(row -> row.toKuduInsert(table)).forEach(insert -> {
                try {
                    session.apply(insert);
                } catch (KuduException e) {
                    log.error("Could not insert row for kudu in table : " + table + " due to error:", e );
                }
            });
            session.flush();
        } catch (Exception e) {
            log.error("Could not send rows to kudu due to error: ", e);
        }

    }

    private void createTableIfNotExists() {
        CreateTableOptions cto = new CreateTableOptions();
        cto.setNumReplicas((int) model.getOptionsOrDefault(OptionsConverter.Options.KUDU_REPLICAS));

        if(!model.getKuduRangeKeys().isEmpty()) {
            cto.setRangePartitionColumns(model.getKuduRangeKeys());
        }
        if(!model.getKuduHashKeys().isEmpty()) {
            cto.addHashPartitions(model.getKuduHashKeys(), (int) model.getOptionsOrDefault(OptionsConverter.Options.KUDU_BUCKETS));
        }

        try {
            client.createTable(tableName, model.getKuduSchema(), cto);
        } catch (KuduException e) {
            if(e.getMessage().contains("already exists")){
                log.info("Table Kudu : "  + tableName + " already exists, hence it will not be created");
            } else {
                log.error("Could not create table due to error", e);
            }
        }

    }

}
