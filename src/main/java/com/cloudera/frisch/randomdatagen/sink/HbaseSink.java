package com.cloudera.frisch.randomdatagen.sink;


import com.cloudera.frisch.randomdatagen.Utils;
import com.cloudera.frisch.randomdatagen.config.ApplicationConfigs;
import com.cloudera.frisch.randomdatagen.config.PropertiesLoader;
import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.OptionsConverter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * This is an HBase Sink using HBase API 2.3
 * It requires in application.properties to define zookeeper quorum, port, znode and type of authentication (simple or kerberos)
 * Each instance is only able to manage one connection to one specific table defined by property hbase.table.name in application.properties
 */
@Slf4j
public class HbaseSink implements SinkInterface {

    private Connection connection;
    private Table table;
    private final String fullTableName;
    private final TableName tableName;
    private Admin admin;

    HbaseSink(Model model, Map<ApplicationConfigs, String> properties) {
        this.fullTableName = model.getTableNames().get(OptionsConverter.TableNames.HBASE_NAMESPACE) + ":" +
            model.getTableNames().get(OptionsConverter.TableNames.HBASE_TABLE_NAME);
        this.tableName = TableName.valueOf(fullTableName);

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", properties.get(ApplicationConfigs.HBASE_ZK_QUORUM));
        config.set("hbase.zookeeper.property.clientPort", properties.get(ApplicationConfigs.HBASE_ZK_QUORUM_PORT));
        config.set("zookeeper.znode.parent", properties.get(ApplicationConfigs.HBASE_ZK_ZNODE));
        Utils.setupHadoopEnv(config, properties);

        // Setup Kerberos auth if needed
        if (Boolean.parseBoolean(properties.get(ApplicationConfigs.HBASE_AUTH_KERBEROS))) {
            Utils.loginUserWithKerberos(properties.get(ApplicationConfigs.HBASE_AUTH_KERBEROS_USER),
                properties.get(ApplicationConfigs.HBASE_AUTH_KERBEROS_KEYTAB), config);
            config.set("hbase.security.authentication", "kerberos");
        }

        try {
            this.connection = ConnectionFactory.createConnection(config);
            this.admin = connection.getAdmin();

            createNamespaceIfNotExists((String) model.getTableNames().get(OptionsConverter.TableNames.HBASE_NAMESPACE));

            if (admin.tableExists(tableName) && (Boolean) model.getOptionsOrDefault(OptionsConverter.Options.DELETE_PREVIOUS)) {
                admin.deleteTable(tableName);
            }

            if (!admin.tableExists(tableName)) {

                TableDescriptorBuilder tbdesc = TableDescriptorBuilder.newBuilder(tableName);

                model.getHBaseColumnFamilyList().forEach(cf ->
                        tbdesc.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes((String) cf)).build())
                );
                // In case of missing columns and to avoid troubles, a default 'cq' is created
                tbdesc.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cq")).build());

                connection.getAdmin().createTable(tbdesc.build());
            }

            this.table = connection.getTable(tableName);

        } catch (IOException e) {
            log.error("Could not initiate HBase connection due to error: ", e);
            System.exit(1);
        }
    }

    @Override
    public void terminate() {
        try {
            table.close();
            connection.close();
        } catch (IOException e) {
            log.error("Impossible to close connection to HBase due to error: ", e);
            System.exit(1);
        }
    }

    @Override
    public void sendOneBatchOfRows(List<com.cloudera.frisch.randomdatagen.model.Row> rows) {
        try {
            List<Put> putList = rows.parallelStream()
                .map(com.cloudera.frisch.randomdatagen.model.Row::toHbasePut)
                .collect(Collectors.toList());
            table.put(putList);
        } catch (Exception e) {
            log.error("Could not write to HBase rows with error : ", e);
        }
    }

    private void createNamespaceIfNotExists(String namespace) {
        try {
            try {
                admin.getNamespaceDescriptor(namespace);
            } catch (NamespaceNotFoundException e) {
                log.debug("Namespace " + namespace + " does not exists, hence it will be created");
                admin.createNamespace(NamespaceDescriptor.create(namespace).build());
            }
        } catch (IOException e) {
            log.error("Could not create namespace " + namespace + " in Hbase, due to following error: ", e);
        }
    }


}
