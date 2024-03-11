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
package com.cloudera.frisch.datagen.connector.db.hbase;


import com.cloudera.frisch.datagen.connector.ConnectorInterface;
import com.cloudera.frisch.datagen.model.type.Field;
import com.cloudera.frisch.datagen.utils.Utils;
import com.cloudera.frisch.datagen.config.ApplicationConfigs;
import com.cloudera.frisch.datagen.model.Model;
import com.cloudera.frisch.datagen.model.OptionsConverter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * This is an HBase connector using HBase API 2.3
 * It requires in application.properties to define zookeeper quorum, port, znode and type of authentication (simple or kerberos)
 * Each instance is only able to manage one connection to one specific table defined by property hbase.table.name in application.properties
 */
@Slf4j
public class HbaseConnector implements ConnectorInterface {

    private Connection connection;
    private Table table;
    private final TableName tableName;
    private Admin admin;
    private final Boolean useKerberos;
    private final Configuration config;

    public HbaseConnector(Model model, Map<ApplicationConfigs, String> properties) {
        String fullTableName = model.getTableNames()
            .get(OptionsConverter.TableNames.HBASE_NAMESPACE) + ":" +
            model.getTableNames()
                .get(OptionsConverter.TableNames.HBASE_TABLE_NAME);
        this.tableName = TableName.valueOf(fullTableName);
        this.useKerberos = Boolean.parseBoolean(
            properties.get(ApplicationConfigs.HBASE_AUTH_KERBEROS));

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum",
            properties.get(ApplicationConfigs.HBASE_ZK_QUORUM));
        config.set("hbase.zookeeper.property.clientPort",
            properties.get(ApplicationConfigs.HBASE_ZK_QUORUM_PORT));
        config.set("zookeeper.znode.parent",
            properties.get(ApplicationConfigs.HBASE_ZK_ZNODE));
        Utils.setupHadoopEnv(config, properties);

        // Setup Kerberos auth if needed
        if (useKerberos) {
            Utils.loginUserWithKerberos(
                properties.get(ApplicationConfigs.HBASE_AUTH_KERBEROS_USER),
                properties.get(ApplicationConfigs.HBASE_AUTH_KERBEROS_KEYTAB),
                config);
            config.set("hbase.security.authentication", "kerberos");
        }
        this.config = config;
    }

    @Override
    public void init(Model model, boolean writer) {
        try {
            this.connection = ConnectionFactory.createConnection(this.config);
            this.admin = connection.getAdmin();

            if(writer) {
                createNamespaceIfNotExists((String) model.getTableNames()
                    .get(OptionsConverter.TableNames.HBASE_NAMESPACE));

                if (admin.tableExists(tableName) &&
                    (Boolean) model.getOptionsOrDefault(
                        OptionsConverter.Options.DELETE_PREVIOUS)) {
                    admin.deleteTable(tableName);
                }

                if (!admin.tableExists(tableName)) {

                    TableDescriptorBuilder tbdesc =
                        TableDescriptorBuilder.newBuilder(tableName);

                    model.getHBaseColumnFamilyList().forEach(cf ->
                        tbdesc.setColumnFamily(
                            ColumnFamilyDescriptorBuilder.newBuilder(
                                Bytes.toBytes((String) cf)).build())
                    );
                    // In case of missing columns and to avoid troubles, a default 'cq' is created
                    tbdesc.setColumnFamily(
                        ColumnFamilyDescriptorBuilder.newBuilder(
                            Bytes.toBytes("cq")).build());

                    connection.getAdmin().createTable(tbdesc.build());
                }
            }

            this.table = connection.getTable(tableName);

        } catch (IOException e) {
            log.error("Could not initiate HBase connection due to error: ", e);
        }
    }

    @Override
    public void terminate() {
        try {
            table.close();
            connection.close();
            if(useKerberos) {
                Utils.logoutUserWithKerberos();
            }
        } catch (IOException e) {
            log.error("Impossible to close connection to HBase due to error: ", e);
            System.exit(1);
        }
    }

    @Override
    public void sendOneBatchOfRows(List<com.cloudera.frisch.datagen.model.Row> rows) {
        try {
            List<Put> putList = rows.parallelStream()
                .map(com.cloudera.frisch.datagen.model.Row::toHbasePut)
                .collect(Collectors.toList());
            table.put(putList);
        } catch (Exception e) {
            log.error("Could not write to HBase rows with error : ", e);
        }
    }

    @Override
    public Model generateModel(Boolean deepAnalysis) {
        LinkedHashMap<String, Field> fields = new LinkedHashMap<String, Field>();
        Map<String, List<String>> primaryKeys = new HashMap<>();
        Map<String, String> tableNames = new HashMap<>();
        Map<String, String> options = new HashMap<>();
        // TODO : Implement logic to create a model with at least names, pk, options and column names/types
        return new Model(fields, primaryKeys, tableNames, options);
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
