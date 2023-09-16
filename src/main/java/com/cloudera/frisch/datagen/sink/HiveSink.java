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
package com.cloudera.frisch.datagen.sink;

import com.cloudera.frisch.datagen.utils.Utils;
import com.cloudera.frisch.datagen.config.ApplicationConfigs;
import com.cloudera.frisch.datagen.model.Model;
import com.cloudera.frisch.datagen.model.OptionsConverter;
import com.cloudera.frisch.datagen.model.Row;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.jdbc.HivePreparedStatement;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CountDownLatch;


/**
 * This a HiveSink, each instance manages its own session and a preparedStatement for insertion
 * It is recommended to use HDFS option that will create HDFS files before laoding them into Hive using a SQL statement.
 * An inner class @see{com.cloudera.frisch.randomdatagen.sink.HiveSinkParallel} below, allows multi threaded inserts,
 * It is recommended to not insert too many rows as Hive is very slow on insertion and use the batch function,
 * with a high number of rows per batch and few batches (to avoid recreating connection to Hive each time)
 * and with a maximum of 20 threads (configurable in application.properties)
 */
@SuppressWarnings("unchecked")
@Slf4j
public class HiveSink implements SinkInterface {

    enum HiveTableType {
        EXTERNAL,
        MANAGED,
        ICEBERG
    }

    public HiveTableType getHiveTableType(String hivetype) {
        switch (hivetype.toLowerCase(Locale.ROOT)) {
            case "iceberg":
                return HiveTableType.ICEBERG;
            case "managed":
                return HiveTableType.MANAGED;
            default:
                return HiveTableType.EXTERNAL;
        }

    }

    private final int threads_number;
    private final String hiveUri;
    private Connection hiveConnection;
    private HdfsParquetSink hdfsSink;
    private final String database;
    private final String tableName;
    private final String tableNameTemporary;
    private String insertStatement;
    private final boolean hiveOnHDFS;
    private final String queue;
    private Boolean useKerberos;
    private Boolean isPartitioned;
    private LinkedList<String> partCols;
    private Boolean isBucketed;
    private LinkedList<String> bucketCols;
    private int bucketNumber;
    private String extraCreate;
    private String extraInsert;
    private HiveTableType hiveTableType;


    HiveSink(Model model, Map<ApplicationConfigs, String> properties) {
        this.hiveOnHDFS = (Boolean) model.getOptionsOrDefault(OptionsConverter.Options.HIVE_ON_HDFS);
        this.threads_number = (Integer) model.getOptionsOrDefault(OptionsConverter.Options.HIVE_THREAD_NUMBER);
        this.database = (String) model.getTableNames().get(OptionsConverter.TableNames.HIVE_DATABASE);
        this.tableName = (String) model.getTableNames().get(OptionsConverter.TableNames.HIVE_TABLE_NAME);
        this.tableNameTemporary = model.getTableNames().get(OptionsConverter.TableNames.HIVE_TEMPORARY_TABLE_NAME)==null ?
            tableName + "_tmp" : (String) model.getTableNames().get(OptionsConverter.TableNames.HIVE_TEMPORARY_TABLE_NAME);
        this.queue = (String) model.getOptionsOrDefault(OptionsConverter.Options.HIVE_TEZ_QUEUE_NAME);
        String locationTemporaryTable = (String) model.getTableNames().get(OptionsConverter.TableNames.HIVE_HDFS_FILE_PATH);
        this.hiveUri = "jdbc:hive2://" + properties.get(ApplicationConfigs.HIVE_ZK_QUORUM) + "/" +
            database + ";serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=" +
            properties.get(ApplicationConfigs.HIVE_ZK_ZNODE) + "?tez.queue.name=" + queue ;
        this.useKerberos = Boolean.parseBoolean(properties.get(ApplicationConfigs.HIVE_AUTH_KERBEROS));
        this.hiveTableType = getHiveTableType((String) model.getOptionsOrDefault(OptionsConverter.Options.HIVE_TABLE_TYPE));

        this.isPartitioned = !((String) model.getOptionsOrDefault(OptionsConverter.Options.HIVE_TABLE_PARTITIONS_COLS)).isEmpty();
        this.partCols = new LinkedList<>();
        if(isPartitioned) {
            for(String colName: ((String) model.getOptionsOrDefault(OptionsConverter.Options.HIVE_TABLE_PARTITIONS_COLS)).split(",")) {
                partCols.add(colName);
            }
            model.reorderColumnsWithPartCols(partCols);
        }

        this.isBucketed = !((String) model.getOptionsOrDefault(OptionsConverter.Options.HIVE_TABLE_BUCKETS_COLS)).isEmpty();
        this.bucketCols = new LinkedList<>();
        if(isBucketed) {
            this.bucketNumber = (Integer) model.getOptionsOrDefault(OptionsConverter.Options.HIVE_TABLE_BUCKETS_NUMBER);
            for(String colName: ((String) model.getOptionsOrDefault(OptionsConverter.Options.HIVE_TABLE_BUCKETS_COLS)).split(",")) {
                bucketCols.add(colName);
            }
        }

        this.extraCreate = model.getSQLPartBucketCreate(partCols, bucketCols, bucketNumber);
        this.extraInsert =  model.getSQLPartBucketInsert(partCols, bucketCols, bucketNumber);

        Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("iceberg.engine.hive.enabled", "true");
        Utils.setupHadoopEnv(hadoopConf, properties);

        try {
            if (useKerberos) {
                Utils.loginUserWithKerberos(properties.get(ApplicationConfigs.HIVE_AUTH_KERBEROS_USER),
                    properties.get(ApplicationConfigs.HIVE_AUTH_KERBEROS_KEYTAB), new Configuration());
            }


            if(properties.get(ApplicationConfigs.HIVE_TRUSTSTORE_LOCATION)!=null && properties.get(ApplicationConfigs.HIVE_TRUSTSTORE_PASSWORD)!=null) {
                System.setProperty("javax.net.ssl.trustStore", properties.get(
                    ApplicationConfigs.HIVE_TRUSTSTORE_LOCATION));
                System.setProperty("javax.net.ssl.trustStorePassword",
                    properties.get(
                        ApplicationConfigs.HIVE_TRUSTSTORE_PASSWORD));
            }

            java.util.Properties propertiesForHive = new Properties();
            propertiesForHive.put("tez.queue.name", queue);
            if(isPartitioned) {
                propertiesForHive.put("hive.exec.dynamic.partition.mode", "nonstrict");
                propertiesForHive.put("hive.exec.dynamic.partition", true);
            }
            if(isBucketed) {
                propertiesForHive.put("hive.enforce.bucketing", true);
            }
            if(hiveTableType == HiveTableType.ICEBERG) {
                propertiesForHive.put("hive.vectorized.execution.enabled",false);
                propertiesForHive.put("tez.mrreader.config.update.properties", "hive.io.file.readcolumn.names,hive.io.file.readcolumn.ids");
            }


            String hiveUriWithNoDatabase = "jdbc:hive2://" + properties.get(ApplicationConfigs.HIVE_ZK_QUORUM) +
                "/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=" + properties.get(ApplicationConfigs.HIVE_ZK_ZNODE) +
                "?tez.queue.name=" + queue ;

            this.hiveConnection = DriverManager.getConnection(hiveUriWithNoDatabase, propertiesForHive);

            prepareAndExecuteStatement("CREATE DATABASE IF NOT EXISTS " + database);

            hiveConnection.setSchema(database);

            if ((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.DELETE_PREVIOUS)) {
                prepareAndExecuteStatement("DROP TABLE IF EXISTS " + tableName);
            }

            log.info("SQL schema for hive: " + model.getSQLSchema(partCols) + this.extraCreate );
            if(hiveTableType == HiveTableType.MANAGED) {
                log.info("Creating Managed table: " + tableName);
                prepareAndExecuteStatement(
                    "CREATE TABLE IF NOT EXISTS " + tableName +
                        model.getSQLSchema(partCols) + this.extraCreate);
            } else if (hiveTableType == HiveTableType.ICEBERG) {
                log.info("Creating Iceberg table: " + tableName);
                prepareAndExecuteStatement(
                    "CREATE TABLE IF NOT EXISTS " + tableName +
                        model.getSQLSchema(partCols) + this.extraCreate + " STORED BY ICEBERG");
            } else if(hiveTableType == HiveTableType.EXTERNAL) {
                log.info("Creating table: " + tableName);
                prepareAndExecuteStatement(
                    "CREATE EXTERNAL TABLE IF NOT EXISTS " + tableName +
                        model.getSQLSchema(null) +
                        " STORED AS PARQUET " +
                        " LOCATION '" + locationTemporaryTable + "'"
                );
            }

            if (hiveOnHDFS) {
                // If using an HDFS sink, we want it to use the Hive HDFS File path and not the Hdfs file path
                properties.put(ApplicationConfigs.HDFS_FOR_HIVE, "true");
                this.hdfsSink = new HdfsParquetSink(model, properties);

                if(hiveTableType == HiveTableType.MANAGED) {
                    log.info("Creating temporary table: " + tableNameTemporary);
                    prepareAndExecuteStatement(
                        "CREATE EXTERNAL TABLE IF NOT EXISTS " + tableNameTemporary + model.getSQLSchema(null) +
                            " STORED AS PARQUET " +
                            " LOCATION '" + locationTemporaryTable + "'"
                    );
                }
                // Reset it to false, so if another sink is hdfs, it does not initialize like Hive
                properties.put(ApplicationConfigs.HDFS_FOR_HIVE, "false");
            }

            log.info("SQL Insert schema for hive: " + model.getInsertSQLStatement() + this.extraInsert );
            insertStatement = "INSERT INTO " + tableName + model.getInsertSQLStatement() + this.extraInsert;

        } catch (SQLException e) {
            log.error("Could not connect to HS2 and create table due to error: ", e);
        }
    }

    @Override
    public void terminate() {
        try {
            if (hiveOnHDFS) {
                if(hiveTableType != HiveTableType.EXTERNAL) {
                    log.info("Starting to load data to final table");
                    prepareAndExecuteStatement(
                        "INSERT INTO " + tableName + this.extraInsert +
                            " SELECT * FROM " + tableNameTemporary);
                    log.info("Dropping Temporary Table");
                    prepareAndExecuteStatement("DROP TABLE IF EXISTS " + tableNameTemporary);
                }
            }

            hiveConnection.close();

            if(useKerberos) {
                Utils.logoutUserWithKerberos();
            }

        } catch (SQLException e) {
            log.error("Could not close the Hive connection due to error: ", e);
        }
    }


    /**
     * As Hive insertions are very slow, this function has been designed to either send directly to Hive or use HDFSCSV
     *
     * @param rows list of rows to write to Hive
     */
    @Override
    public void sendOneBatchOfRows(List<Row> rows) {
        if (hiveOnHDFS) {
            hdfsSink.sendOneBatchOfRows(rows);
        } else {
            senOneBatchOfRowsDirectlyToHive(rows);
        }
    }


    /**
     * As Hive insertions are very slow, this function has been designed to send request to Hive through many threads synchronously
     *
     * @param rows list of rows to write to Hive
     */
    private void senOneBatchOfRowsDirectlyToHive(List<Row> rows) {
        int lengthToTake = rows.size() / threads_number;
        CountDownLatch latch = new CountDownLatch(threads_number);

        for (int i = 0; i < threads_number; i++) {
            HiveSinkParallel oneHiveSinkThread = new HiveSinkParallel(rows.subList(i * lengthToTake, i * lengthToTake + lengthToTake), latch);
            oneHiveSinkThread.setName("Thread-" + i);
            oneHiveSinkThread.start();
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            log.error("Thread interrupted in the middle of a treatment : ", e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * In order to accelerate Hive ingestion of data, this class has been created to allow parallelization of insertion
     * Hive is commit enabled by default (and cannot be changed), hence each request is taking few seconds to be fully committed and agreed by HIVE
     */
    private class HiveSinkParallel extends Thread {
        private List<Row> rows;
        private final CountDownLatch latch;

        HiveSinkParallel(List<Row> rows, CountDownLatch latch) {
            this.rows = rows;
            this.latch = latch;
        }

        @Override
        public void run() {
            try (HiveConnection hiveConnectionPerThread =
                     new HiveConnection(hiveUri, new Properties())) {

                HivePreparedStatement hivePreparedStatementByThread =
                        (HivePreparedStatement) hiveConnectionPerThread.prepareStatement(insertStatement);

                rows.forEach(row -> {
                    try {
                        log.debug("Inserting one row " + row.toString() + " from thread : " + getName());

                        hivePreparedStatementByThread.clearParameters();
                        row.toHiveStatement(hivePreparedStatementByThread);
                        hivePreparedStatementByThread.execute();

                        log.debug("Finished to insert one row " + row.toString() + " from thread : " + getName());
                    } catch (SQLException e) {
                        log.error("Could not execute the request on row: " + row.toString() + " due to error: ", e);
                    } catch (StringIndexOutOfBoundsException e) {
                        log.warn("This row has not been inserted due to a Hive API row bugs : " + row.toString());
                    }
                });

                hivePreparedStatementByThread.close();
            } catch (SQLException e) {
                log.error("Could not prepare statement for Hive");
            }

            latch.countDown();
        }
    }

    private void prepareAndExecuteStatement(String sqlQuery) {
        try (PreparedStatement preparedStatement = hiveConnection.prepareStatement(sqlQuery)) {
            preparedStatement.execute();
        } catch (SQLException e) {
            log.error("Could not execute request due to error: ", e);
        }
    }


}
