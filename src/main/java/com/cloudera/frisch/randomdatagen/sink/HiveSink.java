package com.cloudera.frisch.randomdatagen.sink;

import com.cloudera.frisch.randomdatagen.Utils;
import com.cloudera.frisch.randomdatagen.config.ApplicationConfigs;
import com.cloudera.frisch.randomdatagen.config.PropertiesLoader;
import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.OptionsConverter;
import com.cloudera.frisch.randomdatagen.model.Row;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.jdbc.HivePreparedStatement;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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

        Utils.setupHadoopEnv(new org.apache.hadoop.conf.Configuration(), properties);

        try {
            if (Boolean.parseBoolean(properties.get(ApplicationConfigs.HIVE_AUTH_KERBEROS))) {
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


            String hiveUriWithNoDatabase = "jdbc:hive2://" + properties.get(ApplicationConfigs.HIVE_ZK_QUORUM) +
                "/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=" + properties.get(ApplicationConfigs.HIVE_ZK_ZNODE) +
                "?tez.queue.name=" + queue ;

            this.hiveConnection = DriverManager.getConnection(hiveUriWithNoDatabase, propertiesForHive);

            prepareAndExecuteStatement("CREATE DATABASE IF NOT EXISTS " + database);

            hiveConnection.setSchema(database);

            if ((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.DELETE_PREVIOUS)) {
                prepareAndExecuteStatement("DROP TABLE IF EXISTS " + tableName);
            }

            log.info("SQL schema for hive: " + model.getSQLSchema());
            prepareAndExecuteStatement("CREATE TABLE IF NOT EXISTS " + tableName + model.getSQLSchema());

            if (hiveOnHDFS) {
                // If using an HDFS sink, we want it to use the Hive HDFS File path and not the Hdfs file path
                model.getTableNames().put(OptionsConverter.TableNames.HDFS_FILE_PATH,
                    model.getTableNames().get(OptionsConverter.TableNames.HIVE_HDFS_FILE_PATH));
                this.hdfsSink = new HdfsParquetSink(model, properties);

                log.info("Creating temporary table: " + tableNameTemporary);
                prepareAndExecuteStatement(
                        "CREATE EXTERNAL TABLE IF NOT EXISTS " + tableNameTemporary + model.getSQLSchema() +
                                " STORED AS PARQUET " +
                                " LOCATION '" + locationTemporaryTable + "'" //+
                );
            }

            log.info("SQL Insert schema for hive: " + model.getInsertSQLStatement());
            insertStatement = "INSERT INTO " + tableName + model.getInsertSQLStatement();

        } catch (SQLException e) {
            log.error("Could not connect to HS2 and create table due to error: ", e);
        }
    }

    @Override
    public void terminate() {
        try {
            if (hiveOnHDFS) {
                log.info("Starting to load data to final table");
                prepareAndExecuteStatement("INSERT INTO " + tableName +
                        " SELECT * FROM " + tableNameTemporary);
            }

            hiveConnection.close();

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
