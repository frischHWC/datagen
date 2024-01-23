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
package com.cloudera.frisch.datagen.sink.storage.hdfs;


import com.cloudera.frisch.datagen.sink.SinkInterface;
import com.cloudera.frisch.datagen.utils.Utils;
import com.cloudera.frisch.datagen.config.ApplicationConfigs;
import com.cloudera.frisch.datagen.model.Model;
import com.cloudera.frisch.datagen.model.OptionsConverter;
import com.cloudera.frisch.datagen.model.Row;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This is an HDFSCSV sink using Hadoop 3.2 API
 * Each instance manages one connection to a file system
 */
@Slf4j
public class HdfsCsvSink implements SinkInterface {

    private FileSystem fileSystem;
    private FSDataOutputStream fsDataOutputStream;
    private int counter;
    private final Model model;
    private final String directoryName;
    private final String fileName;
    private final Boolean oneFilePerIteration;
    private final short replicationFactor;
    private String hdfsUri;
    private Boolean useKerberos;

    /**
     * Initiate HDFSCSV connection with Kerberos or not
     */
    public HdfsCsvSink(Model model, Map<ApplicationConfigs, String> properties) {
        // If using an HDFS sink, we want it to use the Hive HDFS File path and not the Hdfs file path
        if(properties.get(ApplicationConfigs.HDFS_FOR_HIVE)!=null
            && properties.get(ApplicationConfigs.HDFS_FOR_HIVE).equalsIgnoreCase("true")) {
            this.directoryName = (String) model.getTableNames()
                .get(OptionsConverter.TableNames.HIVE_HDFS_FILE_PATH);
        } else {
            this.directoryName = (String) model.getTableNames()
                .get(OptionsConverter.TableNames.HDFS_FILE_PATH);
        }
        log.debug("HDFS sink will generates data into HDFS directory: "+ this.directoryName);
        this.counter = 0;
        this.model = model;
        this.fileName = (String) model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_NAME);
        this.oneFilePerIteration = (Boolean) model.getOptionsOrDefault(OptionsConverter.Options.ONE_FILE_PER_ITERATION);
        this.replicationFactor = (short) model.getOptionsOrDefault(OptionsConverter.Options.HDFS_REPLICATION_FACTOR);
        this.hdfsUri = properties.get(ApplicationConfigs.HDFS_URI);
        this.useKerberos = Boolean.parseBoolean(properties.get(ApplicationConfigs.HDFS_AUTH_KERBEROS));

        Configuration config = new Configuration();
        Utils.setupHadoopEnv(config, properties);

        // Set all kerberos if needed (Note that connection will require a user and its appropriate keytab with right privileges to access folders and files on HDFSCSV)
        if (useKerberos) {
            Utils.loginUserWithKerberos(properties.get(ApplicationConfigs.HDFS_AUTH_KERBEROS_USER),
                properties.get(ApplicationConfigs.HDFS_AUTH_KERBEROS_KEYTAB),config);
        }

        try {
            fileSystem = FileSystem.get(URI.create(hdfsUri), config);
        } catch (IOException e) {
            log.error("Could not access to HDFSCSV !", e);
        }

        Utils.createHdfsDirectory(fileSystem, directoryName);

        if ((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.DELETE_PREVIOUS)) {
            Utils.deleteAllHdfsFiles(fileSystem, directoryName, fileName, "csv");
        }

        if (!oneFilePerIteration) {
            createFileWithOverwrite(directoryName + fileName + ".csv");
            appendCSVHeader(model);
        }

    }

    @Override
    public void terminate() {
        try {
            fsDataOutputStream.close();
            if(useKerberos) {
                Utils.logoutUserWithKerberos();
            }
        } catch (IOException e) {
            log.error(" Unable to close HDFSCSV file with error :", e);
        }
    }

    @Override
    public void sendOneBatchOfRows(List<Row> rows){
        try {
            if (oneFilePerIteration) {
                createFileWithOverwrite(directoryName + fileName + "-" + String.format("%010d", counter) + ".csv");
                appendCSVHeader(model);
                counter++;
            }

            List<String> rowsInString = rows.stream().map(Row::toCSV).collect(Collectors.toList());
            fsDataOutputStream.writeChars(String.join(System.getProperty("line.separator"), rowsInString));
            fsDataOutputStream.writeChars(System.getProperty("line.separator"));

            if (oneFilePerIteration) {
                fsDataOutputStream.close();
            }
        } catch (IOException e) {
            log.error("Can not write data to the HDFSCSV file due to error: ", e);
        }
    }

    void appendCSVHeader(Model model) {
        try {
            if ((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.CSV_HEADER)) {
                fsDataOutputStream.writeChars(model.getCsvHeader());
                fsDataOutputStream.writeChars(
                    System.getProperty("line.separator"));
            }
        } catch (IOException e) {
            log.error("Can not write header to the hdfs file due to error: ", e);
        }
    }

    void createFileWithOverwrite(String path) {
        try {
            Utils.deleteHdfsFile(fileSystem, path);
            fsDataOutputStream = fileSystem.create(new Path(path), replicationFactor);
            log.debug("Successfully created hdfs file : " + path);
        } catch (IOException e) {
            log.error("Tried to create hdfs file : " + path + " with no success :", e);
        }
    }

}
