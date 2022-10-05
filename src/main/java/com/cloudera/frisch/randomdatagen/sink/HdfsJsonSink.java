package com.cloudera.frisch.randomdatagen.sink;


import com.cloudera.frisch.randomdatagen.Utils;
import com.cloudera.frisch.randomdatagen.config.ApplicationConfigs;
import com.cloudera.frisch.randomdatagen.config.PropertiesLoader;
import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.OptionsConverter;
import com.cloudera.frisch.randomdatagen.model.Row;
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
 * This is an HDFSJSON sink using Hadoop 3.1 API
 * Each instance manages one connection to a file system and one specific file
 */
@Slf4j
public class HdfsJsonSink implements SinkInterface {

    private FileSystem fileSystem;
    private FSDataOutputStream fsDataOutputStream;
    private int counter;
    private final Model model;
    private final String directoryName;
    private final String fileName;
    private final Boolean oneFilePerIteration;
    private final short replicationFactor;
    private String hdfsUri;

    /**
     * Initiate HDFSJSON connection with Kerberos or not
     * @return filesystem connection to HDFSJSON
     */
    public HdfsJsonSink(Model model, Map<ApplicationConfigs, String> properties) {
        this.directoryName = (String) model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_PATH);
        this.fileName = (String) model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_NAME);
        this.oneFilePerIteration = (Boolean) model.getOptionsOrDefault(OptionsConverter.Options.ONE_FILE_PER_ITERATION);
        this.model = model;
        this.counter = 0;
        this.replicationFactor = (short) model.getOptionsOrDefault(OptionsConverter.Options.HDFS_REPLICATION_FACTOR);
        this.hdfsUri = properties.get(ApplicationConfigs.HDFS_URI);

        Configuration config = new Configuration();
        Utils.setupHadoopEnv(config, properties);

        // Set all kerberos if needed (Note that connection will require a user and its appropriate keytab with right privileges to access folders and files on HDFSCSV)
        if (Boolean.parseBoolean(properties.get(ApplicationConfigs.HDFS_AUTH_KERBEROS))) {
            Utils.loginUserWithKerberos(properties.get(ApplicationConfigs.HDFS_AUTH_KERBEROS_USER),
                properties.get(ApplicationConfigs.HDFS_AUTH_KERBEROS_KEYTAB),config);
        }

        log.debug("Setting up access to HDFSJSON");
        try {
            fileSystem = FileSystem.get(URI.create(hdfsUri), config);
        } catch (IOException e) {
            log.error("Could not access to HDFSJSON !", e);
        }

        Utils.createHdfsDirectory(fileSystem, directoryName);

        if ((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.DELETE_PREVIOUS)) {
            Utils.deleteAllHdfsFiles(fileSystem, directoryName, fileName, "json");
        }

        if (!oneFilePerIteration) {
            createFileWithOverwrite(directoryName + fileName + ".json");
        }

    }

    @Override
    public void terminate() {
        try {
        fsDataOutputStream.close();
        } catch (IOException e) {
            log.error(" Unable to close HDFSJSON file with error :", e);
        }
    }

    @Override
    public void sendOneBatchOfRows(List<Row> rows){
        try {
            if (oneFilePerIteration) {
                createFileWithOverwrite(directoryName + fileName + "-" + String.format("%010d", counter) + ".json");
                counter++;
            }

            List<String> rowsInString = rows.stream().map(Row::toJSON).collect(Collectors.toList());
            fsDataOutputStream.writeChars(String.join(System.getProperty("line.separator"), rowsInString));
            fsDataOutputStream.writeChars(System.getProperty("line.separator"));

            if (oneFilePerIteration) {
                fsDataOutputStream.close();
            }
        } catch (IOException e) {
            log.error("Can not write data to the HDFSJSON file due to error: ", e);
        }
    }

    void createFileWithOverwrite(String path) {
        try {
            Utils.deleteHdfsFile(fileSystem, path);
            fsDataOutputStream = fileSystem.create(new Path(path), replicationFactor);
            log.debug("Successfully created hdfs file : " + path);
        } catch (IOException e) {
            log.error("Tried to create file : " + path + " with no success :", e);
        }
    }

}
