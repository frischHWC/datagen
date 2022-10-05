package com.cloudera.frisch.randomdatagen.sink;


import com.cloudera.frisch.randomdatagen.Utils;
import com.cloudera.frisch.randomdatagen.config.ApplicationConfigs;
import com.cloudera.frisch.randomdatagen.config.PropertiesLoader;
import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.OptionsConverter;
import com.cloudera.frisch.randomdatagen.model.Row;
import com.cloudera.frisch.randomdatagen.model.type.Field;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * This is an ORC HDFS sink using Hadoop 3.2 API
 */
@SuppressWarnings("unchecked")
@Slf4j
public class HdfsOrcSink implements SinkInterface {

    private FileSystem fileSystem;
    private final TypeDescription schema;
    private Writer writer;
    private final Map<String, ColumnVector> vectors;
    private final VectorizedRowBatch batch;
    private int counter;
    private final Model model;
    private final String directoryName;
    private final String fileName;
    private final Boolean oneFilePerIteration;
    private final short replicationFactor;
    private final Configuration conf;
    private String hdfsUri;

    /**
     * Initiate HDFS connection with Kerberos or not
     *
     * @return filesystem connection to HDFS
     */
    public HdfsOrcSink(Model model, Map<ApplicationConfigs, String> properties) {
        this.model = model;
        this.counter = 0;
        this.directoryName = (String) model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_PATH);
        this.fileName = (String) model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_NAME);
        this.oneFilePerIteration = (Boolean) model.getOptionsOrDefault(OptionsConverter.Options.ONE_FILE_PER_ITERATION);
        this.replicationFactor = (short) model.getOptionsOrDefault(OptionsConverter.Options.HDFS_REPLICATION_FACTOR);
        this.conf = new Configuration();
        conf.set("dfs.replication", String.valueOf(replicationFactor));
        this.hdfsUri = properties.get(ApplicationConfigs.HDFS_URI);

        org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
        Utils.setupHadoopEnv(config, properties);

        // Set all kerberos if needed (Note that connection will require a user and its appropriate keytab with right privileges to access folders and files on HDFSCSV)
        if (Boolean.parseBoolean(properties.get(ApplicationConfigs.HDFS_AUTH_KERBEROS))) {
            Utils.loginUserWithKerberos(properties.get(ApplicationConfigs.HDFS_AUTH_KERBEROS_USER),
                properties.get(ApplicationConfigs.HDFS_AUTH_KERBEROS_KEYTAB),config);
        }

        try {
            fileSystem = FileSystem.get(URI.create(hdfsUri), config);
        } catch (IOException e) {
            log.error("Could not access to ORC HDFS !", e);
        }

        this.schema = model.getOrcSchema();
        this.batch = schema.createRowBatch();
        this.vectors = model.createOrcVectors(batch);

        Utils.createHdfsDirectory(fileSystem, directoryName);

        if ((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.DELETE_PREVIOUS)) {
            Utils.deleteAllHdfsFiles(fileSystem, directoryName, fileName, "orc");
        }

        if (!oneFilePerIteration) {
            creatFileWithOverwrite(hdfsUri + directoryName + fileName + ".orc");
        }

    }

    @Override
    public void terminate() {
        try {
            if (!oneFilePerIteration) {
                writer.close();
            }
        } catch (IOException e) {
            log.error(" Unable to close ORC HDFS file with error :", e);
        }
    }

    @Override
    public void sendOneBatchOfRows(List<Row> rows) {
        if (oneFilePerIteration) {
            creatFileWithOverwrite(hdfsUri + directoryName + fileName + "-" + String.format("%010d", counter) + ".orc");
            counter++;
        }

        for (Row row : rows) {
            int rowNumber = batch.size++;
            row.fillinOrcVector(rowNumber, vectors);
            try {
                if (batch.size == batch.getMaxSize()) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
            } catch (IOException e) {
                log.error("Can not write data to the ORC HDFS file due to error: ", e);
            }
        }

        try {
            if (batch.size != 0) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        } catch (IOException e) {
            log.error("Can not write data to the ORC HDFS file due to error: ", e);
        }

        if (oneFilePerIteration) {
            try {
                writer.close();
            } catch (IOException e) {
                log.error(" Unable to close ORC HDFS file with error :", e);
            }
        }

    }

    private void creatFileWithOverwrite(String path) {
        try {
            Utils.deleteHdfsFile(fileSystem, path);
            writer = OrcFile.createWriter(new Path(path),
                OrcFile.writerOptions(conf)
                    .setSchema(schema));
        } catch (IOException e) {
            log.warn("Could not create writer to ORC HDFS file due to error:", e);
        }
    }
}
