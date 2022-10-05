package com.cloudera.frisch.randomdatagen.sink;

import com.cloudera.frisch.randomdatagen.Utils;
import com.cloudera.frisch.randomdatagen.config.ApplicationConfigs;
import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.OptionsConverter;
import com.cloudera.frisch.randomdatagen.model.Row;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Avro Sink to create Local Avro files
 */
@Slf4j
public class AvroSink implements SinkInterface {

    private File file;
    private final Schema schema;
    private DataFileWriter<GenericRecord> dataFileWriter;
    private final DatumWriter<GenericRecord> datumWriter;
    private int counter;
    private final Model model;
    private final String directoryName;
    private final String fileName;
    private final Boolean oneFilePerIteration;

    /**
     * Init local Avro file with header
     */
    AvroSink(Model model, Map<ApplicationConfigs, String> properties) {
        this.schema = model.getAvroSchema();
        this.datumWriter = new GenericDatumWriter<>(schema);
        this.model = model;
        this.counter = 0;
        this.directoryName = (String) model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_PATH);
        this.fileName = (String) model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_NAME);
        this.oneFilePerIteration = (Boolean) model.getOptionsOrDefault(OptionsConverter.Options.ONE_FILE_PER_ITERATION);

        Utils.createLocalDirectory(directoryName);

        if ((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.DELETE_PREVIOUS)) {
            Utils.deleteAllLocalFiles(directoryName, fileName , "avro");
        }

        if (!oneFilePerIteration) {
            createFileWithOverwrite(directoryName + fileName + ".avro");
            appendAvscHeader();
        }
    }

    @Override
    public void terminate() {
        try {
            if (!oneFilePerIteration) {
                dataFileWriter.close();
            }
        } catch (IOException e) {
            log.error(" Unable to close local file with error :", e);
        }
    }

    @Override
    public void sendOneBatchOfRows(List<Row> rows) {

        if (oneFilePerIteration) {
            createFileWithOverwrite(directoryName + fileName + "-" + String.format("%010d", counter) + ".avro");
            appendAvscHeader();
            counter++;
        }

        rows.stream().map(row -> row.toGenericRecord(schema)).forEach(genericRecord -> {
            try {
                dataFileWriter.append(genericRecord);
            } catch (IOException e) {
                log.error("Can not write data to the local file due to error: ", e);
            }
        });

        if (oneFilePerIteration) {
            try {
                dataFileWriter.close();
            } catch (IOException e) {
                log.error(" Unable to close local file with error :", e);
            }
        } else {
            try {
                dataFileWriter.flush();
            } catch (IOException e) {
                log.error(" Unable to flush local file with error :", e);
            }
        }
    }

    void createFileWithOverwrite(String path) {
        try {
            file = new File(path);
            if(!file.getParentFile().mkdirs()) { log.warn("Could not create parent dir");}
            if(!file.createNewFile()) { log.warn("Could not create file");}
            dataFileWriter = new DataFileWriter<>(datumWriter);
            log.debug("Successfully created local file : " + path);
        } catch (IOException e) {
            log.error("Tried to create file : " + path + " with no success :", e);
        }
    }

    void appendAvscHeader() {
        try {
            dataFileWriter.create(schema, file);
        } catch (IOException e) {
            log.error("Can not write header to the local file due to error: ", e);
        }
    }
}
