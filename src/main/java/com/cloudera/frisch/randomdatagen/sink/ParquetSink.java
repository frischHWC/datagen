package com.cloudera.frisch.randomdatagen.sink;


import com.cloudera.frisch.randomdatagen.Utils;
import com.cloudera.frisch.randomdatagen.config.ApplicationConfigs;
import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.OptionsConverter;
import com.cloudera.frisch.randomdatagen.model.Row;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@Slf4j
public class ParquetSink implements SinkInterface {


    private final Schema schema;
    private ParquetWriter<GenericRecord> writer;
    private int counter;
    private final Model model;
    private final String directoryName;
    private final String fileName;
    private final Boolean oneFilePerIteration;

    /**
     * Init local Parquet file
     */
    ParquetSink(Model model, Map<ApplicationConfigs, String> properties) {
        this.counter = 0;
        this.model = model;
        this.directoryName = (String) model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_PATH);
        this.fileName = (String) model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_NAME);
        this.oneFilePerIteration = (Boolean) model.getOptionsOrDefault(OptionsConverter.Options.ONE_FILE_PER_ITERATION);

        this.schema = model.getAvroSchema();

        Utils.createLocalDirectory(directoryName);

        if ((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.DELETE_PREVIOUS)) {
            Utils.deleteAllLocalFiles(directoryName, fileName , "parquet");
        }

        if (!oneFilePerIteration) {
            createFileWithOverwrite(directoryName + fileName + ".parquet");
        }

    }


    @Override
    public void terminate() {
        try {
            if (!oneFilePerIteration) {
                writer.close();
            }
        } catch (IOException e) {
            log.error(" Unable to close local file with error :", e);
        }
    }

    @Override
    public void sendOneBatchOfRows(List<Row> rows) {
        if (oneFilePerIteration) {
            createFileWithOverwrite( directoryName + fileName + "-" + String.format("%010d", counter) + ".parquet");
            counter++;
        }
        rows.stream().map(row -> row.toGenericRecord(schema)).forEach(genericRecord -> {
            try {
                writer.write(genericRecord);
            } catch (IOException e) {
                log.error("Can not write data to the local file due to error: ", e);
            }
        });
        if (oneFilePerIteration) {
            try {
                writer.close();
            } catch (IOException e) {
                log.error(" Unable to close local file with error :", e);
            }
        }
    }

    private void createFileWithOverwrite(String path) {
        try {
            Utils.deleteLocalFile(path);
            if(!new File(path).getParentFile().mkdirs()) { log.warn("Could not create parent dir");}
            this.writer = AvroParquetWriter
                    .<GenericRecord>builder(new Path(path))
                    .withSchema(schema)
                    .withConf(new Configuration())
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withPageSize((int) model.getOptionsOrDefault(OptionsConverter.Options.PARQUET_PAGE_SIZE))
                    .withDictionaryEncoding((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.PARQUET_DICTIONARY_ENCODING))
                    .withDictionaryPageSize((int) model.getOptionsOrDefault(OptionsConverter.Options.PARQUET_DICTIONARY_PAGE_SIZE))
                    .withRowGroupSize((int) model.getOptionsOrDefault(OptionsConverter.Options.PARQUET_ROW_GROUP_SIZE))
                    .build();
            log.debug("Successfully created local Parquet file : " + path);

        } catch (IOException e) {
            log.error("Tried to create Parquet local file : " + path + " with no success :", e);
        }
    }

}