package com.cloudera.frisch.randomdatagen.sink;


import com.cloudera.frisch.randomdatagen.Utils;
import com.cloudera.frisch.randomdatagen.config.ApplicationConfigs;
import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.OptionsConverter;
import com.cloudera.frisch.randomdatagen.model.Row;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This is a JSON sink
 * Its goal is to write into ONE single json file data randomly generated
 */
@Slf4j
public class JsonSink implements SinkInterface {

    private FileOutputStream outputStream;
    private int counter;
    private final Model model;
    private final String directoryName;
    private final String fileName;
    private final Boolean oneFilePerIteration;
    private final String lineSeparator;

    /**
     * Init local JSON file
     */
    public JsonSink(Model model, Map<ApplicationConfigs, String> properties) {
        this.directoryName = (String) model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_PATH);
        this.fileName = (String) model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_NAME);
        this.oneFilePerIteration = (Boolean) model.getOptionsOrDefault(OptionsConverter.Options.ONE_FILE_PER_ITERATION);
        this.model = model;
        this.counter = 0;
        this.lineSeparator = System.getProperty("line.separator");

        Utils.createLocalDirectory(directoryName);

        if ((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.DELETE_PREVIOUS)) {
            Utils.deleteAllLocalFiles(directoryName, fileName , "json");
        }

        if (!oneFilePerIteration) {
            createFileWithOverwrite(directoryName + fileName + ".json");
        }
    }

    @Override
    public void terminate() {
        try {
            if (!oneFilePerIteration) {
                outputStream.close();
            }
        } catch (IOException e) {
            log.error(" Unable to close local file with error :", e);
        }
    }

    @Override
    public void sendOneBatchOfRows(List<Row> rows) {
        try {
            if (oneFilePerIteration) {
                createFileWithOverwrite(directoryName + fileName + "-" + String.format("%010d", counter) + ".json");
                counter++;
            }

            rows.stream().map(Row::toJSON).forEach(r -> {
                try {
                    outputStream.write(r.getBytes());
                    outputStream.write(lineSeparator.getBytes());
                } catch (IOException e) {
                    log.error("Could not write row: " + r + " to file: " + outputStream.getChannel());
                }
            });
            outputStream.write(lineSeparator.getBytes());

            if (oneFilePerIteration) {
                outputStream.close();
            }
        } catch (IOException e) {
            log.error("Can not write data to the local file due to error: ", e);
        }
    }

    void createFileWithOverwrite(String path) {
        try {
            File file = new File(path);
            if(!file.getParentFile().mkdirs()) { log.warn("Could not create parent dir of {}", path);}
            if(!file.createNewFile()) { log.warn("Could not create file: {}", path);}
            outputStream = new FileOutputStream(path, false);
            log.debug("Successfully created local file : " + path);
        } catch (IOException e) {
            log.error("Tried to create file : " + path + " with no success :", e);
        }
    }


}
