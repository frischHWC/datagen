package com.cloudera.frisch.randomdatagen.service;

import com.cloudera.frisch.randomdatagen.config.ApplicationConfigs;
import com.cloudera.frisch.randomdatagen.config.SinkParser;
import com.cloudera.frisch.randomdatagen.model.Model;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;


@Setter
@Getter
@Slf4j
public class Command implements Serializable {

  private static final long serialVersionUID = 1L;

  private UUID commandUuid;
  private CommandStatus status;
  private String commandComment;
  private String modelFilePath;
  private Model model;
  private Integer numberOfThreads;
  private Long numberOfBatches;
  private Long rowsPerBatch;
  private Boolean scheduled;
  private Long delayBetweenExecutions;
  private List<SinkParser.Sink> sinksListAsString;
  private Map<ApplicationConfigs, String> properties;
  private Long durationSeconds;
  private Long lastFinishedTimestamp;
  private double progress;


  private void writeObject(ObjectOutputStream oos) throws IOException {
    oos.writeObject(commandUuid);
    oos.writeObject(status);
    oos.writeObject(commandComment);
    oos.writeObject(modelFilePath);
    oos.writeObject(numberOfThreads);
    oos.writeObject(numberOfBatches);
    oos.writeObject(rowsPerBatch);
    oos.writeObject(scheduled);
    oos.writeObject(delayBetweenExecutions);
    oos.writeObject(sinksListAsString);
    oos.writeObject(properties);
    oos.writeObject(durationSeconds);
    oos.writeObject(lastFinishedTimestamp);
    oos.writeObject(progress);
  }

  private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
    this.commandUuid = (UUID) ois.readObject();
    this.status = (CommandStatus) ois.readObject();
    this.commandComment = (String) ois.readObject();
    this.modelFilePath = (String) ois.readObject();
    this.numberOfThreads = (Integer) ois.readObject();
    this.numberOfBatches = (Long) ois.readObject();
    this.rowsPerBatch = (Long) ois.readObject();
    this.scheduled = (Boolean) ois.readObject();
    this.delayBetweenExecutions = (Long) ois.readObject();
    this.sinksListAsString = (List<SinkParser.Sink>) ois.readObject();
    this.properties = (Map<ApplicationConfigs, String>) ois.readObject();
    this.durationSeconds = (Long) ois.readObject();
    this.lastFinishedTimestamp = (Long) ois.readObject();
    this.progress = (double) ois.readObject();
  }

  @Override
  public String toString() {
    StringBuffer sinkList = new StringBuffer();
    sinksListAsString.forEach(s -> {sinkList.append(s) ; sinkList.append(" ; ");});

    StringBuffer propertiesAsString = new StringBuffer();
    properties.forEach((config, value) -> {
      propertiesAsString.append(config);
      propertiesAsString.append(" -> ");
      String valueEscaped = value.replaceAll("\"", "\\\"");
      propertiesAsString.append(valueEscaped);
      propertiesAsString.append(" ; ");
    });

    return "{ " +
        "\"uuid\": \"" + commandUuid.toString() + "\"" +
        " , \"status\": \"" + status.toString() + "\"" +
        " , \"duration\": \"" + durationSeconds + "\"" +
        " , \"progress\": \"" + progress + "\"" +
        " , \"comment\": \"" + commandComment + "\"" +
        " , \"model_file\": \"" + modelFilePath + "\"" +
        " , \"number_of_batches\": \"" + numberOfBatches + "\"" +
        " , \"rows_per_batch\": \"" + rowsPerBatch + "\"" +
        " , \"scheduled\": \"" + scheduled + "\"" +
        " , \"delay_between_executions\": \"" + delayBetweenExecutions + "\"" +
        " , \"last_finished_timestamp\": \"" + lastFinishedTimestamp + "\"" +
        " , \"sinks\": \"" + sinkList + "\"" +
        " , \"extra_properties\": \"" + propertiesAsString + "\"" +
        " }";
  }

  public Command(String modelFilePath,
                 Model model,
                 Integer numberOfThreads,
                 Long numberOfBatches,
                 Long rowsPerBatch,
                 Boolean scheduled,
                 Long delayBewtweenExecutions,
                 List<SinkParser.Sink> sinksListAsString,
                 Map<ApplicationConfigs, String> properties) {
    this.commandUuid = UUID.randomUUID();
    this.status = CommandStatus.QUEUED;
    this.commandComment = "";
    this.model = model;
    this.modelFilePath = modelFilePath;
    this.numberOfThreads = numberOfThreads;
    this.numberOfBatches = numberOfBatches;
    this.rowsPerBatch = rowsPerBatch;
    this.scheduled = scheduled;
    this.delayBetweenExecutions = delayBewtweenExecutions;
    this.lastFinishedTimestamp = 0L;
    this.sinksListAsString = sinksListAsString;
    this.properties = properties;
    this.durationSeconds = 0L;
    this.progress = 0f;
  }


  public enum CommandStatus {
    QUEUED,
    STARTED,
    RUNNING,
    FINISHED,
    FAILED
  }

}
