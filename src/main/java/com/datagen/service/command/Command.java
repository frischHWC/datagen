/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datagen.service.command;

import com.datagen.config.ApplicationConfigs;
import com.datagen.config.ConnectorParser;
import com.datagen.model.Model;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.UUID;


@Setter
@Getter
@Slf4j
@NoArgsConstructor
public class Command implements Serializable {

  @Serial
  private static final long serialVersionUID = 1L;

  private UUID commandUuid;
  private CommandStatus status;
  private String commandError;
  private String modelId;
  private String modelFilePath;
  private String error;
  private String owner;
  @JsonIgnore
  private Model model;
  private Integer numberOfThreads;
  private Long numberOfBatches;
  private Long rowsPerBatch;
  private Boolean scheduled;
  private Long delayBetweenExecutions;
  private List<ConnectorParser.Connector> connectorsList;
  private Map<ApplicationConfigs, String> properties;
  private Long durationMilliSeconds;
  private Long lastFinishedTimestamp;
  private Long lastStartedTimestamp;
  private double progress;


  private void writeObject(ObjectOutputStream oos) throws IOException {
    oos.writeObject(commandUuid);
    oos.writeObject(status);
    oos.writeObject(commandError);
    oos.writeObject(modelId);
    oos.writeObject(modelFilePath);
    oos.writeObject(owner);
    oos.writeObject(numberOfThreads);
    oos.writeObject(numberOfBatches);
    oos.writeObject(rowsPerBatch);
    oos.writeObject(scheduled);
    oos.writeObject(delayBetweenExecutions);
    oos.writeObject(connectorsList);
    oos.writeObject(properties);
    oos.writeObject(durationMilliSeconds);
    oos.writeObject(lastFinishedTimestamp);
    oos.writeObject(lastStartedTimestamp);
    oos.writeObject(progress);
  }

  private void readObject(ObjectInputStream ois)
      throws ClassNotFoundException, IOException {
    this.commandUuid = (UUID) ois.readObject();
    this.status = (CommandStatus) ois.readObject();
    this.commandError = (String) ois.readObject();
    this.modelId = (String) ois.readObject();
    this.modelFilePath = (String) ois.readObject();
    this.owner = (String) ois.readObject();
    this.numberOfThreads = (Integer) ois.readObject();
    this.numberOfBatches = (Long) ois.readObject();
    this.rowsPerBatch = (Long) ois.readObject();
    this.scheduled = (Boolean) ois.readObject();
    this.delayBetweenExecutions = (Long) ois.readObject();
    this.connectorsList = (List<ConnectorParser.Connector>) ois.readObject();
    this.properties = (Map<ApplicationConfigs, String>) ois.readObject();
    this.durationMilliSeconds = (Long) ois.readObject();
    this.lastFinishedTimestamp = (Long) ois.readObject();
    this.lastStartedTimestamp = (Long) ois.readObject();
    this.progress = (double) ois.readObject();
  }

  public void writeCommandAsJSON(OutputStream outputStream) {
    var writer = new ObjectMapper().writer().withDefaultPrettyPrinter();
    try {
      writer.writeValue(outputStream, this);
    } catch (Exception e) {
      log.warn("Could not write command to output");
    }
  }

  public void writeCommandAsJSON(String path) {
    try {
      writeCommandAsJSON(new FileOutputStream(path));
    } catch (Exception e) {
      log.warn("Could not write command to file: {}", path);
    }
  }

  public static Command readCommandFromJSON(InputStream inputStream) {
    var reader = new ObjectMapper().reader();
    try {
      return reader.readValue(inputStream, Command.class);
    } catch (Exception e) {
      log.warn("Could not read command from input due to error", e);
    }
    return null;
  }

  @Override
  public String toString() {
    var writer = new ObjectMapper().writer().withDefaultPrettyPrinter();
    try {
      return writer.writeValueAsString(this);
    } catch (Exception e) {
      log.warn("Could not write command as JSON");
    }
    // Do a manual json
    StringBuffer connectorList = new StringBuffer();
    connectorsList.forEach(s -> {
      connectorList.append(s);
      connectorList.append(" ; ");
    });

    StringBuffer propertiesAsString = new StringBuffer();
    properties.forEach((config, value) -> {
      propertiesAsString.append("\"");
      propertiesAsString.append(config);
      propertiesAsString.append("\"");
      propertiesAsString.append(":");
      propertiesAsString.append("\"");
      String valueEscaped = value.replaceAll("\"", "\\\"");
      propertiesAsString.append(valueEscaped);
      propertiesAsString.append("\"");
      propertiesAsString.append(",");
      propertiesAsString.append(System.lineSeparator());
    });
    propertiesAsString.deleteCharAt(propertiesAsString.lastIndexOf(","));

    return "{ " +
        "\"uuid\": \"" + commandUuid.toString() + "\"," + System.lineSeparator() +
        "\"status\": \"" + status.toString() + "\"," + System.lineSeparator() +
        "\"duration_ms\": \"" + durationMilliSeconds + "\"," + System.lineSeparator() +
        "\"progress\": \"" + progress + "\"," + System.lineSeparator() +
        "\"error\": \"" + commandError + "\"," + System.lineSeparator() +
        "\"owner\": \"" + owner + "\"," + System.lineSeparator() +
        "\"modelId\": \"" + modelId + "\"," + System.lineSeparator() +
        "\"model_file\": \"" + modelFilePath + "\"," + System.lineSeparator() +
        "\"number_of_batches\": \"" + numberOfBatches + "\"," + System.lineSeparator() +
        "\"rows_per_batch\": \"" + rowsPerBatch + "\"," + System.lineSeparator() +
        "\"scheduled\": \"" + scheduled + "\"," + System.lineSeparator() +
        "\"delay_between_executions\": \"" + delayBetweenExecutions + "\"," + System.lineSeparator() +
        "\"last_finished_timestamp\": \"" + lastFinishedTimestamp + "\"," + System.lineSeparator() +
        "\"last_started_timestamp\": \"" + lastStartedTimestamp + "\"," + System.lineSeparator() +
        "\"connectors\": \"" + connectorList + "\"," + System.lineSeparator() +
        "\"extra_properties\": [ " + propertiesAsString + " ] " + System.lineSeparator() +
        " }";
  }

  public String toMinimalString() {
    // Do a manual json
    StringBuffer connectorList = new StringBuffer();
    connectorsList.forEach(s -> {
      connectorList.append(s);
      connectorList.append(" ; ");
    });

    return "{ " + System.lineSeparator() +
        "\"uuid\": \"" + commandUuid.toString() + "\"," + System.lineSeparator() +
        "\"status\": \"" + status.toString() + "\"," + System.lineSeparator() +
        "\"duration_ms\": \"" + durationMilliSeconds + "\"," + System.lineSeparator() +
        "\"progress\": \"" + progress + "\"," + System.lineSeparator() +
        "\"error\": \"" + commandError + "\"," + System.lineSeparator() +
        "\"modelId\": \"" + modelId + "\"," + System.lineSeparator() +
        "\"model_file\": \"" + modelFilePath + "\"," + System.lineSeparator() +
        "\"number_of_batches\": \"" + numberOfBatches + "\"," + System.lineSeparator() +
        "\"rows_per_batch\": \"" + rowsPerBatch + "\"," + System.lineSeparator() +
        "\"scheduled\": \"" + scheduled + "\"," + System.lineSeparator() +
        "\"delay_between_executions\": \"" + delayBetweenExecutions + "\"," + System.lineSeparator() +
        "\"last_finished_timestamp\": \"" + lastFinishedTimestamp + "\"," + System.lineSeparator() +
        "\"last_started_timestamp\": \"" + lastFinishedTimestamp + "\"," + System.lineSeparator() +
        "\"connectors\": \"" + connectorList + "\"" + System.lineSeparator() +
        " }";
  }

  public Command(String modelFilePath,
                 Model model,
                 String owner,
                 Integer numberOfThreads,
                 Long numberOfBatches,
                 Long rowsPerBatch,
                 Boolean scheduled,
                 Long delayBetweenExecutions,
                 List<ConnectorParser.Connector> connectorsList,
                 Map<ApplicationConfigs, String> properties) {
    this.commandUuid = UUID.randomUUID();
    this.status = CommandStatus.QUEUED;
    this.owner = owner == null || owner.isEmpty() ? "anonymous":owner;
    this.commandError = "";
    this.model = model;
    this.modelId = model.getName();
    this.modelFilePath = modelFilePath;
    this.numberOfThreads = numberOfThreads;
    this.numberOfBatches = numberOfBatches;
    this.rowsPerBatch = rowsPerBatch;
    this.scheduled = scheduled;
    this.delayBetweenExecutions = delayBetweenExecutions;
    this.lastFinishedTimestamp = 0L;
    this.lastStartedTimestamp = System.currentTimeMillis();
    this.connectorsList = connectorsList;
    this.properties = properties;
    this.durationMilliSeconds = 0L;
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
