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
package com.cloudera.frisch.datagen.service;

import com.cloudera.frisch.datagen.config.ApplicationConfigs;
import com.cloudera.frisch.datagen.config.ConnectorParser;
import com.cloudera.frisch.datagen.model.Model;
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
  private List<ConnectorParser.Connector> connectorsListAsString;
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
    oos.writeObject(connectorsListAsString);
    oos.writeObject(properties);
    oos.writeObject(durationSeconds);
    oos.writeObject(lastFinishedTimestamp);
    oos.writeObject(progress);
  }

  private void readObject(ObjectInputStream ois)
      throws ClassNotFoundException, IOException {
    this.commandUuid = (UUID) ois.readObject();
    this.status = (CommandStatus) ois.readObject();
    this.commandComment = (String) ois.readObject();
    this.modelFilePath = (String) ois.readObject();
    this.numberOfThreads = (Integer) ois.readObject();
    this.numberOfBatches = (Long) ois.readObject();
    this.rowsPerBatch = (Long) ois.readObject();
    this.scheduled = (Boolean) ois.readObject();
    this.delayBetweenExecutions = (Long) ois.readObject();
    this.connectorsListAsString = (List<ConnectorParser.Connector>) ois.readObject();
    this.properties = (Map<ApplicationConfigs, String>) ois.readObject();
    this.durationSeconds = (Long) ois.readObject();
    this.lastFinishedTimestamp = (Long) ois.readObject();
    this.progress = (double) ois.readObject();
  }

  @Override
  public String toString() {
    StringBuffer connectorList = new StringBuffer();
    connectorsListAsString.forEach(s -> {
      connectorList.append(s);
      connectorList.append(" ; ");
    });

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
        " , \"connectors\": \"" + connectorList + "\"" +
        " , \"extra_properties\": \"" + propertiesAsString + "\"" +
        " }";
  }

  public Command(String modelFilePath,
                 Model model,
                 Integer numberOfThreads,
                 Long numberOfBatches,
                 Long rowsPerBatch,
                 Boolean scheduled,
                 Long delayBetweenExecutions,
                 List<ConnectorParser.Connector> connectorsListAsString,
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
    this.delayBetweenExecutions = delayBetweenExecutions;
    this.lastFinishedTimestamp = 0L;
    this.connectorsListAsString = connectorsListAsString;
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
