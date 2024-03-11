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
import com.cloudera.frisch.datagen.config.PropertiesLoader;
import com.cloudera.frisch.datagen.config.ConnectorParser;
import com.cloudera.frisch.datagen.model.Model;
import com.cloudera.frisch.datagen.model.Row;
import com.cloudera.frisch.datagen.parsers.JsonParser;
import com.cloudera.frisch.datagen.connector.ConnectorInterface;
import com.cloudera.frisch.datagen.connector.ConnectorsUtils;
import com.cloudera.frisch.datagen.utils.Utils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Nullable;
import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;


@Service
@Slf4j
public class CommandRunnerService {

  private PropertiesLoader propertiesLoader;

  @Autowired
  private MetricsService metricsService;

  private final Map<UUID, Command> commands;
  private final ConcurrentLinkedQueue<Command> commandsToProcess;
  private final Map<UUID, Command> scheduledCommands;
  private final String scheduledCommandsFilePath;

  @Autowired
  public CommandRunnerService(PropertiesLoader propertiesLoader) {
    this.propertiesLoader = propertiesLoader;
    this.scheduledCommandsFilePath = propertiesLoader.getPropertiesCopy()
        .get(ApplicationConfigs.SCHEDULER_FILE_PATH);
    this.commandsToProcess = new ConcurrentLinkedQueue<>();
    this.scheduledCommands = new HashMap<>();
    this.commands = new HashMap<>();

    readScheduledCommands();
    // After reading scheduled values, file should be re-written
    writeScheduledCommands();

    Utils.createLocalDirectory(propertiesLoader.getPropertiesCopy()
        .get(ApplicationConfigs.DATA_MODEL_RECEIVED_PATH));
  }

  public CommandSoft getCommandStatusShort(UUID uuid) {
    return new CommandSoft(commands.get(uuid));
  }

  public CommandSoft getScheduledCommandStatusShort(UUID uuid) {
    return new CommandSoft(scheduledCommands.get(uuid));
  }

  public String getCommandAsString(UUID uuid) {
    Command command = commands.get(uuid);
    return command != null ? command.toString() : "Not Found";
  }

  public List<CommandSoft> getAllCommands() {
    List<CommandSoft> commandsAsList = new ArrayList<>();
    commands.forEach((u, c) -> commandsAsList.add(
        getCommandStatusShort(c.getCommandUuid())));
    return commandsAsList;
  }

  public List<CommandSoft> getCommandsByStatus(Command.CommandStatus status) {
    List<CommandSoft> commandsAsList = new ArrayList<>();
    commands.forEach((u, c) -> {
      if (c.getStatus() == status) {
        commandsAsList.add(getCommandStatusShort(c.getCommandUuid()));
      }
    });
    return commandsAsList;
  }

  public List<CommandSoft> getAllScheduledCommands() {
    List<CommandSoft> commandsAsList = new ArrayList<>();
    scheduledCommands.forEach((u, c) -> commandsAsList.add(
        getScheduledCommandStatusShort(c.getCommandUuid())));
    return commandsAsList;
  }

  public void removeScheduledCommands(UUID uuid) {
    synchronized (scheduledCommands) {
      scheduledCommands.remove(uuid);
    }
    writeScheduledCommands();
    log.info("Remove command from scheduler: {}", uuid);
  }

  public void writeScheduledCommands() {
    try {
      log.info("Starting to write all scheduled commands to scheduler file");

      String scheduledCommandsFilepathTemp = scheduledCommandsFilePath + "_tmp";

      Utils.deleteLocalFile(scheduledCommandsFilepathTemp);
      File scheduledCommandTempFile = new File(scheduledCommandsFilepathTemp);
      scheduledCommandTempFile.getParentFile().mkdirs();
      scheduledCommandTempFile.createNewFile();

      FileOutputStream f = new FileOutputStream(scheduledCommandTempFile);
      ObjectOutputStream o = new ObjectOutputStream(f);

      synchronized (scheduledCommands) {
        o.writeObject(scheduledCommands);

        o.close();
        f.close();

        scheduledCommandTempFile.renameTo(new File(scheduledCommandsFilePath));
      }

      log.info("Finished to write all scheduled commands to scheduler file");

      Utils.deleteLocalFile(scheduledCommandsFilepathTemp);

    } catch (Exception e) {
      log.error("Could not write scheduled commands to local file, error is: ",
          e);
    }
  }

  public void readScheduledCommands() {
    try {
      log.info("Starting to read all scheduled commands to scheduler file");

      File scheduledCommandsFile = new File(scheduledCommandsFilePath);
      scheduledCommandsFile.getParentFile().mkdirs();
      scheduledCommandsFile.createNewFile();

      if (scheduledCommandsFile.length() > 0) {
        FileInputStream fi = new FileInputStream(scheduledCommandsFile);
        ObjectInputStream oi = new ObjectInputStream(fi);

        Map<UUID, Command> scheduledCommandsRead =
            (Map<UUID, Command>) oi.readObject();

        // Each time we read commands again, we need to recompute the model as this one is not serialized
        List<UUID> wrongScheduledCommandsRead = new ArrayList<>();
        scheduledCommandsRead.values().stream().forEach(c -> {
          JsonParser parser = new JsonParser(c.getModelFilePath());
          if (parser.getRoot() == null) {
            log.warn("Error when parsing model file");
            c.setCommandComment(
                "Model has not been found or is incorrect, correct it. This command has been removed from scheduler");
            wrongScheduledCommandsRead.add(c.getCommandUuid());
          }
          c.setModel(parser.renderModelFromFile());

          // Previous Failed commands should not be taken
          if (c.getStatus() == Command.CommandStatus.FAILED) {
            wrongScheduledCommandsRead.add(c.getCommandUuid());
          }
          // If commands were stopped in the middle, their status should be rest
          if (c.getStatus() == Command.CommandStatus.QUEUED ||
              c.getStatus() == Command.CommandStatus.STARTED) {
            c.setStatus(Command.CommandStatus.FINISHED);
          }
        });

        wrongScheduledCommandsRead.forEach(scheduledCommandsRead::remove);

        synchronized (scheduledCommands) {
          scheduledCommands.putAll(scheduledCommandsRead);
        }

        oi.close();
        fi.close();

      }

      log.info("Finished to read all scheduled commands to scheduler file");

    } catch (Exception e) {
      log.error("Could not read scheduled commands to local file, error is: ",
          e);
    }

  }

  /**
   * Create a command by solving all properties, model and all empty vars and queue the command to be processed
   * @param modelFilePath
   * @param numberOfThreads
   * @param numberOfBatches
   * @param rowsPerBatch
   * @param connectorsListAsString
   * @param extraProperties
   */
  public String generateData(
      @Nullable MultipartFile modelFileAsFile,
      @Nullable String modelFilePath,
      @Nullable Integer numberOfThreads,
      @Nullable Long numberOfBatches,
      @Nullable Long rowsPerBatch,
      @Nullable Boolean scheduledReceived,
      @Nullable Long delayBetweenExecutionsReceived,
      List<String> connectorsListAsString,
      @Nullable Map<ApplicationConfigs, String> extraProperties) {

    // Get default values if some are not set
    Map<ApplicationConfigs, String> properties =
        propertiesLoader.getPropertiesCopy();

    if (extraProperties != null && !extraProperties.isEmpty()) {
      log.info(
          "Found extra properties sent with the call, these will replace defaults ones");
      properties.putAll(extraProperties);
    }

    int threads = 1;
    if (numberOfThreads != null) {
      threads = numberOfThreads;
    } else if (properties.get(ApplicationConfigs.THREADS) != null) {
      threads = Integer.parseInt(properties.get(ApplicationConfigs.THREADS));
    }
    log.info("Will run generation using {} thread(s)", threads);

    Long batches = 1L;
    if (numberOfBatches != null) {
      batches = numberOfBatches;
    } else if (properties.get(ApplicationConfigs.NUMBER_OF_BATCHES_DEFAULT) !=
        null) {
      batches = Long.valueOf(
          properties.get(ApplicationConfigs.NUMBER_OF_BATCHES_DEFAULT));
    }
    log.info("Will run generation for {} batches", batches);

    Long rows = 1L;
    if (rowsPerBatch != null) {
      rows = rowsPerBatch;
    } else if (properties.get(ApplicationConfigs.NUMBER_OF_ROWS_DEFAULT) !=
        null) {
      rows = Long.valueOf(
          properties.get(ApplicationConfigs.NUMBER_OF_ROWS_DEFAULT));
    }
    log.info("Will run generation for {} rows", rows);

    Boolean isModelUploaded = false;
    String modelFile = modelFilePath;
    if (modelFilePath == null &&
        (modelFileAsFile == null || modelFileAsFile.isEmpty())) {
      log.info(
          "No model file passed, will default to custom data model or default defined one in configuration");
      if (properties.get(ApplicationConfigs.CUSTOM_DATA_MODEL_DEFAULT) !=
          null) {
        modelFile =
            properties.get(ApplicationConfigs.CUSTOM_DATA_MODEL_DEFAULT);
      } else {
        modelFile = properties.get(ApplicationConfigs.DATA_MODEL_PATH_DEFAULT) +
            properties.get(ApplicationConfigs.DATA_MODEL_DEFAULT);
      }
    }
    if (modelFilePath != null && !modelFilePath.contains("/")) {
      log.info(
          "Model file passed is identified as one of the one provided, so will look for it in data model path: {} ",
          properties.get(ApplicationConfigs.DATA_MODEL_PATH_DEFAULT));
      modelFile = properties.get(ApplicationConfigs.DATA_MODEL_PATH_DEFAULT) +
          modelFilePath;
    }
    if (modelFileAsFile != null && !modelFileAsFile.isEmpty()) {
      log.info("Model passed is an uploaded file");
      modelFile = properties.get(ApplicationConfigs.DATA_MODEL_RECEIVED_PATH) +
          "/model-" + new Random().nextInt() + ".json";
      try {
        modelFileAsFile.transferTo(new File(modelFile));
      } catch (IOException e) {
        log.error(
            "Could not save model file passed in request locally, due to error:",
            e);
        return "{ \"commandUuid\": \"\" , \"error\": \"Error with Model File - Cannot save it locally\" }";
      }
      isModelUploaded = true;
    }

    Boolean scheduled = false;
    if (scheduledReceived != null) {
      scheduled = scheduledReceived;
    }

    long delayBetweenExecutions = 0L;
    if (delayBetweenExecutionsReceived != null) {
      delayBetweenExecutions = delayBetweenExecutionsReceived * 1000L;
    }

    // Parsing model
    log.info("Parsing of model file: {}", modelFile);
    JsonParser parser = new JsonParser(modelFile);
    if (parser.getRoot() == null) {
      log.warn("Error when parsing model file");
      return "{ \"commandUuid\": \"\" , \"error\": \"Error with Model File - Verify its path and structure\" }";
    }
    Model model = parser.renderModelFromFile();

    // Creation of connectors
    List<ConnectorParser.Connector> connectorsList = new ArrayList<>();
    try {
      if (connectorsListAsString == null || connectorsListAsString.isEmpty()) {
        log.info("No connector has been defined, so defaulting to JSON connector");
        connectorsList.add(ConnectorParser.stringToConnector("JSON"));
      } else {
        for (String s : connectorsListAsString) {
          connectorsList.add(ConnectorParser.stringToConnector(s));
        }
      }
    } catch (Exception e) {
      log.warn(
          "Could not parse list of connectors passed, check if it's well formed");
      return "{ \"commandUuid\": \"\" , \"error\": \"Wrong connectors\" }";
    }

    // Creation of command and queued to be processed
    Command command =
        new Command(modelFile, model, threads, batches, rows, scheduled,
            delayBetweenExecutions, connectorsList, properties);
    if (isModelUploaded) {
      // If model has been uploaded, it must be renamed to use its UUID for user and admin convenience
      String newModelFilePath =
          properties.get(ApplicationConfigs.DATA_MODEL_RECEIVED_PATH) +
              "/model-" + command.getCommandUuid().toString() + ".json";
      Utils.moveLocalFile(modelFile, newModelFilePath);
      command.setModelFilePath(newModelFilePath);
    }
    commands.put(command.getCommandUuid(), command);
    commandsToProcess.add(command);

    if (scheduled) {
      scheduledCommands.put(command.getCommandUuid(), command);
      writeScheduledCommands();
      log.info(
          "Command {} found as scheduled with delay between two executions: {}",
          command.getCommandUuid(), command.getDelayBetweenExecutions());
    }

    log.info("Command: {} has been queued to be processed",
        command.getCommandUuid());

    return "{ \"commandUuid\": \"" + command.getCommandUuid() +
        "\" , \"error\": \"\" }";

  }

  /**
   * Processer of the command queued
   */
  @Scheduled(fixedDelay = 1000, initialDelay = 10000)
  public void processCommands() {
    Command command = commandsToProcess.poll();

    if (command != null) {
      command.setStatus(Command.CommandStatus.STARTED);
      long start = System.currentTimeMillis();

      try {
        log.info("Starting Generation for command: {}",
            command.getCommandUuid());


        log.info("Initialization of all connectors");
        /**
         *  WARNING 1 : Having Ozone initiated after other connectors will corrupt Hadoop config and Hive or HDFS will not work, so need to initialize it first
         *  WARNING 2 : If Hive is in the list of connectors, it should be initialized first as it changes columns orders if there are partitions
         *  WARNING 3 : If Kudu is in the list of connectors, it should be initialized first as it changes columns orders if there are partitions
         *  Hence, we need to order connectors properly: Ozone first, Hive always before other (except for ozone) and then the rest
         **/
        List<ConnectorParser.Connector> connectorList = command.getConnectorsListAsString();
        connectorList.sort(ConnectorParser.Connector.conenctorInitPrecedence);

        log.debug("Print connector in order: ");
        connectorList.forEach(s -> log.debug("connector: {}", s.toString()));

        List<ConnectorInterface> connectors = ConnectorsUtils
            .connectorInit(command.getModel(), command.getProperties(),
                connectorList,
                true);

        // Launch Generation of data
        command.setStatus(Command.CommandStatus.RUNNING);
        for (long i = 1; i <= command.getNumberOfBatches(); i++) {
          log.info("Start to process batch {}/{} of {} rows", i,
              command.getNumberOfBatches(), command.getRowsPerBatch());

          List<Row> randomDataList = command.getModel()
              .generateRandomRows(command.getRowsPerBatch(),
                  command.getNumberOfThreads());

          // Send Data to connectors in parallel if there are multiple connectors
          connectors.parallelStream()
              .forEach(connector -> connector.sendOneBatchOfRows(randomDataList));

          // For tests only: print generated data
          if (log.isDebugEnabled()) {
            randomDataList.forEach(
                data -> log.debug("Data is : " + data.toString()));
          }

          log.info("Finished to process batch {}/{} of {} rows", i,
              command.getNumberOfBatches(), command.getRowsPerBatch());
          command.setDurationSeconds(
              (System.currentTimeMillis() - start) / 1000);
          command.setProgress(
              ((double) i / (double) command.getNumberOfBatches()) * 100.0);
        }

        // Terminate all connectors
        connectors.forEach(ConnectorInterface::terminate);

        // Add metrics
        metricsService.updateMetrics(command.getNumberOfBatches(),
            command.getRowsPerBatch(), command.getConnectorsListAsString());

        // Recap of what has been generated
        Utils.recap(command.getNumberOfBatches(), command.getRowsPerBatch(),
            command.getConnectorsListAsString(), command.getModel());
        command.setStatus(Command.CommandStatus.FINISHED);
        command.setLastFinishedTimestamp(System.currentTimeMillis());

      } catch (Exception e) {
        log.warn(
            "An error occurred on command: {} => Mark this command as failed, error is: ",
            command.getCommandUuid(), e);
        command.setStatus(Command.CommandStatus.FAILED);
        command.setLastFinishedTimestamp(System.currentTimeMillis());
      }

      // Compute and print time taken
      log.info("Generation Finished");
      log.info("Data Generation for command: {} took : {} to run",
          command.getCommandUuid(),
          Utils.formatTimetaken(System.currentTimeMillis() - start));
    }
  }


  @Scheduled(fixedDelay = 1000, initialDelay = 15000)
  public void checkScheduledCommandsToProcess() {
    for (Command c : scheduledCommands.values()) {
      if (c.getStatus() == Command.CommandStatus.FAILED) {
        log.info(
            "Removing command {} from scheduled commands as last status is FAILED",
            c.getCommandUuid());
        c.setCommandComment(
            "Command removed from scheduler as last state is failed, please correct it and add it again");
      } else if (c.getStatus() == Command.CommandStatus.FINISHED) {
        if ((System.currentTimeMillis() - c.getLastFinishedTimestamp()) >
            c.getDelayBetweenExecutions()) {
          commandsToProcess.add(c);
          log.info(
              "Command {} set to queue of process as it its delay between executions has passed and last status is FINISHED",
              c.getCommandUuid());
        }
      }
    }
  }

}
