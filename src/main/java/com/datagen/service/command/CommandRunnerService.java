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
import com.datagen.config.PropertiesLoader;
import com.datagen.connector.ConnectorInterface;
import com.datagen.connector.ConnectorsUtils;
import com.datagen.connector.storage.utils.FileUtils;
import com.datagen.model.Model;
import com.datagen.service.credentials.Credentials;
import com.datagen.service.credentials.CredentialsService;
import com.datagen.service.metrics.MetricsService;
import com.datagen.service.model.ModelStoreService;
import com.datagen.utils.Utils;
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


  private final ModelStoreService modelStoreService;

  private final Map<ApplicationConfigs, String> properties;

  @Autowired
  private MetricsService metricsService;

  @Autowired
  private CredentialsService credentialsService;

  private final Map<UUID, Command> commands;
  private final ConcurrentLinkedQueue<Command> commandsToProcess;
  private final Map<UUID, Command> scheduledCommands;
  private final String scheduledCommandsFilePath;

  @Autowired
  public CommandRunnerService(PropertiesLoader propertiesLoader, ModelStoreService modelStoreService) {
    this.properties = propertiesLoader.getPropertiesCopy();
    this.modelStoreService = modelStoreService;
    this.scheduledCommandsFilePath = properties.get(ApplicationConfigs.DATAGEN_SCHEDULER_FILE_PATH);
    this.commandsToProcess = new ConcurrentLinkedQueue<>();
    this.scheduledCommands = new HashMap<>();
    this.commands = new HashMap<>();

    FileUtils.createLocalDirectory(this.properties.get(ApplicationConfigs.DATAGEN_HOME_DIRECTORY));

    FileUtils.createLocalDirectoryWithStrongRights(this.properties.get(ApplicationConfigs.DATAGEN_COMMANDS_PATH));

    readScheduledCommands();
    // After reading scheduled values, file should be re-written
    writeScheduledCommands();

    readCommands();
  }

  /**
   * Every 5 minutes, clean old models received, older than a day
   */
  @Scheduled(fixedDelay = 360000, initialDelay = 100000)
  private void cleanReceivedModels() {
    var files = FileUtils.listLocalFiles(properties.get(ApplicationConfigs.DATAGEN_MODEL_RECEIVED_PATH));
    if(files!=null) {
      Arrays.stream(files).filter(file -> (System.currentTimeMillis()-file.lastModified())>(24*60*60*1000))
          .forEach(file -> FileUtils.deleteLocalFile(file.getAbsolutePath()));
    }
  }

  /**
   * Every hour, clean old commands, older than 7 days
   */
  @Scheduled(fixedDelay = 600000, initialDelay = 100000)
  private void cleanCommands() {
    var files = FileUtils.listLocalFiles(properties.get(ApplicationConfigs.DATAGEN_COMMANDS_PATH));
    if(files!=null) {
      Arrays.stream(files).filter(file -> (System.currentTimeMillis()-file.lastModified())>(7*24*60*60*1000))
          .forEach(file -> FileUtils.deleteLocalFile(file.getAbsolutePath()));
    }
  }

  private void readCommands() {
    var commandFiles = FileUtils.listLocalFiles(properties.get(ApplicationConfigs.DATAGEN_COMMANDS_PATH));
    if(commandFiles!=null) {
      Arrays.stream(commandFiles).forEach(c -> {
        try {
          var command = Command.readCommandFromJSON(new FileInputStream(c));
          var model = modelStoreService.getModel(command.getModelId());
          if(model==null) {
            log.warn("Model: {} not found for command: {}", command.getModelId(), command.getCommandUuid());
            FileUtils.deleteLocalFile(c.getAbsolutePath());
            log.info("Command: {} has been deleted as its model is no longer available", command.getCommandUuid());
          } else {
            command.setModel(model);
            commands.put(command.getCommandUuid(), command);
          }
        } catch (Exception e) {
          log.warn("Cannot read command from file: {}", c.getAbsolutePath());
          log.debug("Error is: ", e);
        }
      });
    }
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

  public List<CommandSoft> getAllCommandsSoft() {
    List<CommandSoft> commandsAsList = new ArrayList<>();
    commands.forEach((u, c) -> commandsAsList.add(
        getCommandStatusShort(c.getCommandUuid())));
    return commandsAsList;
  }

  public List<Command> getAllCommands() {
    return commands.values().stream().toList();
  }


  public List<CommandSoft> getCommandsSoftByStatus(Command.CommandStatus status) {
    List<CommandSoft> commandsAsList = new ArrayList<>();
    commands.forEach((u, c) -> {
      if (c.getStatus() == status) {
        commandsAsList.add(getCommandStatusShort(c.getCommandUuid()));
      }
    });
    return commandsAsList;
  }

  public List<CommandSoft> getAllScheduledCommandsSoft() {
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

      FileUtils.deleteLocalFile(scheduledCommandsFilepathTemp);
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

      FileUtils.deleteLocalFile(scheduledCommandsFilepathTemp);

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
          Model model = null;
          if(c.getModelId()!=null) {
            // Check if model is already existing
            model = modelStoreService.getModel(c.getModelId());
          }
          // If not, create it
          if(model==null) {
            model = modelStoreService.addModel(c.getModelFilePath(), false);
            c.setModel(model);
            c.setModelId(model.getName());
          } else {
            c.setModel(model);
          }

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
   * This function stays for previous way to generate data but should be suppressed soon
   *
   * @param modelFileAsFile
   * @param modelName
   * @param numberOfThreads
   * @param numberOfBatches
   * @param rowsPerBatch
   * @param scheduledReceived
   * @param delayBetweenExecutionsReceived
   * @param connectorsListAsString
   * @param extraProperties
   * @return
   */
  public String generateData(
      @Nullable MultipartFile modelFileAsFile,
      @Nullable String modelName,
      @Nullable String owner,
      @Nullable Integer numberOfThreads,
      @Nullable Long numberOfBatches,
      @Nullable Long rowsPerBatch,
      @Nullable Boolean scheduledReceived,
      @Nullable Long delayBetweenExecutionsReceived,
      List<String> connectorsListAsString,
      @Nullable Map<ApplicationConfigs, String> extraProperties,
      @Nullable List<String> credentialsList) {

    var modelId = modelName;
    var modelFile = "";

    if (modelName != null) {
      log.info(
          "Model file passed is identified as one of the one provided, so will look for in already registered models ");
    } else if (modelFileAsFile != null && !modelFileAsFile.isEmpty()) {
      log.info("Model passed is an uploaded file");
      modelFile = properties.get(ApplicationConfigs.DATAGEN_MODEL_RECEIVED_PATH) +
          "/model-" + System.currentTimeMillis() + "-" + String.format("%06d",new Random().nextInt(100000)) + ".json";
      try {
        modelFileAsFile.transferTo(new File(modelFile));
      } catch (IOException e) {
        log.error(
            "Could not save model file passed in request locally, due to error:",
            e);
      }

      // WHY ??? If model has been uploaded, it must be renamed to use its UUID for user and admin convenience
      //String newModelFilePath =properties.get(ApplicationConfigs.DATAGEN_MODEL_RECEIVED_PATH) +"/model-" + System.nanoTime() + ".json";
      //FileUtils.moveLocalFile(modelFile, newModelFilePath);
      // Parsing model
      modelId = modelStoreService.addModel(modelFile, false, owner).getName();
    } else {
      return "{ \"commandUuid\": \"\" , \"error\": \"Error Provide a model file or an existing model named\" }";
    }

    // Transform creds passed into real creds
    var credsList = new ArrayList<Credentials>();
    var allCreds = credentialsService.listCredentialsMetaAsMap();
    if(credentialsList!=null && !credentialsList.isEmpty()) {
      credentialsList.forEach(c -> {
        credsList.add(allCreds.get(c));
      });
    }

    var commandUUID = generateData(modelStoreService.getModelAsModelStored(modelId), owner,
        numberOfThreads, numberOfBatches, rowsPerBatch,
        scheduledReceived, delayBetweenExecutionsReceived, connectorsListAsString, extraProperties, credsList);

    if(commandUUID!=null) {
      return "{ \"commandUuid\": \"" + commandUUID + "\" , \"error\": \"\" }";
    } else {
      return "{ \"commandUuid\": \"\" , \"error\": \"Wrong connectors\" }";
    }
  }

    /**
     * Create a command by solving all properties, model and all empty vars and queue the command to be processed
     * @param numberOfThreads
     * @param numberOfBatches
     * @param rowsPerBatch
     * @param connectorsListAsString
     * @param extraProperties
     */
  public UUID generateData(
      ModelStoreService.ModelStored model,
      @Nullable String owner,
      @Nullable Integer numberOfThreads,
      @Nullable Long numberOfBatches,
      @Nullable Long rowsPerBatch,
      @Nullable Boolean scheduledReceived,
      @Nullable Long delayBetweenExecutionsReceived,
      List<String> connectorsListAsString,
      @Nullable Map<ApplicationConfigs, String> extraProperties,
      @Nullable List<Credentials> credentialsList) {

    // Before generating any data, a deep copy of the model must be made as it will be enhanced with extra properties and credentials
    var modelForTheRun = new Model(model.getModel());

    if (extraProperties != null && !extraProperties.isEmpty()) {
      log.info(
          "Found extra properties sent with the call, these will replace defaults ones");
      properties.putAll(extraProperties);
    }

    int threads = 1;
    if (numberOfThreads != null) {
      threads = numberOfThreads;
    } else if (properties.get(ApplicationConfigs.GENERATION_THREADS_DEFAULT) != null) {
      threads = Integer.parseInt(properties.get(ApplicationConfigs.GENERATION_THREADS_DEFAULT));
    }
    log.info("Will run generation using {} thread(s)", threads);

    Long batches = 1L;
    if (numberOfBatches != null) {
      batches = numberOfBatches;
    } else if (properties.get(ApplicationConfigs.GENERATION_BATCHES_DEFAULT) !=
        null) {
      batches = Long.valueOf(
          properties.get(ApplicationConfigs.GENERATION_BATCHES_DEFAULT));
    }
    log.info("Will run generation for {} batches", batches);

    Long rows = 1L;
    if (rowsPerBatch != null) {
      rows = rowsPerBatch;
    } else if (properties.get(ApplicationConfigs.GENERATION_ROWS_DEFAULT) !=
        null) {
      rows = Long.valueOf(
          properties.get(ApplicationConfigs.GENERATION_ROWS_DEFAULT));
    }
    log.info("Will run generation for {} rows", rows);


    Boolean scheduled = false;
    if (scheduledReceived != null) {
      scheduled = scheduledReceived;
    }

    long delayBetweenExecutions = 0L;
    if (delayBetweenExecutionsReceived != null) {
      delayBetweenExecutions = delayBetweenExecutionsReceived * 1000L;
    }

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
      return null;
    }

    credentialsList.forEach(c -> credentialsService.enrichModelWithCredentials(modelForTheRun, c));

    // Creation of command and queued to be processed
    Command command =
        new Command(model.getPath(), modelForTheRun, owner, threads, batches, rows, scheduled,
            delayBetweenExecutions, connectorsList, properties);
    commands.put(command.getCommandUuid(), command);
    commandsToProcess.add(command);
    command.writeCommandAsJSON(properties.get(ApplicationConfigs.DATAGEN_COMMANDS_PATH)+"/"+command.getCommandUuid());

    if (scheduled) {
      scheduledCommands.put(command.getCommandUuid(), command);
      writeScheduledCommands();
      log.info(
          "Command {} found as scheduled with delay between two executions: {}",
          command.getCommandUuid(), command.getDelayBetweenExecutions());
    }

    log.info("Command: {} has been queued to be processed",
        command.getCommandUuid());

    return command.getCommandUuid();

  }

  /**
   * Processor of the command queued
   */
  @Scheduled(fixedDelay = 1000, initialDelay = 10000)
  public void processCommands() {
    Command command = commandsToProcess.poll();

    if (command != null) {
      command.setStatus(Command.CommandStatus.STARTED);
      long start = System.currentTimeMillis();
      command.writeCommandAsJSON(properties.get(ApplicationConfigs.DATAGEN_COMMANDS_PATH)+"/"+command.getCommandUuid());

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
        List<ConnectorParser.Connector> connectorList = command.getConnectorsList();
        connectorList.sort(ConnectorParser.Connector.connectorInitPrecedence);

        log.debug("Print connector in order: ");
        connectorList.forEach(s -> log.debug("connector: {}", s.toString()));

        List<ConnectorInterface> connectors = ConnectorsUtils
            .connectorInit(command.getModel(), command.getProperties(),
                connectorList,
                true);

        // Launch Generation of data
        command.setStatus(Command.CommandStatus.RUNNING);
        command.writeCommandAsJSON(properties.get(ApplicationConfigs.DATAGEN_COMMANDS_PATH)+"/"+command.getCommandUuid());
        for (long i = 1; i <= command.getNumberOfBatches(); i++) {
          log.info("Start to process batch {}/{} of {} rows", i,
              command.getNumberOfBatches(), command.getRowsPerBatch());

          List randomDataList = command.getModel()
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
          command.setDurationMilliSeconds(System.currentTimeMillis() - start);
          command.setProgress(
              ((double) i / (double) command.getNumberOfBatches()) * 100.0);
          command.writeCommandAsJSON(properties.get(ApplicationConfigs.DATAGEN_COMMANDS_PATH)+"/"+command.getCommandUuid());
        }

        // Terminate all connectors
        connectors.forEach(ConnectorInterface::terminate);

        // Add metrics
        metricsService.updateMetrics(command.getNumberOfBatches(),
            command.getRowsPerBatch(), command.getConnectorsList());

        // Recap of what has been generated
        Utils.recap(command.getNumberOfBatches(), command.getRowsPerBatch(),
            command.getConnectorsList(), command.getModel());
        command.setStatus(Command.CommandStatus.FINISHED);
        command.setLastFinishedTimestamp(System.currentTimeMillis());
        command.writeCommandAsJSON(properties.get(ApplicationConfigs.DATAGEN_COMMANDS_PATH)+"/"+command.getCommandUuid());
      } catch (Exception e) {
        log.warn(
            "An error occurred on command: {} => Mark this command as failed, error is: ",
            command.getCommandUuid(), e);

        // To print full error
        var sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        command.setError(sw.toString());

        command.setCommandError(e.getMessage());
        command.setStatus(Command.CommandStatus.FAILED);
        command.setLastFinishedTimestamp(System.currentTimeMillis());
        command.writeCommandAsJSON(properties.get(ApplicationConfigs.DATAGEN_COMMANDS_PATH)+"/"+command.getCommandUuid());
      }

      // Compute and print time taken
      log.info("Generation Finished");
      var timeTaken = System.currentTimeMillis() - start;
      command.setDurationMilliSeconds(timeTaken);
      log.info("Data Generation for command: {} took : {} to run",
          command.getCommandUuid(), Utils.formatTimetaken(timeTaken));
    }
  }


  @Scheduled(fixedDelay = 1000, initialDelay = 15000)
  public void checkScheduledCommandsToProcess() {
    for (Command c : scheduledCommands.values()) {
      if (c.getStatus() == Command.CommandStatus.FAILED) {
        log.info(
            "Removing command {} from scheduled commands as last status is FAILED",
            c.getCommandUuid());
        c.setCommandError(
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
