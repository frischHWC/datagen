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
package com.datagen.controller.generation;


import com.datagen.config.ApplicationConfigMapper;
import com.datagen.config.PropertiesLoader;
import com.datagen.service.api.APIService;
import com.datagen.service.command.CommandRunnerService;
import com.datagen.service.model.ModelStoreService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.userdetails.User;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.Collections;
import java.util.List;
import java.util.Map;


@Slf4j
@RestController
@RequestMapping("/api/v1/datagen")
public class DataGenerationController {

  @Autowired
  private CommandRunnerService commandRunnerService;

  @Autowired
  private ModelStoreService modelStoreService;

  @Autowired
  private APIService apiService;

  @Autowired
  private PropertiesLoader propertiesLoader;

  @PostMapping(value = "/multipleconnectors", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoMultipleConnectors(
      @RequestPart(required = false, name = "model_file")
      MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
      Long delayBetweenExecutions,
      @RequestParam(required = false, name = "extraProperties")
      Map<String, String> extraProperties,
      @RequestParam(required = false, name = "credentials")
      List<String> credentials,
      @RequestParam(name = "connectors") List<String> connectors,
      @AuthenticationPrincipal User user
  ) {
    StringBuffer connectorList = new StringBuffer();
    connectors.forEach(s -> {
      connectorList.append(s);
      connectorList.append(" ; ");
    });
    Boolean scheduled = delayBetweenExecutions != null;
    log.debug(
        "Received request with model: {} , threads: {} , batches: {}, rows: {}, to connectors: {}",
        modelName, threads, numberOfBatches, rowsPerBatch, connectorList);
    return commandRunnerService.generateData(modelFile, modelName, user.getUsername(), threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        connectors,
        ApplicationConfigMapper.parsePropertiesMap(extraProperties), credentials);
  }

  @PostMapping(value = "/csv", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoCsv(
      @RequestPart(required = false, name = "model_file")
      MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
      Long delayBetweenExecutions,
      @RequestParam(required = false, name = "extraProperties")
      Map<String, String> extraProperties,
      @RequestParam(required = false, name = "credentials")
      List<String> credentials,
      @AuthenticationPrincipal User user
  ) {
    log.debug(
        "Received request for CSV with model: {} , threads: {} , batches: {}, rows: {}",
        modelName, threads, numberOfBatches, rowsPerBatch);
    Boolean scheduled = delayBetweenExecutions != null;
    return commandRunnerService.generateData(modelFile, modelName, user.getUsername(), threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("CSV"),
        ApplicationConfigMapper.parsePropertiesMap(extraProperties), credentials);
  }

  @PostMapping(value = "/json", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoJson(
      @RequestPart(required = false, name = "model_file")
      MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
      Long delayBetweenExecutions,
      @RequestParam(required = false, name = "extraProperties")
      Map<String, String> extraProperties,
      @RequestParam(required = false, name = "credentials")
      List<String> credentials,
      @AuthenticationPrincipal User user
  ) {
    log.debug(
        "Received request for JSON with model: {} , threads: {} , batches: {}, rows: {}",
        modelName, threads, numberOfBatches, rowsPerBatch);
    Boolean scheduled = delayBetweenExecutions != null;
    return commandRunnerService.generateData(modelFile, modelName, user.getUsername(), threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("JSON"),
        ApplicationConfigMapper.parsePropertiesMap(extraProperties), credentials);
  }

  @PostMapping(value = "/avro", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoAvro(
      @RequestPart(required = false, name = "model_file")
      MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
      Long delayBetweenExecutions,
      @RequestParam(required = false, name = "extraProperties")
      Map<String, String> extraProperties,
      @RequestParam(required = false, name = "credentials")
      List<String> credentials,
      @AuthenticationPrincipal User user
  ) {
    log.debug(
        "Received request for Avro with model: {} , threads: {} , batches: {}, rows: {}",
        modelName, threads, numberOfBatches, rowsPerBatch);
    Boolean scheduled = delayBetweenExecutions != null;
    return commandRunnerService.generateData(modelFile, modelName, user.getUsername(), threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("AVRO"),
        ApplicationConfigMapper.parsePropertiesMap(extraProperties), credentials);
  }

  @PostMapping(value = "/parquet", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoParquet(
      @RequestPart(required = false, name = "model_file")
      MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
      Long delayBetweenExecutions,
      @RequestParam(required = false, name = "extraProperties")
      Map<String, String> extraProperties,
      @RequestParam(required = false, name = "credentials")
      List<String> credentials,
      @AuthenticationPrincipal User user
  ) {
    log.debug(
        "Received request for Parquet with model: {} , threads: {} , batches: {}, rows: {}",
        modelName, threads, numberOfBatches, rowsPerBatch);
    Boolean scheduled = delayBetweenExecutions != null;
    return commandRunnerService.generateData(modelFile, modelName, user.getUsername(), threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("PARQUET"),
        ApplicationConfigMapper.parsePropertiesMap(extraProperties), credentials);
  }

  @PostMapping(value = "/orc", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoOrc(
      @RequestPart(required = false, name = "model_file")
      MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
      Long delayBetweenExecutions,
      @RequestParam(required = false, name = "extraProperties")
      Map<String, String> extraProperties,
      @RequestParam(required = false, name = "credentials")
      List<String> credentials,
      @AuthenticationPrincipal User user
  ) {
    log.debug(
        "Received request for ORC with model: {} , threads: {} , batches: {}, rows: {}",
        modelName, threads, numberOfBatches, rowsPerBatch);
    Boolean scheduled = delayBetweenExecutions != null;
    return commandRunnerService.generateData(modelFile, modelName, user.getUsername(), threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("ORC"),
        ApplicationConfigMapper.parsePropertiesMap(extraProperties), credentials);
  }

  @PostMapping(value = "/hdfs_csv", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoHdfsCsv(
      @RequestPart(required = false, name = "model_file")
      MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
      Long delayBetweenExecutions,
      @RequestParam(required = false, name = "extraProperties")
      Map<String, String> extraProperties,
      @RequestParam(required = false, name = "credentials")
      List<String> credentials,
      @AuthenticationPrincipal User user
  ) {
    log.debug(
        "Received request for HDFS_CSV with model: {} , threads: {} , batches: {}, rows: {}",
        modelName, threads, numberOfBatches, rowsPerBatch);
    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelName, user.getUsername(), threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("HDFS_CSV"),
        ApplicationConfigMapper.parsePropertiesMap(extraProperties), credentials);
  }

  @PostMapping(value = "/hdfs_avro", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoHdfsAvro(
      @RequestPart(required = false, name = "model_file")
      MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
      Long delayBetweenExecutions,
      @RequestParam(required = false, name = "extraProperties")
      Map<String, String> extraProperties,
      @RequestParam(required = false, name = "credentials")
      List<String> credentials,
      @AuthenticationPrincipal User user
  ) {
    log.debug(
        "Received request for HDFS_AVRO with model: {} , threads: {} , batches: {}, rows: {}",
        modelName, threads, numberOfBatches, rowsPerBatch);
    Boolean scheduled = delayBetweenExecutions != null;


    return commandRunnerService.generateData(modelFile, modelName, user.getUsername(), threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("HDFS_AVRO"),
        ApplicationConfigMapper.parsePropertiesMap(extraProperties), credentials);
  }

  @PostMapping(value = "/hdfs_json", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoHdfsJson(
      @RequestPart(required = false, name = "model_file")
      MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
      Long delayBetweenExecutions,
      @RequestParam(required = false, name = "extraProperties")
      Map<String, String> extraProperties,
      @RequestParam(required = false, name = "credentials")
      List<String> credentials,
      @AuthenticationPrincipal User user
  ) {
    log.debug(
        "Received request for HDFS_JSON with model: {} , threads: {} , batches: {}, rows: {}",
        modelName, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelName, user.getUsername(), threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("HDFS_JSON"),
        ApplicationConfigMapper.parsePropertiesMap(extraProperties), credentials);
  }

  @PostMapping(value = "/hdfs_parquet", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoHdfsParquet(
      @RequestPart(required = false, name = "model_file")
      MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
      Long delayBetweenExecutions,
      @RequestParam(required = false, name = "extraProperties")
      Map<String, String> extraProperties,
      @RequestParam(required = false, name = "credentials")
      List<String> credentials,
      @AuthenticationPrincipal User user
  ) {
    log.debug(
        "Received request for HDFS_PARQUET with model: {} , threads: {} , batches: {}, rows: {}",
        modelName, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelName, user.getUsername(), threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("HDFS_PARQUET"),
        ApplicationConfigMapper.parsePropertiesMap(extraProperties), credentials);
  }

  @PostMapping(value = "/hdfs_orc", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoHdfsOrc(
      @RequestPart(required = false, name = "model_file")
      MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
      Long delayBetweenExecutions,
      @RequestParam(required = false, name = "extraProperties")
      Map<String, String> extraProperties,
      @RequestParam(required = false, name = "credentials")
      List<String> credentials,
      @AuthenticationPrincipal User user
  ) {
    log.debug(
        "Received request for HDFS_ORC with model: {} , threads: {} , batches: {}, rows: {}",
        modelName, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelName, user.getUsername(), threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("HDFS_ORC"),
        ApplicationConfigMapper.parsePropertiesMap(extraProperties), credentials);
  }

  @PostMapping(value = "/hbase", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoHbase(
      @RequestPart(required = false, name = "model_file")
      MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
      Long delayBetweenExecutions,
      @RequestParam(required = false, name = "extraProperties")
      Map<String, String> extraProperties,
      @RequestParam(required = false, name = "credentials")
      List<String> credentials,
      @AuthenticationPrincipal User user
  ) {
    log.debug(
        "Received request for HBASE with model: {} , threads: {} , batches: {}, rows: {}",
        modelName, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelName, user.getUsername(), threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("HBASE"),
        ApplicationConfigMapper.parsePropertiesMap(extraProperties), credentials);
  }

  @PostMapping(value = "/hive", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoHive(
      @RequestPart(required = false, name = "model_file")
      MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
      Long delayBetweenExecutions,
      @RequestParam(required = false, name = "extraProperties")
      Map<String, String> extraProperties,
      @RequestParam(required = false, name = "credentials")
      List<String> credentials,
      @AuthenticationPrincipal User user
  ) {
    log.debug(
        "Received request for HIVE with model: {} , threads: {} , batches: {}, rows: {}",
        modelName, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelName, user.getUsername(), threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("HIVE"),
        ApplicationConfigMapper.parsePropertiesMap(extraProperties), credentials);
  }

  @PostMapping(value = "/ozone", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoOzone(
      @RequestPart(required = false, name = "model_file")
      MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
      Long delayBetweenExecutions,
      @RequestParam(required = false, name = "extraProperties")
      Map<String, String> extraProperties,
      @RequestParam(required = false, name = "credentials")
      List<String> credentials,
      @AuthenticationPrincipal User user
  ) {
    log.debug(
        "Received request for OZONE with model: {} , threads: {} , batches: {}, rows: {}",
        modelName, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelName, user.getUsername(), threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("OZONE"),
        ApplicationConfigMapper.parsePropertiesMap(extraProperties), credentials);
  }

  @PostMapping(value = "/ozone_csv", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoOzoneCsv(
      @RequestPart(required = false, name = "model_file")
      MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
      Long delayBetweenExecutions,
      @RequestParam(required = false, name = "extraProperties")
      Map<String, String> extraProperties,
      @RequestParam(required = false, name = "credentials")
      List<String> credentials,
      @AuthenticationPrincipal User user
  ) {
    log.debug(
        "Received request for OZONE_CSV with model: {} , threads: {} , batches: {}, rows: {}",
        modelName, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelName, user.getUsername(), threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("OZONE_CSV"),
        ApplicationConfigMapper.parsePropertiesMap(extraProperties), credentials);
  }

  @PostMapping(value = "/ozone_json", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoOzoneJson(
      @RequestPart(required = false, name = "model_file")
      MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
      Long delayBetweenExecutions,
      @RequestParam(required = false, name = "extraProperties")
      Map<String, String> extraProperties,
      @RequestParam(required = false, name = "credentials")
      List<String> credentials,
      @AuthenticationPrincipal User user
  ) {
    log.debug(
        "Received request for OZONE_JSON with model: {} , threads: {} , batches: {}, rows: {}",
        modelName, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelName, user.getUsername(), threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("OZONE_JSON"),
        ApplicationConfigMapper.parsePropertiesMap(extraProperties), credentials);
  }

  @PostMapping(value = "/ozone_avro", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoOzoneAvro(
      @RequestPart(required = false, name = "model_file")
      MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
      Long delayBetweenExecutions,
      @RequestParam(required = false, name = "extraProperties")
      Map<String, String> extraProperties,
      @RequestParam(required = false, name = "credentials")
      List<String> credentials,
      @AuthenticationPrincipal User user
  ) {
    log.debug(
        "Received request for OZONE_AVRO with model: {} , threads: {} , batches: {}, rows: {}",
        modelName, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelName, user.getUsername(), threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("OZONE_AVRO"),
        ApplicationConfigMapper.parsePropertiesMap(extraProperties), credentials);
  }

  @PostMapping(value = "/ozone_parquet", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoOzoneParquet(
      @RequestPart(required = false, name = "model_file")
      MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
      Long delayBetweenExecutions,
      @RequestParam(required = false, name = "extraProperties")
      Map<String, String> extraProperties,
      @RequestParam(required = false, name = "credentials")
      List<String> credentials,
      @AuthenticationPrincipal User user
  ) {
    log.debug(
        "Received request for OZONE_PARQUET with model: {} , threads: {} , batches: {}, rows: {}",
        modelName, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelName, user.getUsername(), threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("OZONE_PARQUET"),
        ApplicationConfigMapper.parsePropertiesMap(extraProperties), credentials);
  }

  @PostMapping(value = "/ozone_orc", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoOzoneOrc(
      @RequestPart(required = false, name = "model_file")
      MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
      Long delayBetweenExecutions,
      @RequestParam(required = false, name = "extraProperties")
      Map<String, String> extraProperties,
      @RequestParam(required = false, name = "credentials")
      List<String> credentials,
      @AuthenticationPrincipal User user
  ) {
    log.debug(
        "Received request for OZONE_ORC with model: {} , threads: {} , batches: {}, rows: {}",
        modelName, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelName, user.getUsername(), threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("OZONE_ORC"),
        ApplicationConfigMapper.parsePropertiesMap(extraProperties), credentials);
  }

  @PostMapping(value = "/kafka", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoKafka(
      @RequestPart(required = false, name = "model_file")
      MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
      Long delayBetweenExecutions,
      @RequestParam(required = false, name = "extraProperties")
      Map<String, String> extraProperties,
      @RequestParam(required = false, name = "credentials")
      List<String> credentials,
      @AuthenticationPrincipal User user
  ) {
    log.debug(
        "Received request for KAFKA with model: {} , threads: {} , batches: {}, rows: {}",
        modelName, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelName, user.getUsername(), threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("KAFKA"),
        ApplicationConfigMapper.parsePropertiesMap(extraProperties), credentials);
  }

  @PostMapping(value = "/solr", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoSolR(
      @RequestPart(required = false, name = "model_file")
      MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
      Long delayBetweenExecutions,
      @RequestParam(required = false, name = "extraProperties")
      Map<String, String> extraProperties,
      @RequestParam(required = false, name = "credentials")
      List<String> credentials,
      @AuthenticationPrincipal User user
  ) {
    log.debug(
        "Received request for SOLR with model: {} , threads: {} , batches: {}, rows: {}",
        modelName, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelName, user.getUsername(), threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("SOLR"),
        ApplicationConfigMapper.parsePropertiesMap(extraProperties), credentials);
  }

  @PostMapping(value = "/kudu", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoKudu(
      @RequestPart(required = false, name = "model_file")
      MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
      Long delayBetweenExecutions,
      @RequestParam(required = false, name = "extraProperties")
      Map<String, String> extraProperties,
      @RequestParam(required = false, name = "credentials")
      List<String> credentials,
      @AuthenticationPrincipal User user
  ) {
    log.debug(
        "Received request for KUDU with model: {} , threads: {} , batches: {}, rows: {}",
        modelName, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelName, user.getUsername(), threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("KUDU"),
        ApplicationConfigMapper.parsePropertiesMap(extraProperties), credentials);
  }

  @PostMapping(value = "/api", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateAPI(
      @RequestPart(required = false, name = "model_file")
      MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelName,
      @AuthenticationPrincipal User user
  ) {
    log.debug("Received request for API with model: {}", modelName);

    return "{ modelId : " + apiService.saveModel(modelFile, modelName) +
        " }";
  }

  @GetMapping(value = "/api/gen")
  @ResponseBody
  public String getFromAPI(@RequestParam(name = "modelId") String modelId,
      @AuthenticationPrincipal User user
  ) {
    log.debug("Received request for API to generate data from model: {}",
        modelId);

    return apiService.generateData(modelId);
  }

  @PostMapping(value = "/s3_csv", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoS3CSV(
      @RequestPart(required = false, name = "model_file")
      MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
      Long delayBetweenExecutions,
      @RequestParam(required = false, name = "extraProperties")
      Map<String, String> extraProperties,
      @RequestParam(required = false, name = "credentials")
      List<String> credentials,
      @AuthenticationPrincipal User user
  ) {
    log.debug(
        "Received request for S3_CSV with model: {} , threads: {} , batches: {}, rows: {}",
        modelName, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelName, user.getUsername(), threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("S3_CSV"),
        ApplicationConfigMapper.parsePropertiesMap(extraProperties), credentials);
  }

  @PostMapping(value = "/s3_json", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoS3Json(
      @RequestPart(required = false, name = "model_file")
      MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
      Long delayBetweenExecutions,
      @RequestParam(required = false, name = "extraProperties")
      Map<String, String> extraProperties,
      @RequestParam(required = false, name = "credentials")
      List<String> credentials,
      @AuthenticationPrincipal User user
  ) {
    log.debug(
        "Received request for S3_JSON with model: {} , threads: {} , batches: {}, rows: {}",
        modelName, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelName, user.getUsername(), threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("S3_JSON"),
        ApplicationConfigMapper.parsePropertiesMap(extraProperties), credentials);
  }

  @PostMapping(value = "/s3_avro", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoS3Avro(
      @RequestPart(required = false, name = "model_file")
      MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
      Long delayBetweenExecutions,
      @RequestParam(required = false, name = "extraProperties")
      Map<String, String> extraProperties,
      @RequestParam(required = false, name = "credentials")
      List<String> credentials,
      @AuthenticationPrincipal User user
  ) {
    log.debug(
        "Received request for S3_AVRO with model: {} , threads: {} , batches: {}, rows: {}",
        modelName, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelName, user.getUsername(), threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("S3_AVRO"),
        ApplicationConfigMapper.parsePropertiesMap(extraProperties), credentials);
  }

  @PostMapping(value = "/s3_parquet", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoS3Parquet(
      @RequestPart(required = false, name = "model_file")
      MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
      Long delayBetweenExecutions,
      @RequestParam(required = false, name = "extraProperties")
      Map<String, String> extraProperties,
      @RequestParam(required = false, name = "credentials")
      List<String> credentials,
      @AuthenticationPrincipal User user
  ) {
    log.debug(
        "Received request for S3_PARQUET with model: {} , threads: {} , batches: {}, rows: {}",
        modelName, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelName, user.getUsername(), threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("S3_PARQUET"),
        ApplicationConfigMapper.parsePropertiesMap(extraProperties), credentials);
  }

  @PostMapping(value = "/s3_orc", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoS3Orc(
      @RequestPart(required = false, name = "model_file")
      MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
      Long delayBetweenExecutions,
      @RequestParam(required = false, name = "extraProperties")
      Map<String, String> extraProperties,
      @RequestParam(required = false, name = "credentials")
      List<String> credentials,
      @AuthenticationPrincipal User user
  ) {
    log.debug(
        "Received request for S3_ORC with model: {} , threads: {} , batches: {}, rows: {}",
        modelName, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelName, user.getUsername(), threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("S3_ORC"),
        ApplicationConfigMapper.parsePropertiesMap(extraProperties), credentials);
  }

  @PostMapping(value = "/adls_csv", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoAdlsCSV(
      @RequestPart(required = false, name = "model_file")
      MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
      Long delayBetweenExecutions,
      @RequestParam(required = false, name = "extraProperties")
      Map<String, String> extraProperties,
      @RequestParam(required = false, name = "credentials")
      List<String> credentials,
      @AuthenticationPrincipal User user
  ) {
    log.debug(
        "Received request for ADLS_CSV with model: {} , threads: {} , batches: {}, rows: {}",
        modelName, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelName, user.getUsername(), threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("ADLS_CSV"),
        ApplicationConfigMapper.parsePropertiesMap(extraProperties), credentials);
  }

  @PostMapping(value = "/adls_json", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoAdlsJson(
      @RequestPart(required = false, name = "model_file")
      MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
      Long delayBetweenExecutions,
      @RequestParam(required = false, name = "extraProperties")
      Map<String, String> extraProperties,
      @RequestParam(required = false, name = "credentials")
      List<String> credentials,
      @AuthenticationPrincipal User user
  ) {
    log.debug(
        "Received request for ADLS_JSON with model: {} , threads: {} , batches: {}, rows: {}",
        modelName, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelName, user.getUsername(), threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("ADLS_JSON"),
        ApplicationConfigMapper.parsePropertiesMap(extraProperties), credentials);
  }

  @PostMapping(value = "/adls_avro", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoAdlsAvro(
      @RequestPart(required = false, name = "model_file")
      MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
      Long delayBetweenExecutions,
      @RequestParam(required = false, name = "extraProperties")
      Map<String, String> extraProperties,
      @RequestParam(required = false, name = "credentials")
      List<String> credentials,
      @AuthenticationPrincipal User user
  ) {
    log.debug(
        "Received request for ADLS_AVRO with model: {} , threads: {} , batches: {}, rows: {}",
        modelName, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelName, user.getUsername(), threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("ADLS_AVRO"),
        ApplicationConfigMapper.parsePropertiesMap(extraProperties), credentials);
  }

  @PostMapping(value = "/adls_parquet", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoAdlsParquet(
      @RequestPart(required = false, name = "model_file")
      MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
      Long delayBetweenExecutions,
      @RequestParam(required = false, name = "extraProperties")
      Map<String, String> extraProperties,
      @RequestParam(required = false, name = "credentials")
      List<String> credentials,
      @AuthenticationPrincipal User user
  ) {
    log.debug(
        "Received request for ADLS_PARQUET with model: {} , threads: {} , batches: {}, rows: {}",
        modelName, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelName, user.getUsername(), threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("ADLS_PARQUET"),
        ApplicationConfigMapper.parsePropertiesMap(extraProperties), credentials);
  }

  @PostMapping(value = "/adls_orc", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoAdlsOrc(
      @RequestPart(required = false, name = "model_file")
      MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
      Long delayBetweenExecutions,
      @RequestParam(required = false, name = "extraProperties")
      Map<String, String> extraProperties,
      @RequestParam(required = false, name = "credentials")
      List<String> credentials,
      @AuthenticationPrincipal User user
  ) {
    log.debug(
        "Received request for ADLS_ORC with model: {} , threads: {} , batches: {}, rows: {}",
        modelName, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelName, user.getUsername(), threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("ADLS_ORC"),
        ApplicationConfigMapper.parsePropertiesMap(extraProperties), credentials);
  }


  @PostMapping(value = "/gcs_csv", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoGcsCSV(
      @RequestPart(required = false, name = "model_file")
      MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
      Long delayBetweenExecutions,
      @RequestParam(required = false, name = "extraProperties")
      Map<String, String> extraProperties,
      @RequestParam(required = false, name = "credentials")
      List<String> credentials,
      @AuthenticationPrincipal User user
  ) {
    log.debug(
        "Received request for GCS_CSV with model: {} , threads: {} , batches: {}, rows: {}",
        modelName, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelName, user.getUsername(), threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("GCS_CSV"),
        ApplicationConfigMapper.parsePropertiesMap(extraProperties), credentials);
  }

  @PostMapping(value = "/gcs_json", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoGcsJson(
      @RequestPart(required = false, name = "model_file")
      MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
      Long delayBetweenExecutions,
      @RequestParam(required = false, name = "extraProperties")
      Map<String, String> extraProperties,
      @RequestParam(required = false, name = "credentials")
      List<String> credentials,
      @AuthenticationPrincipal User user
  ) {
    log.debug(
        "Received request for GCS_JSON with model: {} , threads: {} , batches: {}, rows: {}",
        modelName, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelName, user.getUsername(), threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("GCS_JSON"),
        ApplicationConfigMapper.parsePropertiesMap(extraProperties), credentials);
  }

  @PostMapping(value = "/gcs_avro", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoGcsAvro(
      @RequestPart(required = false, name = "model_file")
      MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
      Long delayBetweenExecutions,
      @RequestParam(required = false, name = "extraProperties")
      Map<String, String> extraProperties,
      @RequestParam(required = false, name = "credentials")
      List<String> credentials,
      @AuthenticationPrincipal User user
  ) {
    log.debug(
        "Received request for GCS_AVRO with model: {} , threads: {} , batches: {}, rows: {}",
        modelName, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelName, user.getUsername(), threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("GCS_AVRO"),
        ApplicationConfigMapper.parsePropertiesMap(extraProperties), credentials);
  }

  @PostMapping(value = "/gcs_parquet", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoGcsParquet(
      @RequestPart(required = false, name = "model_file")
      MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
      Long delayBetweenExecutions,
      @RequestParam(required = false, name = "extraProperties")
      Map<String, String> extraProperties,
      @RequestParam(required = false, name = "credentials")
      List<String> credentials,
      @AuthenticationPrincipal User user
  ) {
    log.debug(
        "Received request for GCS_PARQUET with model: {} , threads: {} , batches: {}, rows: {}",
        modelName, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelName, user.getUsername(), threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("GCS_PARQUET"),
        ApplicationConfigMapper.parsePropertiesMap(extraProperties), credentials);
  }

  @PostMapping(value = "/gcs_orc", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoGcsOrc(
      @RequestPart(required = false, name = "model_file")
      MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
      Long delayBetweenExecutions,
      @RequestParam(required = false, name = "extraProperties")
      Map<String, String> extraProperties,
      @RequestParam(required = false, name = "credentials")
      List<String> credentials,
      @AuthenticationPrincipal User user
  ) {
    log.debug(
        "Received request for GCS_ORC with model: {} , threads: {} , batches: {}, rows: {}",
        modelName, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelName, user.getUsername(), threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("GCS_ORC"),
        ApplicationConfigMapper.parsePropertiesMap(extraProperties), credentials);
  }

}
