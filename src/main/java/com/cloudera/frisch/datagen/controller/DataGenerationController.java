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
package com.cloudera.frisch.datagen.controller;


import com.cloudera.frisch.datagen.config.PropertiesLoader;
import com.cloudera.frisch.datagen.service.APISevice;
import com.cloudera.frisch.datagen.service.CommandRunnerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.Collections;
import java.util.List;
import java.util.UUID;


@Slf4j
@RestController
@RequestMapping("/datagen")
public class DataGenerationController {

  @Autowired
  private CommandRunnerService commandRunnerService;

  @Autowired
  private APISevice apiSevice;

  @Autowired
  private PropertiesLoader propertiesLoader;


  @PostMapping(value = "/multipleconnectors", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoMultipleConnectors(
      @RequestPart(required = false, name = "model_file")
          MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
          Long delayBetweenExecutions,
      @RequestParam(name = "connectors") List<String> connectors
  ) {
    StringBuffer connectorList = new StringBuffer();
    connectors.forEach(s -> {
      connectorList.append(s);
      connectorList.append(" ; ");
    });
    Boolean scheduled = delayBetweenExecutions != null;
    log.debug(
        "Received request with model: {} , threads: {} , batches: {}, rows: {}, to connectors: {}",
        modelFilePath, threads, numberOfBatches, rowsPerBatch, connectorList);
    return commandRunnerService.generateData(modelFile, modelFilePath, threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions, connectors,
        null);
  }

  @PostMapping(value = "/csv", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoCsv(
      @RequestPart(required = false, name = "model_file")
          MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
          Long delayBetweenExecutions
  ) {
    log.debug(
        "Received request for CSV with model: {} , threads: {} , batches: {}, rows: {}",
        modelFilePath, threads, numberOfBatches, rowsPerBatch);
    Boolean scheduled = delayBetweenExecutions != null;
    return commandRunnerService.generateData(modelFile, modelFilePath, threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("CSV"), null);
  }

  @PostMapping(value = "/json", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoJson(
      @RequestPart(required = false, name = "model_file")
          MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
          Long delayBetweenExecutions
  ) {
    log.debug(
        "Received request for JSON with model: {} , threads: {} , batches: {}, rows: {}",
        modelFilePath, threads, numberOfBatches, rowsPerBatch);
    Boolean scheduled = delayBetweenExecutions != null;
    return commandRunnerService.generateData(modelFile, modelFilePath, threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("JSON"), null);
  }

  @PostMapping(value = "/avro", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoAvro(
      @RequestPart(required = false, name = "model_file")
          MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
          Long delayBetweenExecutions
  ) {
    log.debug(
        "Received request for Avro with model: {} , threads: {} , batches: {}, rows: {}",
        modelFilePath, threads, numberOfBatches, rowsPerBatch);
    Boolean scheduled = delayBetweenExecutions != null;
    return commandRunnerService.generateData(modelFile, modelFilePath, threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("AVRO"), null);
  }

  @PostMapping(value = "/parquet", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoParquet(
      @RequestPart(required = false, name = "model_file")
          MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
          Long delayBetweenExecutions
  ) {
    log.debug(
        "Received request for Parquet with model: {} , threads: {} , batches: {}, rows: {}",
        modelFilePath, threads, numberOfBatches, rowsPerBatch);
    Boolean scheduled = delayBetweenExecutions != null;
    return commandRunnerService.generateData(modelFile, modelFilePath, threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("PARQUET"), null);
  }

  @PostMapping(value = "/orc", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoOrc(
      @RequestPart(required = false, name = "model_file")
          MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
          Long delayBetweenExecutions
  ) {
    log.debug(
        "Received request for ORC with model: {} , threads: {} , batches: {}, rows: {}",
        modelFilePath, threads, numberOfBatches, rowsPerBatch);
    Boolean scheduled = delayBetweenExecutions != null;
    return commandRunnerService.generateData(modelFile, modelFilePath, threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("ORC"), null);
  }

  @PostMapping(value = "/hdfs-csv", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoHdfsCsv(
      @RequestPart(required = false, name = "model_file")
          MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
          Long delayBetweenExecutions
  ) {
    log.debug(
        "Received request for HDFS-CSV with model: {} , threads: {} , batches: {}, rows: {}",
        modelFilePath, threads, numberOfBatches, rowsPerBatch);
    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelFilePath, threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("HDFS-CSV"), null);
  }

  @PostMapping(value = "/hdfs-avro", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoHdfsAvro(
      @RequestPart(required = false, name = "model_file")
          MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
          Long delayBetweenExecutions
  ) {
    log.debug(
        "Received request for HDFS-AVRO with model: {} , threads: {} , batches: {}, rows: {}",
        modelFilePath, threads, numberOfBatches, rowsPerBatch);
    Boolean scheduled = delayBetweenExecutions != null;


    return commandRunnerService.generateData(modelFile, modelFilePath, threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("HDFS-AVRO"), null);
  }

  @PostMapping(value = "/hdfs-json", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoHdfsJson(
      @RequestPart(required = false, name = "model_file")
          MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
          Long delayBetweenExecutions
  ) {
    log.debug(
        "Received request for HDFS-JSON with model: {} , threads: {} , batches: {}, rows: {}",
        modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelFilePath, threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("HDFS-JSON"), null);
  }

  @PostMapping(value = "/hdfs-parquet", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoHdfsParquet(
      @RequestPart(required = false, name = "model_file")
          MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
          Long delayBetweenExecutions
  ) {
    log.debug(
        "Received request for HDFS-PARQUET with model: {} , threads: {} , batches: {}, rows: {}",
        modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelFilePath, threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("HDFS-PARQUET"), null);
  }

  @PostMapping(value = "/hdfs-orc", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoHdfsOrc(
      @RequestPart(required = false, name = "model_file")
          MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
          Long delayBetweenExecutions
  ) {
    log.debug(
        "Received request for HDFS-ORC with model: {} , threads: {} , batches: {}, rows: {}",
        modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelFilePath, threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("HDFS-ORC"), null);
  }

  @PostMapping(value = "/hbase", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoHbase(
      @RequestPart(required = false, name = "model_file")
          MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
          Long delayBetweenExecutions
  ) {
    log.debug(
        "Received request for HBASE with model: {} , threads: {} , batches: {}, rows: {}",
        modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelFilePath, threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("HBASE"), null);
  }

  @PostMapping(value = "/hive", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoHive(
      @RequestPart(required = false, name = "model_file")
          MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
          Long delayBetweenExecutions
  ) {
    log.debug(
        "Received request for HIVE with model: {} , threads: {} , batches: {}, rows: {}",
        modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelFilePath, threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("HIVE"), null);
  }

  @PostMapping(value = "/ozone", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoOzone(
      @RequestPart(required = false, name = "model_file")
          MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
          Long delayBetweenExecutions
  ) {
    log.debug(
        "Received request for OZONE with model: {} , threads: {} , batches: {}, rows: {}",
        modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelFilePath, threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("OZONE"), null);
  }

  @PostMapping(value = "/ozone-csv", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoOzoneCsv(
      @RequestPart(required = false, name = "model_file")
          MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
          Long delayBetweenExecutions
  ) {
    log.debug(
        "Received request for OZONE-CSV with model: {} , threads: {} , batches: {}, rows: {}",
        modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelFilePath, threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("OZONE-CSV"), null);
  }

  @PostMapping(value = "/ozone-json", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoOzoneJson(
      @RequestPart(required = false, name = "model_file")
          MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
          Long delayBetweenExecutions
  ) {
    log.debug(
        "Received request for OZONE-JSON with model: {} , threads: {} , batches: {}, rows: {}",
        modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelFilePath, threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("OZONE-JSON"), null);
  }

  @PostMapping(value = "/ozone-avro", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoOzoneAvro(
      @RequestPart(required = false, name = "model_file")
          MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
          Long delayBetweenExecutions
  ) {
    log.debug(
        "Received request for OZONE-AVRO with model: {} , threads: {} , batches: {}, rows: {}",
        modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelFilePath, threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("OZONE-AVRO"), null);
  }

  @PostMapping(value = "/ozone-parquet", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoOzoneParquet(
      @RequestPart(required = false, name = "model_file")
          MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
          Long delayBetweenExecutions
  ) {
    log.debug(
        "Received request for OZONE-PARQUET with model: {} , threads: {} , batches: {}, rows: {}",
        modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelFilePath, threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("OZONE-PARQUET"), null);
  }

  @PostMapping(value = "/ozone-orc", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoOzoneOrc(
      @RequestPart(required = false, name = "model_file")
          MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
          Long delayBetweenExecutions
  ) {
    log.debug(
        "Received request for OZONE-ORC with model: {} , threads: {} , batches: {}, rows: {}",
        modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelFilePath, threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("OZONE-ORC"), null);
  }

  @PostMapping(value = "/kafka", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoKafka(
      @RequestPart(required = false, name = "model_file")
          MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
          Long delayBetweenExecutions
  ) {
    log.debug(
        "Received request for KAFKA with model: {} , threads: {} , batches: {}, rows: {}",
        modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelFilePath, threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("KAFKA"), null);
  }

  @PostMapping(value = "/solr", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoSolR(
      @RequestPart(required = false, name = "model_file")
          MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
          Long delayBetweenExecutions
  ) {
    log.debug(
        "Received request for SOLR with model: {} , threads: {} , batches: {}, rows: {}",
        modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelFilePath, threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("SOLR"), null);
  }

  @PostMapping(value = "/kudu", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoKudu(
      @RequestPart(required = false, name = "model_file")
          MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
          Long delayBetweenExecutions
  ) {
    log.debug(
        "Received request for KUDU with model: {} , threads: {} , batches: {}, rows: {}",
        modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelFilePath, threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("KUDU"), null);
  }

  @PostMapping(value = "/api", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateAPI(
      @RequestPart(required = false, name = "model_file")
          MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelFilePath
  ) {
    log.debug("Received request for API with model: {}", modelFilePath);

    return "{ modelId : " + apiSevice.saveModel(modelFile, modelFilePath) +
        " }";
  }

  @GetMapping(value = "/api/gen")
  @ResponseBody
  public String getFromAPI(@RequestParam(name = "modelId") UUID modelId) {
    log.debug("Received request for API to generate data from model: {}",
        modelId);

    return apiSevice.generateData(modelId);
  }

  @PostMapping(value = "/s3-csv", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoS3CSV(
      @RequestPart(required = false, name = "model_file")
          MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
          Long delayBetweenExecutions
  ) {
    log.debug(
        "Received request for S3-CSV with model: {} , threads: {} , batches: {}, rows: {}",
        modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelFilePath, threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("S3-CSV"), null);
  }

  @PostMapping(value = "/s3-json", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoS3Json(
      @RequestPart(required = false, name = "model_file")
          MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
          Long delayBetweenExecutions
  ) {
    log.debug(
        "Received request for S3-JSON with model: {} , threads: {} , batches: {}, rows: {}",
        modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelFilePath, threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("S3-JSON"), null);
  }

  @PostMapping(value = "/s3-avro", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoS3Avro(
      @RequestPart(required = false, name = "model_file")
          MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
          Long delayBetweenExecutions
  ) {
    log.debug(
        "Received request for S3-AVRO with model: {} , threads: {} , batches: {}, rows: {}",
        modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelFilePath, threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("S3-AVRO"), null);
  }

  @PostMapping(value = "/s3-parquet", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoS3Parquet(
      @RequestPart(required = false, name = "model_file")
          MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
          Long delayBetweenExecutions
  ) {
    log.debug(
        "Received request for S3-PARQUET with model: {} , threads: {} , batches: {}, rows: {}",
        modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelFilePath, threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("S3-PARQUET"), null);
  }

  @PostMapping(value = "/s3-orc", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoS3Orc(
      @RequestPart(required = false, name = "model_file")
          MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
          Long delayBetweenExecutions
  ) {
    log.debug(
        "Received request for S3-ORC with model: {} , threads: {} , batches: {}, rows: {}",
        modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelFilePath, threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("S3-ORC"), null);
  }

  @PostMapping(value = "/adls-csv", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoAdlsCSV(
      @RequestPart(required = false, name = "model_file")
          MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
          Long delayBetweenExecutions
  ) {
    log.debug(
        "Received request for ADLS-CSV with model: {} , threads: {} , batches: {}, rows: {}",
        modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelFilePath, threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("ADLS-CSV"), null);
  }

  @PostMapping(value = "/adls-json", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoAdlsJson(
      @RequestPart(required = false, name = "model_file")
          MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
          Long delayBetweenExecutions
  ) {
    log.debug(
        "Received request for ADLS-JSON with model: {} , threads: {} , batches: {}, rows: {}",
        modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelFilePath, threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("ADLS-JSON"), null);
  }

  @PostMapping(value = "/adls-avro", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoAdlsAvro(
      @RequestPart(required = false, name = "model_file")
          MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
          Long delayBetweenExecutions
  ) {
    log.debug(
        "Received request for ADLS-AVRO with model: {} , threads: {} , batches: {}, rows: {}",
        modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelFilePath, threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("ADLS-AVRO"), null);
  }

  @PostMapping(value = "/adls-parquet", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoAdlsParquet(
      @RequestPart(required = false, name = "model_file")
          MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
          Long delayBetweenExecutions
  ) {
    log.debug(
        "Received request for ADLS-PARQUET with model: {} , threads: {} , batches: {}, rows: {}",
        modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelFilePath, threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("ADLS-PARQUET"), null);
  }

  @PostMapping(value = "/adls-orc", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoAdlsOrc(
      @RequestPart(required = false, name = "model_file")
          MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
          Long delayBetweenExecutions
  ) {
    log.debug(
        "Received request for ADLS-ORC with model: {} , threads: {} , batches: {}, rows: {}",
        modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelFilePath, threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("ADLS-ORC"), null);
  }


  @PostMapping(value = "/gcs-csv", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoGcsCSV(
      @RequestPart(required = false, name = "model_file")
          MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
          Long delayBetweenExecutions
  ) {
    log.debug(
        "Received request for GCS-CSV with model: {} , threads: {} , batches: {}, rows: {}",
        modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelFilePath, threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("GCS-CSV"), null);
  }

  @PostMapping(value = "/gcs-json", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoGcsJson(
      @RequestPart(required = false, name = "model_file")
          MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
          Long delayBetweenExecutions
  ) {
    log.debug(
        "Received request for GCS-JSON with model: {} , threads: {} , batches: {}, rows: {}",
        modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelFilePath, threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("GCS-JSON"), null);
  }

  @PostMapping(value = "/gcs-avro", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoGcsAvro(
      @RequestPart(required = false, name = "model_file")
          MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
          Long delayBetweenExecutions
  ) {
    log.debug(
        "Received request for GCS-AVRO with model: {} , threads: {} , batches: {}, rows: {}",
        modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelFilePath, threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("GCS-AVRO"), null);
  }

  @PostMapping(value = "/gcs-parquet", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoGcsParquet(
      @RequestPart(required = false, name = "model_file")
          MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
          Long delayBetweenExecutions
  ) {
    log.debug(
        "Received request for GCS-PARQUET with model: {} , threads: {} , batches: {}, rows: {}",
        modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelFilePath, threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("GCS-PARQUET"), null);
  }

  @PostMapping(value = "/gcs-orc", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @ResponseBody
  public String generateIntoGcsOrc(
      @RequestPart(required = false, name = "model_file")
          MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "delay_between_executions_seconds")
          Long delayBetweenExecutions
  ) {
    log.debug(
        "Received request for GCS-ORC with model: {} , threads: {} , batches: {}, rows: {}",
        modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Boolean scheduled = delayBetweenExecutions != null;

    return commandRunnerService.generateData(modelFile, modelFilePath, threads,
        numberOfBatches, rowsPerBatch, scheduled, delayBetweenExecutions,
        Collections.singletonList("GCS-ORC"), null);
  }

}
