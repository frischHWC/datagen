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


import com.cloudera.frisch.datagen.service.ModelGeneraterSevice;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;


@Slf4j
@RestController
@RequestMapping("/model_generation")
public class ModelGenerationController {

  @Autowired
  private ModelGeneraterSevice modelGeneraterSevice;

  // TODO: Later make it asynchronous for deep analysis especially + Add real deep analysis also

  @PostMapping(value = "/hive")
  public String generateHiveModel(
      @RequestParam(required = true, name = "database") String database,
      @RequestParam(required = true, name = "table") String table,
      @RequestParam(required = false, name = "deep_analysis", defaultValue = "false")
          Boolean deepAnalysis
  ) {
    log.debug(
        "Received request to generate model for Hive DB {} and Table: {} ",
        database, table);
    return modelGeneraterSevice.generateModel("hive", deepAnalysis, null,
        database, table, null, null, null);
  }

  @PostMapping(value = "/csv")
  public String generateCsvModel(
      @RequestParam(required = true, name = "filepath") String filepath,
      @RequestParam(required = false, name = "deep_analysis", defaultValue = "false")
          Boolean deepAnalysis
  ) {
    log.debug(
        "Received request to generate model for CSV Local file: {} ",
        filepath);
    return modelGeneraterSevice.generateModel("csv", deepAnalysis, filepath,
        null, null, null, null, null);
  }

  @PostMapping(value = "/orc")
  public String generateOrcModel(
      @RequestParam(required = true, name = "filepath") String filepath,
      @RequestParam(required = false, name = "deep_analysis", defaultValue = "false")
          Boolean deepAnalysis
  ) {
    log.debug(
        "Received request to generate model for ORC Local file: {} ",
        filepath);
    return modelGeneraterSevice.generateModel("orc", deepAnalysis, filepath,
        null, null, null, null, null);
  }

  @PostMapping(value = "/parquet")
  public String generateParquetModel(
      @RequestParam(required = true, name = "filepath") String filepath,
      @RequestParam(required = false, name = "deep_analysis", defaultValue = "false")
          Boolean deepAnalysis
  ) {
    log.debug(
        "Received request to generate model for Parquet Local file: {} ",
        filepath);
    return modelGeneraterSevice.generateModel("parquet", deepAnalysis, filepath,
        null, null, null, null, null);
  }

  @PostMapping(value = "/avro")
  public String generateAvroModel(
      @RequestParam(required = true, name = "filepath") String filepath,
      @RequestParam(required = false, name = "deep_analysis", defaultValue = "false")
          Boolean deepAnalysis
  ) {
    log.debug(
        "Received request to generate model for Avro Local file: {} ",
        filepath);
    return modelGeneraterSevice.generateModel("avro", deepAnalysis, filepath,
        null, null, null, null, null);
  }


  @PostMapping(value = "/hdfs-csv")
  public String generateHdfsCsvModel(
      @RequestParam(required = true, name = "filepath") String filepath,
      @RequestParam(required = false, name = "deep_analysis", defaultValue = "false")
          Boolean deepAnalysis
  ) {
    log.debug(
        "Received request to generate model for CSV HDFS file: {} ",
        filepath);
    return modelGeneraterSevice.generateModel("hdfs-csv", deepAnalysis,
        filepath, null, null, null, null, null);
  }

  @PostMapping(value = "/hdfs-orc")
  public String generateHdfsOrcModel(
      @RequestParam(required = true, name = "filepath") String filepath,
      @RequestParam(required = false, name = "deep_analysis", defaultValue = "false")
          Boolean deepAnalysis
  ) {
    log.debug(
        "Received request to generate model for ORC HDFS file: {} ",
        filepath);
    return modelGeneraterSevice.generateModel("hdfs-orc", deepAnalysis,
        filepath, null, null, null, null, null);
  }

  @PostMapping(value = "/hdfs-parquet")
  public String generateHdfsParquetModel(
      @RequestParam(required = true, name = "filepath") String filepath,
      @RequestParam(required = false, name = "deep_analysis", defaultValue = "false")
          Boolean deepAnalysis
  ) {
    log.debug(
        "Received request to generate model for Parquet HDFS file: {} ",
        filepath);
    return modelGeneraterSevice.generateModel("hdfs-parquet", deepAnalysis,
        filepath, null, null, null, null, null);
  }

  @PostMapping(value = "/hdfs-avro")
  public String generateHdfsAvroModel(
      @RequestParam(required = true, name = "filepath") String filepath,
      @RequestParam(required = false, name = "deep_analysis", defaultValue = "false")
          Boolean deepAnalysis
  ) {
    log.debug(
        "Received request to generate model for Avro HDFS file: {} ",
        filepath);
    return modelGeneraterSevice.generateModel("hdfs-avro", deepAnalysis,
        filepath, null, null, null, null, null);
  }


  @PostMapping(value = "/ozone-csv")
  public String generateOzoneCsvModel(
      @RequestParam(required = true, name = "volume") String volume,
      @RequestParam(required = true, name = "bucket") String bucket,
      @RequestParam(required = true, name = "key") String key,
      @RequestParam(required = false, name = "deep_analysis", defaultValue = "false")
          Boolean deepAnalysis
  ) {
    log.debug(
        "Received request to generate model for CSV Ozone file: {} in volume: {} inside bucket: {}",
        key, volume, bucket);
    return modelGeneraterSevice.generateModel("ozone-csv", deepAnalysis,
        null, null, null, volume, bucket, key);
  }

  @PostMapping(value = "/ozone-orc")
  public String generateOzoneOrcModel(
      @RequestParam(required = true, name = "volume") String volume,
      @RequestParam(required = true, name = "bucket") String bucket,
      @RequestParam(required = true, name = "key") String key,
      @RequestParam(required = false, name = "deep_analysis", defaultValue = "false")
          Boolean deepAnalysis
  ) {
    log.debug(
        "Received request to generate model for ORC Ozone file: {} in volume: {} inside bucket: {}",
        key, volume, bucket);
    return modelGeneraterSevice.generateModel("ozone-orc", deepAnalysis,
        null, null, null, volume, bucket, key);
  }

  @PostMapping(value = "/ozone-parquet")
  public String generateOzoneParquetModel(
      @RequestParam(required = true, name = "volume") String volume,
      @RequestParam(required = true, name = "bucket") String bucket,
      @RequestParam(required = true, name = "key") String key,
      @RequestParam(required = false, name = "deep_analysis", defaultValue = "false")
          Boolean deepAnalysis
  ) {
    log.debug(
        "Received request to generate model for Parquet Ozone file: {} in volume: {} inside bucket: {}",
        key, volume, bucket);
    return modelGeneraterSevice.generateModel("ozone-parquet", deepAnalysis,
        null, null, null, volume, bucket, key);
  }

  @PostMapping(value = "/ozone-avro")
  public String generateOzoneAvroModel(
      @RequestParam(required = true, name = "volume") String volume,
      @RequestParam(required = true, name = "bucket") String bucket,
      @RequestParam(required = true, name = "key") String key,
      @RequestParam(required = false, name = "deep_analysis", defaultValue = "false")
          Boolean deepAnalysis
  ) {
    log.debug(
        "Received request to generate model for Avro Ozone file: {} in volume: {} inside bucket: {}",
        key, volume, bucket);
    return modelGeneraterSevice.generateModel("ozone-avro", deepAnalysis,
        null, null, null, volume, bucket, key);
  }


  // TODO: implement these connectors

  /*
  @PostMapping(value = "/hbase")
  public String generateHbaseModel(
      @RequestParam(required = true, name = "filepath") String filepath,
      @RequestParam(required = false, name = "deep_analysis", defaultValue = "false") Boolean deepAnalysis
  ) {
    log.debug(
        "Received request to generate model for Avro HDFS file: {} ",
        filepath);
    return modelGeneraterSevice.generateModel("hdfs-avro", deepAnalysis, filepath, null, null);
  }

  @PostMapping(value = "/kafka")
  public String generateKafkaModel(
      @RequestParam(required = true, name = "filepath") String filepath,
      @RequestParam(required = false, name = "deep_analysis", defaultValue = "false") Boolean deepAnalysis
  ) {
    log.debug(
        "Received request to generate model for Avro HDFS file: {} ",
        filepath);
    return modelGeneraterSevice.generateModel("hdfs-avro", deepAnalysis, filepath, null, null);
  }

  @PostMapping(value = "/kudu")
  public String generateKuduModel(
      @RequestParam(required = true, name = "filepath") String filepath,
      @RequestParam(required = false, name = "deep_analysis", defaultValue = "false") Boolean deepAnalysis
  ) {
    log.debug(
        "Received request to generate model for Avro HDFS file: {} ",
        filepath);
    return modelGeneraterSevice.generateModel("hdfs-avro", deepAnalysis, filepath, null, null);
  }

  @PostMapping(value = "/solr")
  public String generateSolRModel(
      @RequestParam(required = true, name = "filepath") String filepath,
      @RequestParam(required = false, name = "deep_analysis", defaultValue = "false") Boolean deepAnalysis
  ) {
    log.debug(
        "Received request to generate model for Avro HDFS file: {} ",
        filepath);
    return modelGeneraterSevice.generateModel("hdfs-avro", deepAnalysis, filepath, null, null);
  }

  */

}
