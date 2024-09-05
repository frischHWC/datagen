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
package com.datagen.controller.model;


import com.datagen.config.ConnectorParser;
import com.datagen.service.model.ModelGeneratorSevice;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;


@Slf4j
@RestController
@RequestMapping("/api/v1/model_generation")
public class ModelGenerationController {

  @Autowired
  private ModelGeneratorSevice modelGeneratorSevice;

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
    return modelGeneratorSevice.generateModel(ConnectorParser.Connector.HIVE, deepAnalysis, null,
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
    return modelGeneratorSevice.generateModel(ConnectorParser.Connector.CSV, deepAnalysis, filepath,
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
    return modelGeneratorSevice.generateModel(ConnectorParser.Connector.ORC, deepAnalysis, filepath,
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
    return modelGeneratorSevice.generateModel(ConnectorParser.Connector.PARQUET, deepAnalysis, filepath,
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
    return modelGeneratorSevice.generateModel(ConnectorParser.Connector.AVRO, deepAnalysis, filepath,
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
    return modelGeneratorSevice.generateModel(ConnectorParser.Connector.HDFS_CSV, deepAnalysis,
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
    return modelGeneratorSevice.generateModel(ConnectorParser.Connector.HDFS_ORC, deepAnalysis,
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
    return modelGeneratorSevice.generateModel(ConnectorParser.Connector.HDFS_PARQUET, deepAnalysis,
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
    return modelGeneratorSevice.generateModel(ConnectorParser.Connector.HDFS_AVRO, deepAnalysis,
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
    return modelGeneratorSevice.generateModel(ConnectorParser.Connector.OZONE_CSV, deepAnalysis,
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
    return modelGeneratorSevice.generateModel(ConnectorParser.Connector.OZONE_ORC, deepAnalysis,
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
    return modelGeneratorSevice.generateModel(ConnectorParser.Connector.OZONE_PARQUET, deepAnalysis,
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
    return modelGeneratorSevice.generateModel(ConnectorParser.Connector.OZONE_AVRO, deepAnalysis,
        null, null, null, volume, bucket, key);
  }

  @PostMapping(value = "/s3-avro")
  public String generateS3AvroModel(
      @RequestParam(required = true, name = "bucket") String bucket,
      @RequestParam(required = true, name = "key") String key,
      @RequestParam(required = false, name = "deep_analysis", defaultValue = "false")
          Boolean deepAnalysis
  ) {
    log.debug(
        "Received request to generate model for Avro S3 file: {} in bucket: {}",
        key, bucket);
    return modelGeneratorSevice.generateModel(ConnectorParser.Connector.S3_AVRO, deepAnalysis,
        null, null, null, null, bucket, key);
  }

  @PostMapping(value = "/s3-csv")
  public String generateS3CsvModel(
      @RequestParam(required = true, name = "bucket") String bucket,
      @RequestParam(required = true, name = "key") String key,
      @RequestParam(required = false, name = "deep_analysis", defaultValue = "false")
          Boolean deepAnalysis
  ) {
    log.debug(
        "Received request to generate model for CSV S3 file: {} in bucket: {}",
        key, bucket);
    return modelGeneratorSevice.generateModel(ConnectorParser.Connector.S3_CSV, deepAnalysis,
        null, null, null, null, bucket, key);
  }

  @PostMapping(value = "/s3-json")
  public String generateS3JsonModel(
      @RequestParam(required = true, name = "bucket") String bucket,
      @RequestParam(required = true, name = "key") String key,
      @RequestParam(required = false, name = "deep_analysis", defaultValue = "false")
          Boolean deepAnalysis
  ) {
    log.debug(
        "Received request to generate model for JSON S3 file: {} in bucket: {}",
        key, bucket);
    return modelGeneratorSevice.generateModel(ConnectorParser.Connector.S3_JSON, deepAnalysis,
        null, null, null, null, bucket, key);
  }

  @PostMapping(value = "/s3-parquet")
  public String generateS3ParquetModel(
      @RequestParam(required = true, name = "bucket") String bucket,
      @RequestParam(required = true, name = "key") String key,
      @RequestParam(required = false, name = "deep_analysis", defaultValue = "false")
          Boolean deepAnalysis
  ) {
    log.debug(
        "Received request to generate model for Parquet S3 file: {} in bucket: {}",
        key, bucket);
    return modelGeneratorSevice.generateModel(ConnectorParser.Connector.S3_PARQUET, deepAnalysis,
        null, null, null, null, bucket, key);
  }

  @PostMapping(value = "/s3-orc")
  public String generateS3OrcModel(
      @RequestParam(required = true, name = "bucket") String bucket,
      @RequestParam(required = true, name = "key") String key,
      @RequestParam(required = false, name = "deep_analysis", defaultValue = "false")
          Boolean deepAnalysis
  ) {
    log.debug(
        "Received request to generate model for ORC S3 file: {} in bucket: {}",
        key, bucket);
    return modelGeneratorSevice.generateModel(ConnectorParser.Connector.S3_ORC, deepAnalysis,
        null, null, null, null, bucket, key);
  }

  @PostMapping(value = "/adls-avro")
  public String generateAdlsAvroModel(
      @RequestParam(required = true, name = "bucket") String bucket,
      @RequestParam(required = true, name = "key") String key,
      @RequestParam(required = false, name = "deep_analysis", defaultValue = "false")
          Boolean deepAnalysis
  ) {
    log.debug(
        "Received request to generate model for Avro ADLS file: {} in bucket: {}",
        key, bucket);
    return modelGeneratorSevice.generateModel(ConnectorParser.Connector.ADLS_AVRO, deepAnalysis,
        null, null, null, null, bucket, key);
  }

  @PostMapping(value = "/adls-csv")
  public String generateAdlsCsvModel(
      @RequestParam(required = true, name = "bucket") String bucket,
      @RequestParam(required = true, name = "key") String key,
      @RequestParam(required = false, name = "deep_analysis", defaultValue = "false")
          Boolean deepAnalysis
  ) {
    log.debug(
        "Received request to generate model for ADLS S3 file: {} in bucket: {}",
        key, bucket);
    return modelGeneratorSevice.generateModel(ConnectorParser.Connector.ADLS_CSV, deepAnalysis,
        null, null, null, null, bucket, key);
  }

  @PostMapping(value = "/adls-json")
  public String generateAdlsJsonModel(
      @RequestParam(required = true, name = "bucket") String bucket,
      @RequestParam(required = true, name = "key") String key,
      @RequestParam(required = false, name = "deep_analysis", defaultValue = "false")
          Boolean deepAnalysis
  ) {
    log.debug(
        "Received request to generate model for JSON ADLS file: {} in bucket: {}",
        key, bucket);
    return modelGeneratorSevice.generateModel(ConnectorParser.Connector.ADLS_JSON, deepAnalysis,
        null, null, null, null, bucket, key);
  }

  @PostMapping(value = "/adls-parquet")
  public String generateAdlsParquetModel(
      @RequestParam(required = true, name = "bucket") String bucket,
      @RequestParam(required = true, name = "key") String key,
      @RequestParam(required = false, name = "deep_analysis", defaultValue = "false")
          Boolean deepAnalysis
  ) {
    log.debug(
        "Received request to generate model for Parquet ADLS file: {} in bucket: {}",
        key, bucket);
    return modelGeneratorSevice.generateModel(ConnectorParser.Connector.ADLS_PARQUET, deepAnalysis,
        null, null, null, null, bucket, key);
  }

  @PostMapping(value = "/adls-orc")
  public String generateAdlsOrcModel(
      @RequestParam(required = true, name = "bucket") String bucket,
      @RequestParam(required = true, name = "key") String key,
      @RequestParam(required = false, name = "deep_analysis", defaultValue = "false")
          Boolean deepAnalysis
  ) {
    log.debug(
        "Received request to generate model for ORC ADLS file: {} in bucket: {}",
        key, bucket);
    return modelGeneratorSevice.generateModel(ConnectorParser.Connector.ADLS_ORC, deepAnalysis,
        null, null, null, null, bucket, key);
  }

  @PostMapping(value = "/gcs-avro")
  public String generateGcsAvroModel(
      @RequestParam(required = true, name = "bucket") String bucket,
      @RequestParam(required = true, name = "key") String key,
      @RequestParam(required = false, name = "deep_analysis", defaultValue = "false")
          Boolean deepAnalysis
  ) {
    log.debug(
        "Received request to generate model for Avro GCS file: {} in bucket: {}",
        key, bucket);
    return modelGeneratorSevice.generateModel(ConnectorParser.Connector.GCS_AVRO, deepAnalysis,
        null, null, null, null, bucket, key);
  }

  @PostMapping(value = "/gcs-csv")
  public String generateGcsCsvModel(
      @RequestParam(required = true, name = "bucket") String bucket,
      @RequestParam(required = true, name = "key") String key,
      @RequestParam(required = false, name = "deep_analysis", defaultValue = "false")
          Boolean deepAnalysis
  ) {
    log.debug(
        "Received request to generate model for GCS S3 file: {} in bucket: {}",
        key, bucket);
    return modelGeneratorSevice.generateModel(ConnectorParser.Connector.GCS_CSV, deepAnalysis,
        null, null, null, null, bucket, key);
  }

  @PostMapping(value = "/gcs-json")
  public String generateGcsJsonModel(
      @RequestParam(required = true, name = "bucket") String bucket,
      @RequestParam(required = true, name = "key") String key,
      @RequestParam(required = false, name = "deep_analysis", defaultValue = "false")
          Boolean deepAnalysis
  ) {
    log.debug(
        "Received request to generate model for JSON GCS file: {} in bucket: {}",
        key, bucket);
    return modelGeneratorSevice.generateModel(ConnectorParser.Connector.GCS_JSON, deepAnalysis,
        null, null, null, null, bucket, key);
  }

  @PostMapping(value = "/gcs-parquet")
  public String generateGcsParquetModel(
      @RequestParam(required = true, name = "bucket") String bucket,
      @RequestParam(required = true, name = "key") String key,
      @RequestParam(required = false, name = "deep_analysis", defaultValue = "false")
          Boolean deepAnalysis
  ) {
    log.debug(
        "Received request to generate model for Parquet GCS file: {} in bucket: {}",
        key, bucket);
    return modelGeneratorSevice.generateModel(ConnectorParser.Connector.GCS_PARQUET, deepAnalysis,
        null, null, null, null, bucket, key);
  }

  @PostMapping(value = "/gcs-orc")
  public String generateGcsOrcModel(
      @RequestParam(required = true, name = "bucket") String bucket,
      @RequestParam(required = true, name = "key") String key,
      @RequestParam(required = false, name = "deep_analysis", defaultValue = "false")
          Boolean deepAnalysis
  ) {
    log.debug(
        "Received request to generate model for ORC GCS file: {} in bucket: {}",
        key, bucket);
    return modelGeneratorSevice.generateModel(ConnectorParser.Connector.GCS_PARQUET, deepAnalysis,
        null, null, null, null, bucket, key);
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
