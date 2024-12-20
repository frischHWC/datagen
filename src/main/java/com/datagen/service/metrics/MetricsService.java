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
package com.datagen.service.metrics;

import com.datagen.config.ConnectorParser;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Service
@Slf4j
public class MetricsService {

  @Getter
  private final Map<Metrics, Long> allMetrics;

  /**
   * Init of the metrics to be with all as Os
   */
  MetricsService() {
    allMetrics = new HashMap<>();
    Arrays.stream(Metrics.values()).forEach(m -> allMetrics.put(m, 0L));
  }

  public synchronized void updateMetrics(long numberOfBatches, long rowPerBatch,
                                         List<ConnectorParser.Connector> connectors) {

    synchronized (allMetrics) {
      connectors.forEach(connector -> {
        allMetrics.put(Metrics.ALL_ROWS_GENERATED,
            allMetrics.get(Metrics.ALL_ROWS_GENERATED) +
                (numberOfBatches * rowPerBatch));
        allMetrics.put(Metrics.GENERATIONS_MADE,
            allMetrics.get(Metrics.GENERATIONS_MADE) + 1);

        switch (connector) {
        case HDFS_CSV:
          allMetrics.put(Metrics.HDFS_CSV_FILES_GENERATED,
              allMetrics.get(Metrics.HDFS_CSV_FILES_GENERATED) +
                  numberOfBatches);
          allMetrics.put(Metrics.HDFS_CSV_ROWS_GENERATED,
              allMetrics.get(Metrics.HDFS_CSV_FILES_GENERATED) +
                  (numberOfBatches * rowPerBatch));
          break;
        case HDFS_AVRO:
          allMetrics.put(Metrics.HDFS_AVRO_FILES_GENERATED,
              allMetrics.get(Metrics.HDFS_AVRO_FILES_GENERATED) +
                  numberOfBatches);
          allMetrics.put(Metrics.HDFS_AVRO_ROWS_GENERATED,
              allMetrics.get(Metrics.HDFS_AVRO_ROWS_GENERATED) +
                  (numberOfBatches * rowPerBatch));
          break;
        case HDFS_JSON:
          allMetrics.put(Metrics.HDFS_JSON_FILES_GENERATED,
              allMetrics.get(Metrics.HDFS_JSON_FILES_GENERATED) +
                  numberOfBatches);
          allMetrics.put(Metrics.HDFS_JSON_ROWS_GENERATED,
              allMetrics.get(Metrics.HDFS_JSON_ROWS_GENERATED) +
                  (numberOfBatches * rowPerBatch));
          break;
        case HDFS_ORC:
          allMetrics.put(Metrics.HDFS_ORC_FILES_GENERATED,
              allMetrics.get(Metrics.HDFS_ORC_FILES_GENERATED) +
                  numberOfBatches);
          allMetrics.put(Metrics.HDFS_ORC_ROWS_GENERATED,
              allMetrics.get(Metrics.HDFS_ORC_ROWS_GENERATED) +
                  (numberOfBatches * rowPerBatch));
          break;
        case HDFS_PARQUET:
          allMetrics.put(Metrics.HDFS_PARQUET_FILES_GENERATED,
              allMetrics.get(Metrics.HDFS_PARQUET_FILES_GENERATED) +
                  numberOfBatches);
          allMetrics.put(Metrics.HDFS_PARQUET_ROWS_GENERATED,
              allMetrics.get(Metrics.HDFS_PARQUET_ROWS_GENERATED) +
                  (numberOfBatches * rowPerBatch));
          break;

        case HBASE:
          allMetrics.put(Metrics.HBASE_ROWS_GENERATED,
              allMetrics.get(Metrics.HBASE_ROWS_GENERATED) +
                  (numberOfBatches * rowPerBatch));
          break;

        case HIVE:
          allMetrics.put(Metrics.HIVE_ROWS_GENERATED,
              allMetrics.get(Metrics.HIVE_ROWS_GENERATED) +
                  (numberOfBatches * rowPerBatch));
          break;

        case KAFKA:
          allMetrics.put(Metrics.KAFKA_ROWS_GENERATED,
              allMetrics.get(Metrics.KAFKA_ROWS_GENERATED) +
                  (numberOfBatches * rowPerBatch));
          break;

        case KUDU:
          allMetrics.put(Metrics.KUDU_ROWS_GENERATED,
              allMetrics.get(Metrics.KUDU_ROWS_GENERATED) +
                  (numberOfBatches * rowPerBatch));
          break;

        case SOLR:
          allMetrics.put(Metrics.SOLR_ROWS_GENERATED,
              allMetrics.get(Metrics.SOLR_ROWS_GENERATED) +
                  (numberOfBatches * rowPerBatch));
          break;

        case OZONE_CSV:
          allMetrics.put(Metrics.OZONE_CSV_FILES_GENERATED,
              allMetrics.get(Metrics.OZONE_CSV_FILES_GENERATED) +
                  numberOfBatches);
          allMetrics.put(Metrics.OZONE_CSV_ROWS_GENERATED,
              allMetrics.get(Metrics.OZONE_CSV_ROWS_GENERATED) +
                  (numberOfBatches * rowPerBatch));
          break;
        case OZONE_AVRO:
          allMetrics.put(Metrics.OZONE_AVRO_FILES_GENERATED,
              allMetrics.get(Metrics.OZONE_AVRO_FILES_GENERATED) +
                  numberOfBatches);
          allMetrics.put(Metrics.OZONE_AVRO_ROWS_GENERATED,
              allMetrics.get(Metrics.OZONE_AVRO_ROWS_GENERATED) +
                  (numberOfBatches * rowPerBatch));
          break;
        case OZONE_JSON:
          allMetrics.put(Metrics.OZONE_JSON_FILES_GENERATED,
              allMetrics.get(Metrics.OZONE_JSON_FILES_GENERATED) +
                  numberOfBatches);
          allMetrics.put(Metrics.OZONE_JSON_ROWS_GENERATED,
              allMetrics.get(Metrics.OZONE_JSON_ROWS_GENERATED) +
                  (numberOfBatches * rowPerBatch));
          break;
        case OZONE_ORC:
          allMetrics.put(Metrics.OZONE_ORC_FILES_GENERATED,
              allMetrics.get(Metrics.OZONE_ORC_FILES_GENERATED) +
                  numberOfBatches);
          allMetrics.put(Metrics.OZONE_ORC_ROWS_GENERATED,
              allMetrics.get(Metrics.OZONE_ORC_ROWS_GENERATED) +
                  (numberOfBatches * rowPerBatch));
          break;
        case OZONE_PARQUET:
          allMetrics.put(Metrics.OZONE_PARQUET_FILES_GENERATED,
              allMetrics.get(Metrics.OZONE_PARQUET_FILES_GENERATED) +
                  numberOfBatches);
          allMetrics.put(Metrics.OZONE_PARQUET_ROWS_GENERATED,
              allMetrics.get(Metrics.OZONE_PARQUET_ROWS_GENERATED) +
                  (numberOfBatches * rowPerBatch));
          break;


        case S3_CSV:
          allMetrics.put(Metrics.S3_CSV_FILES_GENERATED,
              allMetrics.get(Metrics.S3_CSV_FILES_GENERATED) +
                  numberOfBatches);
          allMetrics.put(Metrics.S3_CSV_ROWS_GENERATED,
              allMetrics.get(Metrics.S3_CSV_ROWS_GENERATED) +
                  (numberOfBatches * rowPerBatch));
          break;
        case S3_AVRO:
          allMetrics.put(Metrics.S3_AVRO_FILES_GENERATED,
              allMetrics.get(Metrics.S3_AVRO_FILES_GENERATED) +
                  numberOfBatches);
          allMetrics.put(Metrics.S3_AVRO_ROWS_GENERATED,
              allMetrics.get(Metrics.S3_AVRO_ROWS_GENERATED) +
                  (numberOfBatches * rowPerBatch));
          break;
        case S3_JSON:
          allMetrics.put(Metrics.S3_JSON_FILES_GENERATED,
              allMetrics.get(Metrics.S3_JSON_FILES_GENERATED) +
                  numberOfBatches);
          allMetrics.put(Metrics.S3_JSON_ROWS_GENERATED,
              allMetrics.get(Metrics.S3_JSON_ROWS_GENERATED) +
                  (numberOfBatches * rowPerBatch));
          break;
        case S3_ORC:
          allMetrics.put(Metrics.S3_ORC_FILES_GENERATED,
              allMetrics.get(Metrics.S3_ORC_FILES_GENERATED) +
                  numberOfBatches);
          allMetrics.put(Metrics.S3_ORC_ROWS_GENERATED,
              allMetrics.get(Metrics.S3_ORC_ROWS_GENERATED) +
                  (numberOfBatches * rowPerBatch));
          break;
        case S3_PARQUET:
          allMetrics.put(Metrics.S3_PARQUET_FILES_GENERATED,
              allMetrics.get(Metrics.S3_PARQUET_FILES_GENERATED) +
                  numberOfBatches);
          allMetrics.put(Metrics.S3_PARQUET_ROWS_GENERATED,
              allMetrics.get(Metrics.S3_PARQUET_ROWS_GENERATED) +
                  (numberOfBatches * rowPerBatch));
          break;


        case ADLS_CSV:
          allMetrics.put(Metrics.ADLS_CSV_FILES_GENERATED,
              allMetrics.get(Metrics.ADLS_CSV_FILES_GENERATED) +
                  numberOfBatches);
          allMetrics.put(Metrics.ADLS_CSV_ROWS_GENERATED,
              allMetrics.get(Metrics.ADLS_CSV_ROWS_GENERATED) +
                  (numberOfBatches * rowPerBatch));
          break;
        case ADLS_AVRO:
          allMetrics.put(Metrics.ADLS_AVRO_FILES_GENERATED,
              allMetrics.get(Metrics.ADLS_AVRO_FILES_GENERATED) +
                  numberOfBatches);
          allMetrics.put(Metrics.ADLS_AVRO_ROWS_GENERATED,
              allMetrics.get(Metrics.ADLS_AVRO_ROWS_GENERATED) +
                  (numberOfBatches * rowPerBatch));
          break;
        case ADLS_JSON:
          allMetrics.put(Metrics.ADLS_JSON_FILES_GENERATED,
              allMetrics.get(Metrics.ADLS_JSON_FILES_GENERATED) +
                  numberOfBatches);
          allMetrics.put(Metrics.ADLS_JSON_ROWS_GENERATED,
              allMetrics.get(Metrics.ADLS_JSON_ROWS_GENERATED) +
                  (numberOfBatches * rowPerBatch));
          break;
        case ADLS_ORC:
          allMetrics.put(Metrics.ADLS_ORC_FILES_GENERATED,
              allMetrics.get(Metrics.ADLS_ORC_FILES_GENERATED) +
                  numberOfBatches);
          allMetrics.put(Metrics.ADLS_ORC_ROWS_GENERATED,
              allMetrics.get(Metrics.ADLS_ORC_ROWS_GENERATED) +
                  (numberOfBatches * rowPerBatch));
          break;
        case ADLS_PARQUET:
          allMetrics.put(Metrics.ADLS_PARQUET_FILES_GENERATED,
              allMetrics.get(Metrics.ADLS_PARQUET_FILES_GENERATED) +
                  numberOfBatches);
          allMetrics.put(Metrics.ADLS_PARQUET_ROWS_GENERATED,
              allMetrics.get(Metrics.ADLS_PARQUET_ROWS_GENERATED) +
                  (numberOfBatches * rowPerBatch));
          break;

        case GCS_CSV:
          allMetrics.put(Metrics.GCS_CSV_FILES_GENERATED,
              allMetrics.get(Metrics.GCS_CSV_FILES_GENERATED) +
                  numberOfBatches);
          allMetrics.put(Metrics.GCS_CSV_ROWS_GENERATED,
              allMetrics.get(Metrics.GCS_CSV_ROWS_GENERATED) +
                  (numberOfBatches * rowPerBatch));
          break;
        case GCS_AVRO:
          allMetrics.put(Metrics.GCS_AVRO_FILES_GENERATED,
              allMetrics.get(Metrics.GCS_AVRO_FILES_GENERATED) +
                  numberOfBatches);
          allMetrics.put(Metrics.GCS_AVRO_ROWS_GENERATED,
              allMetrics.get(Metrics.GCS_AVRO_ROWS_GENERATED) +
                  (numberOfBatches * rowPerBatch));
          break;
        case GCS_JSON:
          allMetrics.put(Metrics.GCS_JSON_FILES_GENERATED,
              allMetrics.get(Metrics.GCS_JSON_FILES_GENERATED) +
                  numberOfBatches);
          allMetrics.put(Metrics.GCS_JSON_ROWS_GENERATED,
              allMetrics.get(Metrics.GCS_JSON_ROWS_GENERATED) +
                  (numberOfBatches * rowPerBatch));
          break;
        case GCS_ORC:
          allMetrics.put(Metrics.GCS_ORC_FILES_GENERATED,
              allMetrics.get(Metrics.GCS_ORC_FILES_GENERATED) +
                  numberOfBatches);
          allMetrics.put(Metrics.GCS_ORC_ROWS_GENERATED,
              allMetrics.get(Metrics.GCS_ORC_ROWS_GENERATED) +
                  (numberOfBatches * rowPerBatch));
          break;
        case GCS_PARQUET:
          allMetrics.put(Metrics.GCS_PARQUET_FILES_GENERATED,
              allMetrics.get(Metrics.GCS_PARQUET_FILES_GENERATED) +
                  numberOfBatches);
          allMetrics.put(Metrics.GCS_PARQUET_ROWS_GENERATED,
              allMetrics.get(Metrics.GCS_PARQUET_ROWS_GENERATED) +
                  (numberOfBatches * rowPerBatch));
          break;

        case CSV:
          allMetrics.put(Metrics.CSV_FILES_GENERATED,
              allMetrics.get(Metrics.CSV_FILES_GENERATED) + numberOfBatches);
          allMetrics.put(Metrics.CSV_ROWS_GENERATED,
              allMetrics.get(Metrics.CSV_FILES_GENERATED) +
                  (numberOfBatches * rowPerBatch));
          break;
        case AVRO:
          allMetrics.put(Metrics.AVRO_FILES_GENERATED,
              allMetrics.get(Metrics.AVRO_FILES_GENERATED) + numberOfBatches);
          allMetrics.put(Metrics.AVRO_ROWS_GENERATED,
              allMetrics.get(Metrics.AVRO_ROWS_GENERATED) +
                  (numberOfBatches * rowPerBatch));
          break;
        case JSON:
          allMetrics.put(Metrics.JSON_FILES_GENERATED,
              allMetrics.get(Metrics.JSON_FILES_GENERATED) + numberOfBatches);
          allMetrics.put(Metrics.JSON_ROWS_GENERATED,
              allMetrics.get(Metrics.JSON_ROWS_GENERATED) +
                  (numberOfBatches * rowPerBatch));
          break;
        case ORC:
          allMetrics.put(Metrics.ORC_FILES_GENERATED,
              allMetrics.get(Metrics.ORC_FILES_GENERATED) + numberOfBatches);
          allMetrics.put(Metrics.ORC_ROWS_GENERATED,
              allMetrics.get(Metrics.ORC_ROWS_GENERATED) +
                  (numberOfBatches * rowPerBatch));
          break;
        case PARQUET:
          allMetrics.put(Metrics.PARQUET_FILES_GENERATED,
              allMetrics.get(Metrics.PARQUET_FILES_GENERATED) +
                  numberOfBatches);
          allMetrics.put(Metrics.PARQUET_ROWS_GENERATED,
              allMetrics.get(Metrics.PARQUET_ROWS_GENERATED) +
                  (numberOfBatches * rowPerBatch));
          break;

        default:
          log.warn(
              "Could not identify the connector, added metrics only for global");
          break;
        }
      });
    }
  }

  public String getMetricsAsAJson() {
    Long start = System.currentTimeMillis();
    StringBuffer sb = new StringBuffer();

    synchronized (allMetrics) {
      sb.append("{ ");
      sb.append(System.lineSeparator());

      allMetrics.forEach((metrics, s) -> {
        sb.append("\"");
        sb.append(metrics.toString());
        sb.append("\"");
        sb.append(" : ");
        sb.append(s.toString());
        sb.append(",");
        sb.append(System.lineSeparator());
      });

      sb.append("\"ANSWER_TIME\" : ");
      sb.append(System.currentTimeMillis() - start);
      sb.append(System.lineSeparator());
      sb.append(" }");
    }

    return sb.toString();
  }

  public enum Metrics {

    ALL_ROWS_GENERATED,
    GENERATIONS_MADE,

    HDFS_CSV_FILES_GENERATED,
    HDFS_CSV_ROWS_GENERATED,
    HDFS_AVRO_FILES_GENERATED,
    HDFS_AVRO_ROWS_GENERATED,
    HDFS_PARQUET_FILES_GENERATED,
    HDFS_PARQUET_ROWS_GENERATED,
    HDFS_ORC_FILES_GENERATED,
    HDFS_ORC_ROWS_GENERATED,
    HDFS_JSON_FILES_GENERATED,
    HDFS_JSON_ROWS_GENERATED,

    HBASE_ROWS_GENERATED,

    HIVE_ROWS_GENERATED,

    KAFKA_ROWS_GENERATED,

    KUDU_ROWS_GENERATED,

    SOLR_ROWS_GENERATED,

    CSV_FILES_GENERATED,
    CSV_ROWS_GENERATED,
    JSON_FILES_GENERATED,
    JSON_ROWS_GENERATED,
    ORC_FILES_GENERATED,
    ORC_ROWS_GENERATED,
    PARQUET_FILES_GENERATED,
    PARQUET_ROWS_GENERATED,
    AVRO_FILES_GENERATED,
    AVRO_ROWS_GENERATED,

    OZONE_CSV_FILES_GENERATED,
    OZONE_CSV_ROWS_GENERATED,
    OZONE_AVRO_FILES_GENERATED,
    OZONE_AVRO_ROWS_GENERATED,
    OZONE_PARQUET_FILES_GENERATED,
    OZONE_PARQUET_ROWS_GENERATED,
    OZONE_ORC_FILES_GENERATED,
    OZONE_ORC_ROWS_GENERATED,
    OZONE_JSON_FILES_GENERATED,
    OZONE_JSON_ROWS_GENERATED,

    S3_CSV_FILES_GENERATED,
    S3_CSV_ROWS_GENERATED,
    S3_AVRO_FILES_GENERATED,
    S3_AVRO_ROWS_GENERATED,
    S3_PARQUET_FILES_GENERATED,
    S3_PARQUET_ROWS_GENERATED,
    S3_ORC_FILES_GENERATED,
    S3_ORC_ROWS_GENERATED,
    S3_JSON_FILES_GENERATED,
    S3_JSON_ROWS_GENERATED,

    ADLS_CSV_FILES_GENERATED,
    ADLS_CSV_ROWS_GENERATED,
    ADLS_AVRO_FILES_GENERATED,
    ADLS_AVRO_ROWS_GENERATED,
    ADLS_PARQUET_FILES_GENERATED,
    ADLS_PARQUET_ROWS_GENERATED,
    ADLS_ORC_FILES_GENERATED,
    ADLS_ORC_ROWS_GENERATED,
    ADLS_JSON_FILES_GENERATED,
    ADLS_JSON_ROWS_GENERATED,

    GCS_CSV_FILES_GENERATED,
    GCS_CSV_ROWS_GENERATED,
    GCS_AVRO_FILES_GENERATED,
    GCS_AVRO_ROWS_GENERATED,
    GCS_PARQUET_FILES_GENERATED,
    GCS_PARQUET_ROWS_GENERATED,
    GCS_ORC_FILES_GENERATED,
    GCS_ORC_ROWS_GENERATED,
    GCS_JSON_FILES_GENERATED,
    GCS_JSON_ROWS_GENERATED


    }

}
