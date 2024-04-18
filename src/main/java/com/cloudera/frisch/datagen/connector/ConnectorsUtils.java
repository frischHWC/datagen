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
package com.cloudera.frisch.datagen.connector;

import com.cloudera.frisch.datagen.config.ApplicationConfigs;
import com.cloudera.frisch.datagen.config.ConnectorParser;
import com.cloudera.frisch.datagen.connector.db.hbase.HbaseConnector;
import com.cloudera.frisch.datagen.connector.db.hive.HiveConnector;
import com.cloudera.frisch.datagen.connector.index.SolRConnector;
import com.cloudera.frisch.datagen.connector.queues.KafkaConnector;
import com.cloudera.frisch.datagen.connector.storage.adls.*;
import com.cloudera.frisch.datagen.connector.storage.files.*;
import com.cloudera.frisch.datagen.connector.storage.gcs.*;
import com.cloudera.frisch.datagen.connector.storage.hdfs.*;
import com.cloudera.frisch.datagen.connector.storage.kudu.KuduConnector;
import com.cloudera.frisch.datagen.connector.storage.ozone.*;
import com.cloudera.frisch.datagen.connector.storage.s3.*;
import com.cloudera.frisch.datagen.model.Model;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Slf4j
public class ConnectorsUtils {

  private ConnectorsUtils() {
    throw new IllegalStateException("Could not initialize this class");
  }


  /**
   * Check what are the all connectors passed and initiates them one by one
   * Each initialization of a connector belongs to it and prepares it to process
   * Moreover, it returns the list of all connectors once initialized
   * @return list of connectors initialized
   */
  @SuppressWarnings("unchecked")
  public static List<ConnectorInterface> connectorInit(
      Model model,
      Map<ApplicationConfigs, String> properties,
      List<ConnectorParser.Connector> connectors,
      boolean writer) {
    List<ConnectorInterface> connectorList = new LinkedList<>();

    connectors.forEach(connector -> {
      ConnectorInterface connectorToInit = null;
      switch (connector) {
      case HDFS_CSV:
        connectorToInit = new HdfsCsvConnector(model, properties);
        break;
      case HDFS_JSON:
        connectorToInit = new HdfsJsonConnector(model, properties);
        break;
      case HDFS_AVRO:
        connectorToInit = new HdfsAvroConnector(model, properties);
        break;
      case HDFS_ORC:
        connectorToInit = new HdfsOrcConnector(model, properties);
        break;
      case HDFS_PARQUET:
        connectorToInit = new HdfsParquetConnector(model, properties);
        break;
      case HBASE:
        connectorToInit = new HbaseConnector(model, properties);
        break;
      case HIVE:
        connectorToInit = new HiveConnector(model, properties);
        break;
      case OZONE_PARQUET:
        connectorToInit = new OzoneParquetConnector(model, properties);
        break;
      case OZONE_AVRO:
        connectorToInit = new OzoneAvroConnector(model, properties);
        break;
      case OZONE_CSV:
        connectorToInit = new OzoneCSVConnector(model, properties);
        break;
      case OZONE_JSON:
        connectorToInit = new OzoneJsonConnector(model, properties);
        break;
      case OZONE_ORC:
        connectorToInit = new OzoneOrcConnector(model, properties);
        break;
      case SOLR:
        connectorToInit = new SolRConnector(model, properties);
        break;
      case KAFKA:
        connectorToInit = new KafkaConnector(model, properties);
        break;
      case KUDU:
        connectorToInit = new KuduConnector(model, properties);
        break;
      case CSV:
        connectorToInit = new CSVConnector(model, properties);
        break;
      case JSON:
        connectorToInit = new JsonConnector(model, properties);
        break;
      case AVRO:
        connectorToInit = new AvroConnector(model, properties);
        break;
      case PARQUET:
        connectorToInit = new ParquetConnector(model, properties);
        break;
      case ORC:
        connectorToInit = new ORCConnector(model, properties);
        break;
      case S3_CSV:
        connectorToInit = new S3CSVConnector(model, properties);
        break;
      case S3_JSON:
        connectorToInit = new S3JsonConnector(model, properties);
        break;
      case S3_AVRO:
        connectorToInit = new S3AvroConnector(model, properties);
        break;
      case S3_ORC:
        connectorToInit = new S3OrcConnector(model, properties);
        break;
      case S3_PARQUET:
        connectorToInit = new S3ParquetConnector(model, properties);
        break;
      case ADLS_CSV:
        connectorToInit = new AdlsCSVConnector(model, properties);
        break;
      case ADLS_JSON:
        connectorToInit = new AdlsJsonConnector(model, properties);
        break;
      case ADLS_AVRO:
        connectorToInit = new AdlsAvroConnector(model, properties);
        break;
      case ADLS_ORC:
        connectorToInit = new AdlsOrcConnector(model, properties);
        break;
      case ADLS_PARQUET:
        connectorToInit = new AdlsParquetConnector(model, properties);
        break;
      case GCS_CSV:
        connectorToInit = new GcsCSVConnector(model, properties);
        break;
      case GCS_JSON:
        connectorToInit = new GcsJsonConnector(model, properties);
        break;
      case GCS_AVRO:
        connectorToInit = new GcsAvroConnector(model, properties);
        break;
      case GCS_ORC:
        connectorToInit = new GcsOrcConnector(model, properties);
        break;
      case GCS_PARQUET:
        connectorToInit = new GcsParquetConnector(model, properties);
        break;
      default:
        log.warn("The connector " + connector +
            " provided has not been recognized as an expected connector");
        break;
      }

      if (connectorToInit != null) {
        log.info(connectorToInit.getClass().getSimpleName() +
            " is added to the list of connectors");
        connectorList.add(connectorToInit);
      }

    });
    connectorList.forEach(s -> s.init(model, writer));
    return connectorList;
  }

  /**
   * Check what are the all connectors passed and terminates them one by one
   * Each initialization of a connector belongs to it and prepares it to
   */
  public static void connectorTerminate(List<ConnectorInterface> connectors) {
    connectors.forEach(ConnectorInterface::terminate);
  }
}
