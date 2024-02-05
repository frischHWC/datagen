/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.frisch.datagen.connector;

import com.cloudera.frisch.datagen.config.ApplicationConfigs;
import com.cloudera.frisch.datagen.config.SinkParser;
import com.cloudera.frisch.datagen.connector.db.HbaseConnector;
import com.cloudera.frisch.datagen.connector.db.HiveConnector;
import com.cloudera.frisch.datagen.connector.index.SolRConnector;
import com.cloudera.frisch.datagen.connector.queues.KafkaConnector;
import com.cloudera.frisch.datagen.connector.storage.KuduConnector;
import com.cloudera.frisch.datagen.connector.storage.files.*;
import com.cloudera.frisch.datagen.connector.storage.hdfs.*;
import com.cloudera.frisch.datagen.connector.storage.ozone.*;
import com.cloudera.frisch.datagen.model.Model;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Slf4j
public class ConnectorsUtils {

    private ConnectorsUtils() { throw new IllegalStateException("Could not initialize this class");}
    

    /**
     * Check what are the all sinks passed and initiates them one by one
     * Each initialization of a sink belongs to it and prepares it to process
     * Moreover, it returns the list of all sinks once initialized
     * @return list of sinks initialized
     */
    @SuppressWarnings("unchecked")
    public static List<ConnectorInterface> sinksInit(
        Model model,
        Map<ApplicationConfigs, String> properties,
        List<SinkParser.Sink> sinks,
        boolean writer)
    {
        List<ConnectorInterface> connectorList = new LinkedList<>();

        sinks.forEach(sink -> {
            ConnectorInterface connectorToInit = null;
            switch (sink) {
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
                case OZONE:
                    connectorToInit = new OzoneConnector(model, properties);
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
                 default:
                     log.warn("The connector " + sink + " provided has not been recognized as an expected connector");
                     break;
            }

            if(connectorToInit != null) {
                log.info(connectorToInit.getClass().getSimpleName() + " is added to the list of connectors");
                connectorList.add(connectorToInit);
            }

        });
        connectorList.forEach(s -> s.init(model,writer));
        return connectorList;
    }

    /**
     * Check what are the all sinks passed and terminates them one by one
     * Each initialization of a sink belongs to it and prepares it to
     */
    public static void sinkTerminate(List<ConnectorInterface> sinks) {
       sinks.forEach(ConnectorInterface::terminate);
    }
}
