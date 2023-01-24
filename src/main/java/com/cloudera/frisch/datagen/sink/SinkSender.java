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
package com.cloudera.frisch.datagen.sink;

import com.cloudera.frisch.datagen.config.ApplicationConfigs;
import com.cloudera.frisch.datagen.config.SinkParser;
import com.cloudera.frisch.datagen.model.Model;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class SinkSender {

    private SinkSender() { throw new IllegalStateException("Could not initialize this class");}
    

    /**
     * Check what are the all sinks passed and initiates them one by one
     * Each initialization of a sink belongs to it and prepares it to process
     * Moreover, it returns the list of all sinks once initialized
     * @return list of sinks initialized
     */
    @SuppressWarnings("unchecked")
    public static List<SinkInterface> sinksInit(Model model, Map<ApplicationConfigs, String> properties, List<SinkParser.Sink> sinks) {
        List<SinkInterface> sinkList = new ArrayList<>();

        sinks.forEach(sink -> {
            SinkInterface sinkToInitAndStart = null;
            switch (sink) {
                case HDFS_CSV:
                    sinkToInitAndStart = new HdfsCsvSink(model, properties);
                    break;
                case HDFS_JSON:
                    sinkToInitAndStart = new HdfsJsonSink(model, properties);
                    break;
                case HDFS_AVRO:
                    sinkToInitAndStart = new HdfsAvroSink(model, properties);
                    break;
                case HDFS_ORC:
                    sinkToInitAndStart = new HdfsOrcSink(model, properties);
                    break;
                case HDFS_PARQUET:
                    sinkToInitAndStart = new HdfsParquetSink(model, properties);
                    break;
                case HBASE:
                    sinkToInitAndStart = new HbaseSink(model, properties);
                    break;
                case HIVE:
                    sinkToInitAndStart = new HiveSink(model, properties);
                    break;
                case OZONE:
                    sinkToInitAndStart = new OzoneSink(model, properties);
                    break;
                case OZONE_PARQUET:
                    sinkToInitAndStart = new OzoneParquetSink(model, properties);
                    break;
                case OZONE_AVRO:
                    sinkToInitAndStart = new OzoneAvroSink(model, properties);
                    break;
                case OZONE_CSV:
                    sinkToInitAndStart = new OzoneCSVSink(model, properties);
                    break;
                case OZONE_JSON:
                    sinkToInitAndStart = new OzoneJsonSink(model, properties);
                    break;
                case OZONE_ORC:
                    sinkToInitAndStart = new OzoneOrcSink(model, properties);
                    break;
                case SOLR:
                    sinkToInitAndStart = new SolRSink(model, properties);
                    break;
                case KAFKA:
                    sinkToInitAndStart = new KafkaSink(model, properties);
                    break;
                case KUDU:
                    sinkToInitAndStart = new KuduSink(model, properties);
                    break;
                case CSV:
                    sinkToInitAndStart = new CSVSink(model, properties);
                    break;
                case JSON:
                    sinkToInitAndStart = new JsonSink(model, properties);
                    break;
                case AVRO:
                    sinkToInitAndStart = new AvroSink(model, properties);
                    break;
                case PARQUET:
                    sinkToInitAndStart = new ParquetSink(model, properties);
                    break;
                case ORC:
                    sinkToInitAndStart = new ORCSink(model, properties);
                    break;
                 default:
                     log.warn("The sink " + sink + " provided has not been recognized as an expected sink");
                     break;
            }

            if(sinkToInitAndStart != null) {
                log.info(sinkToInitAndStart.getClass().getSimpleName() + " is added to the list of sink");
                sinkList.add(sinkToInitAndStart);
            }

        });
        return sinkList;
    }

    /**
     * Check what are the all sinks passed and terminates them one by one
     * Each initialization of a sink belongs to it and prepares it to
     */
    public static void sinkTerminate(List<SinkInterface> sinks) {
       sinks.forEach(SinkInterface::terminate);
    }
}
