package com.cloudera.frisch.randomdatagen.sink;

import com.cloudera.frisch.randomdatagen.config.ApplicationConfigs;
import com.cloudera.frisch.randomdatagen.config.SinkParser;
import com.cloudera.frisch.randomdatagen.model.Model;
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
