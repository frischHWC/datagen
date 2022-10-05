package com.cloudera.frisch.randomdatagen.sink;


import com.cloudera.frisch.randomdatagen.Utils;
import com.cloudera.frisch.randomdatagen.config.ApplicationConfigs;
import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.OptionsConverter;
import com.cloudera.frisch.randomdatagen.model.Row;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.ozone.client.*;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * This is an Ozone Sink base on 0.4 API
 * Note that it could produce some Timeout on heavy workload but it still inserts correctly
 */
@Slf4j
public class OzoneOrcSink implements SinkInterface {

    private OzoneClient ozClient;
    private ObjectStore objectStore;
    private OzoneVolume volume;
    private final String volumeName;
    private final String bucketName;
    private final String keyNamePrefix;
    private final ReplicationFactor replicationFactor;
    private final String localFileTempDir;

    private final TypeDescription schema;
    private Writer writer;
    private final Map<String, ColumnVector> vectors;
    private final VectorizedRowBatch batch;
    private final Boolean oneFilePerIteration;
    private final Model model;
    private int counter;
    private OzoneBucket bucket;


    OzoneOrcSink(Model model, Map<ApplicationConfigs, String> properties) {
        this.volumeName = (String) model.getTableNames().get(OptionsConverter.TableNames.OZONE_VOLUME);
        this.bucketName = (String) model.getTableNames().get(OptionsConverter.TableNames.OZONE_BUCKET);
        this.keyNamePrefix = (String) model.getTableNames().get(OptionsConverter.TableNames.OZONE_KEY_NAME);
        this.localFileTempDir = (String) model.getTableNames().get(OptionsConverter.TableNames.OZONE_LOCAL_FILE_PATH);
        this.replicationFactor = ReplicationFactor.valueOf((int) model.getOptionsOrDefault(OptionsConverter.Options.OZONE_REPLICATION_FACTOR));
        this.oneFilePerIteration = (Boolean) model.getOptionsOrDefault(OptionsConverter.Options.ONE_FILE_PER_ITERATION);
        this.model = model;
        this.counter = 0;
        this.schema = model.getOrcSchema();
        this.batch = schema.createRowBatch();
        this.vectors = model.createOrcVectors(batch);

        try {
            OzoneConfiguration config = new OzoneConfiguration();
            Utils.setupHadoopEnv(config, properties);

            if (Boolean.parseBoolean(properties.get(ApplicationConfigs.OZONE_AUTH_KERBEROS))) {
                Utils.loginUserWithKerberos(properties.get(ApplicationConfigs.OZONE_AUTH_KERBEROS_USER),
                    properties.get(ApplicationConfigs.OZONE_AUTH_KERBEROS_KEYTAB), config);
            }

            this.ozClient = OzoneClientFactory.getRpcClient(properties.get(ApplicationConfigs.OZONE_SERVICE_ID), config);
            this.objectStore = ozClient.getObjectStore();

            if ((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.DELETE_PREVIOUS)) {
                deleteEverythingUnderAVolume(volumeName);
            }
            createVolumeIfItDoesNotExist(volumeName);
            this.volume = objectStore.getVolume(volumeName);
            createBucketIfNotExist(bucketName);
            this.bucket = volume.getBucket(bucketName);

            // Will use a local directory before pushing data to Ozone
            Utils.createLocalDirectory(localFileTempDir);
            Utils.deleteAllLocalFiles(localFileTempDir, keyNamePrefix , "orc");

            if (!oneFilePerIteration) {
                createLocalFileWithOverwrite(localFileTempDir + keyNamePrefix + ".orc");
            }

        } catch (IOException e) {
            log.error("Could not connect and create Volume into Ozone, due to error: ", e);
        }

    }

    @Override
    public void terminate() {
        try {
            if (!oneFilePerIteration) {
                writer.close();
                // Send local file to Ozone
                String keyName = keyNamePrefix + ".orc";
                try {
                    byte[] dataToWrite = Files.readAllBytes(java.nio.file.Path.of(localFileTempDir + keyName));
                    OzoneOutputStream os = bucket.createKey(keyName, dataToWrite.length, ReplicationType.RATIS, replicationFactor, new HashMap<>());
                    os.write(dataToWrite);
                    os.getOutputStream().flush();
                    os.close();
                } catch (IOException e) {
                    log.error("Could not write row to Ozone volume: {} bucket: {}, key: {} ; error: ", volumeName, bucketName, keyName, e);
                }
            }
            ozClient.close();
            Utils.deleteAllLocalFiles(localFileTempDir, keyNamePrefix , "orc");
        } catch (IOException e) {
            log.warn("Could not close properly Ozone connection, due to error: ", e);
        }
    }

    @Override
    public void sendOneBatchOfRows(List<Row> rows) {
        // Let's create a temp local file and then pushes it to ozone ?
        String keyName = keyNamePrefix + "-" + String.format("%010d", counter) + ".orc";
        // Write to local file
        if (oneFilePerIteration) {
            createLocalFileWithOverwrite( localFileTempDir + keyName);
            counter++;
        }
        for (Row row : rows) {
            int rowNumber = batch.size++;
            row.fillinOrcVector(rowNumber, vectors);
            try {
                if (batch.size == batch.getMaxSize()) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
            } catch (IOException e) {
                log.error("Can not write data to the local file due to error: ", e);
            }
        }
        try {
            if (batch.size != 0) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        } catch (IOException e) {
            log.error("Can not write data to the local file due to error: ", e);
        }

        if (oneFilePerIteration) {
            try {
                writer.close();
            } catch (IOException e) {
                log.error(" Unable to close local file with error :", e);
            }

            // Send local file to Ozone
            try {
                byte[] dataToWrite = Files.readAllBytes(java.nio.file.Path.of(localFileTempDir + keyName));
                OzoneOutputStream os = bucket.createKey(keyName, dataToWrite.length, ReplicationType.RATIS, replicationFactor, new HashMap<>());
                os.write(dataToWrite);
                os.getOutputStream().flush();
                os.close();
            } catch (IOException e) {
                log.error("Could not write row to Ozone volume: {} bucket: {}, key: {} ; error: ", volumeName, bucketName, keyName, e);
            }
            Utils.deleteAllLocalFiles(localFileTempDir, keyNamePrefix , "orc");
        }

    }

    /**
     * Create a bucket if it does not exist
     * In case it exists, it just skips the error and log that bucket already exists
     *
     * @param bucketName
     */
    private void createBucketIfNotExist(String bucketName) {
        try {
            volume.createBucket(bucketName);
            log.debug("Created successfully bucket : " + bucketName + " under volume : " + volume);
        } catch (OMException e) {
            if (e.getResult() == OMException.ResultCodes.BUCKET_ALREADY_EXISTS) {
                log.info("Bucket: " + bucketName + " under volume : " + volume.getName() + " already exists ");
            } else {
                log.error("An error occurred while creating volume " +
                        this.volumeName + " : ", e);
            }
        } catch (IOException e) {
            log.error("Could not create bucket to Ozone volume: " +
                    this.volumeName + " and bucket : " + bucketName + " due to error: ", e);
        }

    }

    /**
     * Try to create a volume if it does not already exist
     */
    private void createVolumeIfItDoesNotExist(String volumeName) {
        try {
            /*
            In class RPCClient of Ozone (which is the one used by default as a ClientProtocol implementation)
            Function createVolume() uses UserGroupInformation.createRemoteUser().getGroupNames() to get groups
            hence it gets all the groups of the logged user and adds them (which is not really good when you're working from a desktop or outside of the cluster machine)
             */
            objectStore.createVolume(volumeName);
        } catch (OMException e) {
            if (e.getResult() == OMException.ResultCodes.VOLUME_ALREADY_EXISTS) {
                log.info("Volume: " + volumeName + " already exists ");
            } else {
                log.error("An error occurred while creating volume " + volumeName + " : ", e);
            }
        } catch (IOException e) {
            log.error("An unexpected exception occurred while creating volume " + volumeName + ": ", e);
        }
    }

    /**
     * Delete all keys in all buckets of a specified volume
     * This is helpful as Ozone does not provide natively this type of function
     *
     * @param volumeName name of the volume to clean and delete
     */
    public void deleteEverythingUnderAVolume(String volumeName) {
        try {
            OzoneVolume volume = objectStore.getVolume(volumeName);

            volume.listBuckets("bucket").forEachRemaining(bucket -> {
                log.debug("Deleting everything in bucket: " + bucket.getName() + " in volume: " + volumeName);
                try {
                    bucket.listKeys(null).forEachRemaining(key -> {
                        try {
                            log.debug("Deleting key: " + key.getName() +
                                " in bucket: " + bucket.getName() +
                                " in volume: " + volumeName);
                            bucket.deleteKey(key.getName());
                        } catch (IOException e) {
                            log.error(
                                "cannot delete key : " + key.getName() +
                                    " in bucket: " + bucket.getName() +
                                    " in volume: " + volumeName +
                                    " due to error: ", e);
                        }
                    });
                } catch (IOException e) {
                    log.error("Could not list keys in bucket " + bucket.getName() + " in volume: " + volumeName);
                }
                try {
                    volume.deleteBucket(bucket.getName());
                } catch (IOException e) {
                    log.error("cannot delete bucket : " + bucket.getName() + " in volume: " + volumeName + " due to error: ", e);
                }
            });

            objectStore.deleteVolume(volumeName);
        } catch (IOException e) {
            log.error("Could not delete volume: " + volumeName + " due to error: ", e);
        }
    }

    private void createLocalFileWithOverwrite(String path) {
        try {
            Utils.deleteLocalFile(path);
            if(!new File(path).getParentFile().mkdirs()) { log.warn("Could not create parent dir");}
            writer = OrcFile.createWriter(new Path(path),
                OrcFile.writerOptions(new Configuration())
                    .setSchema(schema));

        } catch (IOException e) {
            log.error("Tried to create Parquet local file : " + path + " with no success :", e);
        }
    }


}
