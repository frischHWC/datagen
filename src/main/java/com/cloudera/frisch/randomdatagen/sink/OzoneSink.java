package com.cloudera.frisch.randomdatagen.sink;


import com.cloudera.frisch.randomdatagen.Utils;
import com.cloudera.frisch.randomdatagen.config.ApplicationConfigs;
import com.cloudera.frisch.randomdatagen.config.PropertiesLoader;
import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.OptionsConverter;
import com.cloudera.frisch.randomdatagen.model.Row;
import com.cloudera.frisch.randomdatagen.sink.storedobjects.OzoneObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.*;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.exceptions.OMException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * This is an Ozone Sink base on 0.4 API
 * Note that it could produce some Timeout on heavy workload but it still inserts correctly
 */
@Slf4j
public class OzoneSink implements SinkInterface {

    private OzoneClient ozClient;
    private ObjectStore objectStore;
    private OzoneVolume volume;
    private final String volumeName;
    private final ReplicationFactor replicationFactor;


    OzoneSink(Model model, Map<ApplicationConfigs, String> properties) {
        this.volumeName = (String) model.getTableNames().get(OptionsConverter.TableNames.OZONE_VOLUME);
        this.replicationFactor = ReplicationFactor.valueOf((int) model.getOptionsOrDefault(OptionsConverter.Options.OZONE_REPLICATION_FACTOR));

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

        } catch (IOException e) {
            log.error("Could not connect and create Volume into Ozone, due to error: ", e);
        }

    }

    @Override
    public void terminate() {
        try {
            ozClient.close();
        } catch (IOException e) {
            log.warn("Could not close properly Ozone connection, due to error: ", e);
        }
    }

    @Override
    public void sendOneBatchOfRows(List<Row> rows) {
        rows.parallelStream().forEach(row -> {
            OzoneObject ob = row.toOzoneObject();
            createBucketIfNotExist(ob.getBucket());
            try {
                OzoneBucket bucket = volume.getBucket(ob.getBucket());
                OzoneOutputStream os = bucket.createKey(ob.getKey(), ob.getValue().length(), ReplicationType.RATIS, replicationFactor, new HashMap<>());
                os.write(ob.getValue().getBytes());
                os.getOutputStream().flush();
                os.close();
            } catch (IOException e) {
                log.error("Could not write row to Ozone volume: " + volume.getName() +
                        " and bucket : " + ob.getBucket() +
                        " and key: " + ob.getKey() + " due to error: ", e);
            }
        });

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


}
