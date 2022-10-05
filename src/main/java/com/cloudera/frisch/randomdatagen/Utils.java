package com.cloudera.frisch.randomdatagen;


import com.cloudera.frisch.randomdatagen.config.ApplicationConfigs;
import com.cloudera.frisch.randomdatagen.config.SinkParser;
import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.OptionsConverter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.security.UserGroupInformation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

@Slf4j
public class Utils {

    private Utils() { throw new IllegalStateException("Could not initialize this class"); }

    private static final long oneHour = 1000 * 60 *60;
    private static final long oneMinute = 1000 * 60;

    private static DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();


    /**
     * Generates a random password between 5 & 35 characters composed of all possible characters
     * @param random
     * @return
     */
    public static String generateRandomPassword(Random random) {
        byte[] bytesArray = new byte[Math.abs(random.nextInt()%30) + 5];
        random.nextBytes(bytesArray);
        return new String(bytesArray, StandardCharsets.UTF_8);
    }

    /**
     * Generates a random AlphaNumeric string of specified length
     * @param n equals length of the string to generate
     * @param random Random object used to generate random string
     * @return
     */
    public static String getAlphaNumericString(int n, Random random)
    {
        // chose a Character random from this String
        String alphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                + "0123456789"
                + "abcdefghijklmnopqrstuvxyz";
        // create StringBuffer size of alphaNumericString
        StringBuilder sb = new StringBuilder(n);
        for (int i = 0; i < n; i++) {
            // generate a random number between
            // 0 to alphaNumericString variable length
            int index
                    = (int) (alphaNumericString.length()
                    * random.nextDouble());
            // add Character one by one in end of sb
            sb.append(alphaNumericString
                    .charAt(index));
        }
        return sb.toString();
    }

    /**
     * Generates a random Alpha string [A-Z] of specified length
     * @param n equals length of the string to generate
     * @param random Random object used to generate random string
     * @return
     */
    public static String getAlphaString(int n, Random random)
    {
        // chose a Character random from this String
        String alphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                + "abcdefghijklmnopqrstuvxyz";
        // create StringBuffer size of alphaNumericString
        StringBuilder sb = new StringBuilder(n);
        for (int i = 0; i < n; i++) {
            // generate a random number between
            // 0 to alphaNumericString variable length
            int index
                    = (int)(alphaNumericString.length()
                    * random.nextDouble());
            // add Character one by one in end of sb
            sb.append(alphaNumericString
                    .charAt(index));
        }
        return sb.toString();
    }


    /**
     * Using map of possible values weighted (between 0 and 100), it gives possible value
     * @param random
     * @param weights
     * @return
     */
    public static String getRandomValueWithWeights(Random random, LinkedHashMap<String, Integer> weights) {
        int randomIntPercentage = random.nextInt(100);
        int sumOfWeight = 0;
        for(Map.Entry<String, Integer> entry : weights.entrySet()) {
            sumOfWeight = sumOfWeight + entry.getValue();
            if(randomIntPercentage < sumOfWeight) {
                return entry.getKey();
            }
        }
        return "";
    }

    /**
     * Login to kerberos using a given user and its associated keytab
     * @param kerberosUser is the kerberos user
     * @param pathToKeytab path to the keytab associated with the user, note that unix read-right are needed to access it
     * @param config hadoop configuration used further
     */
    public static void loginUserWithKerberos(String kerberosUser, String pathToKeytab, Configuration config) {
        if(config != null) {
            config.set("hadoop.security.authentication", "kerberos");
            UserGroupInformation.setConfiguration(config);
        }
            try {
                UserGroupInformation.loginUserFromKeytab(kerberosUser, pathToKeytab);
            } catch (IOException e) {
                log.error("Could not load keytab file",e);
            }

    }

    /**
     * Setup haddop env by setting up needed Hadoop system property and adding to configuration required files
     * @param config Hadoop configuration to set up
     */
    public static void setupHadoopEnv(Configuration config, Map<ApplicationConfigs, String> properties) {

        config.addResource(new Path("file://"+ properties.get(ApplicationConfigs.HADOOP_CORE_SITE_PATH)));
        config.addResource(new Path("file://"+ properties.get(ApplicationConfigs.HADOOP_HDFS_SITE_PATH)));
        config.addResource(new Path("file://"+ properties.get(ApplicationConfigs.HADOOP_OZONE_SITE_PATH)));
        config.addResource(new Path("file://"+ properties.get(ApplicationConfigs.HADOOP_HBASE_SITE_PATH)));

        System.setProperty("HADOOP_USER_NAME", properties.get(ApplicationConfigs.HADOOP_USER));
        System.setProperty("hadoop.home.dir", properties.get(ApplicationConfigs.HADOOP_HOME));
    }

    /**
     * Delete all local files in a specified directory with a specified extension and a name
     * @param directory
     * @param extension
     */
    public static void deleteAllLocalFiles(String directory, String name, String extension) {
        File folder = new File(directory);
        File[] files = folder.listFiles((dir,f) -> f.matches(name + ".*[.]" + extension));
        for(File f: files){
            log.debug("Will delete local file: " + f);
            if(!f.delete()) { log.warn("Could not delete file: " + f);}
        }
    }

    /**
     * Delete all HDFS files in a specified directory with a specified extension and a name
     * @param directory
     * @param extension
     */
    public static void deleteAllHdfsFiles(FileSystem fileSystem, String directory, String name, String extension) {
        try {
            RemoteIterator<LocatedFileStatus> fileiterator =  fileSystem.listFiles(new Path(directory), false);
            while(fileiterator.hasNext()) {
                LocatedFileStatus file = fileiterator.next();
                if(file.getPath().getName().matches(name + ".*[.]" + extension)) {
                    log.debug("Will delete HDFS file: " + file.getPath());
                    fileSystem.delete(file.getPath(), false);
                }
            }
        } catch (Exception e) {
            log.warn("Could not delete files under " + directory + " due to error: ", e);
        }
    }

    /**
     * Creates a directory on HDFS
     * @param fileSystem
     * @param path
     */
    public static void createHdfsDirectory(FileSystem fileSystem, String path) {
        try {
            fileSystem.mkdirs(new Path(path));
        } catch (IOException e) {
            log.error("Unable to create hdfs directory of : " + path + " due to error: ", e);
        }
    }

    /**
     * Creates a directory locally
     * @param path
     */
    public static void createLocalDirectory(String path) {
        try {
            new File(path).mkdirs();
        } catch (Exception e) {
            log.error("Unable to create directory of : " + path + " due to error: ", e);
        }
    }

    /**
     * Delete a file locally
     * @param path
     */
    public static void deleteLocalFile(String path) {
        try {
            new File(path).delete();
            log.debug("Successfully delete local file : " + path);
        } catch (Exception e) {
            log.error("Tried to delete file : " + path + " with no success :", e);
        }
    }

    /**
     * Create a file locally
     * @param path
     */
    public static void createLocalFile(String path) {
        try {
            new File(path).getParentFile().mkdirs();
            new File(path).createNewFile();
            log.debug("Successfully delete local file : " + path);
        } catch (Exception e) {
            log.error("Tried to delete file : " + path + " with no success :", e);
        }
    }

    /**
     * Delete an HDFS file
     * @param fileSystem
     * @param path
     */
    public static void deleteHdfsFile(FileSystem fileSystem, String path) {
        try {
            fileSystem.delete(new Path(path), true);
            log.debug("Successfully deleted hdfs file : " + path);
        } catch (IOException e) {
            log.error("Tried to delete hdfs file : " + path + " with no success :", e);
        }
    }


    /**
     * Write an JAAS config file that will be used by the application
     * Note that it overrides any existing files and its content
     * @param fileName File path + nam of jaas config file that will be created
     * @param clientName that will represent the client in the JAAS config file
     * @param keytabPath and name of the keytab to put on the file
     * @param principal in the form of principal@REALM as a string
     * @param useKeytab true/false or null if must not be set in the JAAS file
     * @param storeKey true/false or null if must not be set in the JAAS file
     */
    public static void createJaasConfigFile(String fileName, String clientName, String keytabPath, String principal, Boolean useKeytab, Boolean storeKey, Boolean appendToFile) {
        new File(fileName).getParentFile().mkdirs();
        if(!appendToFile) {
            // Destroy previous file if existing
            deleteLocalFile(fileName);
        }
        try(Writer fileWriter = new FileWriter(fileName, appendToFile)) {
            if(Boolean.TRUE.equals(appendToFile)) { fileWriter.append(System.getProperty("line.separator")); }
            fileWriter.append(clientName);
            fileWriter.append(" { ");
            fileWriter.append(System.getProperty("line.separator"));
            fileWriter.append("com.sun.security.auth.module.Krb5LoginModule required");
            fileWriter.append(System.getProperty("line.separator"));
            if(useKeytab!=null) {
                fileWriter.append("useKeyTab=");
                fileWriter.append(useKeytab.toString());
                fileWriter.append(System.getProperty("line.separator"));
            }
            if(storeKey!=null) {
                fileWriter.append("storeKey=");
                fileWriter.append(storeKey.toString());
                fileWriter.append(System.getProperty("line.separator"));
            }
            fileWriter.append("keyTab=\"");
            fileWriter.append(keytabPath);
            fileWriter.append("\"");
            fileWriter.append(System.getProperty("line.separator"));
            fileWriter.append("principal=\"");
            fileWriter.append(principal);
            fileWriter.append("\";");
            fileWriter.append(System.getProperty("line.separator"));
            fileWriter.append("};");
            fileWriter.flush();
        } catch (IOException e) {
            log.error("Could not write proper JAAS config file : " + fileName + " due to error : ", e);
        }
    }

    /**
     * Given a time in milliseconds, format it to a better human comprehensive way
     * @param timeTaken
     * @return
     */
    public static String formatTimetaken(long timeTaken) {
        long timeTakenHere = timeTaken;
        String formattedTime = "";

        if(timeTakenHere >= oneHour ) {
            formattedTime = (timeTakenHere/oneHour) + "h ";
            timeTakenHere = timeTakenHere%oneHour;
        }

        if(timeTakenHere >= oneMinute) {
            formattedTime += (timeTakenHere/oneMinute) + "m ";
            timeTakenHere = timeTakenHere%oneMinute;
        }

        if(timeTakenHere > 1000) {
            formattedTime += (timeTakenHere / 1000) + "s ";
            timeTakenHere = timeTakenHere%1000;
        }

        formattedTime += timeTakenHere + "ms";
        
        return formattedTime;
    }

    /**
     * Test on classpath
     * Run it with /opt/cloudera/parcels/CDH-7.0.3-1.cdh7.0.3.p0.1635019/bin/hadoop is different than /opt/cloudera/parcels/CDH/bin/hadoop
     */
    public static void testClasspath() {
        String classpath = System.getProperty("java.class.path");
        String[] paths = classpath.split(System.getProperty("path.separator"));
        log.info("Home is : " + System.getProperty("java.home"));

        try(FileWriter myWriter = new FileWriter("/home/frisch/classpath.txt")) {
            for(String p: paths) {
                myWriter.write(p + System.getProperty("line.separator"));
                log.info("This file is in path : " + p);
            }
            myWriter.flush();
        } catch (IOException e) {
            log.error("Could not write to file", e);
        }
    }

    /**
     * Log in the recap of what's been generated
     */
    public static void recap(long numberOfBatches, long rowPerBatch, List<SinkParser.Sink> sinks, Model model) {
        log.info(" ************************* Recap of data generation ****************** ");
        log.info("Generated " + formatNumber(rowPerBatch*numberOfBatches) + " rows into : ");

        sinks.forEach(sink -> {
            switch (sink) {
            case HDFS_CSV:
                log.info("   - HDFS as CSV files of " + rowPerBatch + " rows : ");
                if((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.ONE_FILE_PER_ITERATION)) {
                    log.info("       From: " +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.HDFS_FILE_PATH) +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.HDFS_FILE_NAME) +
                        "-0000000000.csv");
                    log.info("       to : " +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.HDFS_FILE_PATH) +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.HDFS_FILE_NAME) +
                        "-" +
                        String.format("%010d", numberOfBatches - 1) + ".csv");
                } else {
                    log.info("       " +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.HDFS_FILE_PATH) +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.HDFS_FILE_NAME) +
                        ".csv");
                }
                break;
            case HDFS_JSON:
                log.info("   - HDFS as JSON files of " + rowPerBatch + " rows : ");
                if((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.ONE_FILE_PER_ITERATION)) {
                    log.info("       From: " +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.HDFS_FILE_PATH) +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.HDFS_FILE_NAME) +
                        "-0000000000.json");
                    log.info("       to : " +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.HDFS_FILE_PATH) +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.HDFS_FILE_NAME) +
                        "-" +
                        String.format("%010d", numberOfBatches - 1) + ".json");
                } else {
                    log.info("       " +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.HDFS_FILE_PATH) +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.HDFS_FILE_NAME) +
                        ".json");
                }
                break;
            case HDFS_AVRO:
                log.info("   - HDFS as Avro files of " + rowPerBatch + " rows : ");
                if((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.ONE_FILE_PER_ITERATION)) {
                    log.info("       From: " +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.HDFS_FILE_PATH) +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.HDFS_FILE_NAME) +
                        "-0000000000.avro");
                    log.info("       to : " +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.HDFS_FILE_PATH) +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.HDFS_FILE_NAME) +
                        "-" +
                        String.format("%010d", numberOfBatches - 1) + ".avro");
                } else {
                    log.info("       " +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.HDFS_FILE_PATH) +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.HDFS_FILE_NAME) +
                        ".avro");
                }
                break;
            case HDFS_ORC:
                log.info("   - HDFS as ORC files of " + rowPerBatch + " rows : ");
                if((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.ONE_FILE_PER_ITERATION)) {
                    log.info("       From: " +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.HDFS_FILE_PATH) +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.HDFS_FILE_NAME) +
                        "-0000000000.orc");
                    log.info("       to : " +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.HDFS_FILE_PATH) +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.HDFS_FILE_NAME) +
                        "-" +
                        String.format("%010d", numberOfBatches - 1) + ".orc");
                } else {
                    log.info("       " +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.HDFS_FILE_PATH) +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.HDFS_FILE_NAME) +
                        ".orc");
                }
                break;
            case HDFS_PARQUET:
                log.info("   - HDFS as Parquet files of " + rowPerBatch + " rows : ");
                if((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.ONE_FILE_PER_ITERATION)) {
                    log.info("       From: " +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.HDFS_FILE_PATH) +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.HDFS_FILE_NAME) +
                        "-0000000000.parquet");
                    log.info("       to : " +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.HDFS_FILE_PATH) +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.HDFS_FILE_NAME) +
                        "-" +
                        String.format("%010d", numberOfBatches - 1) + ".parquet");
                } else {
                    log.info("       " +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.HDFS_FILE_PATH) +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.HDFS_FILE_NAME) +
                        ".parquet");
                }
                break;
            case HBASE:
                log.info("   - HBase in namespace " + model.getTableNames().get(OptionsConverter.TableNames.HBASE_NAMESPACE) +
                    " in table : " + model.getTableNames().get(OptionsConverter.TableNames.HBASE_TABLE_NAME));
                break;
            case HIVE:
                log.info("   - Hive in database: " + model.getTableNames().get(OptionsConverter.TableNames.HIVE_DATABASE) +
                    " in table : " + model.getTableNames().get(OptionsConverter.TableNames.HIVE_TABLE_NAME));
                if((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.HIVE_ON_HDFS)) {
                    String tableNameTemporary = model.getTableNames().get(OptionsConverter.TableNames.HIVE_TEMPORARY_TABLE_NAME)==null ?
                        (String) model.getTableNames().get(OptionsConverter.TableNames.HIVE_TABLE_NAME) + "_tmp" :
                        (String) model.getTableNames().get(OptionsConverter.TableNames.HIVE_TEMPORARY_TABLE_NAME);
                    log.info("   - Hive in database: " + model.getTableNames().get(OptionsConverter.TableNames.HIVE_DATABASE) +
                        " in external table : " + tableNameTemporary + " located in HDFS at: " +
                        model.getTableNames().get(OptionsConverter.TableNames.HIVE_HDFS_FILE_PATH));
                }
                break;
            case OZONE:
                log.info("   - Ozone in volume " + model.getTableNames().get(OptionsConverter.TableNames.OZONE_VOLUME));
                break;
            case OZONE_PARQUET:
                log.info("   - Ozone as Parquet files of " + rowPerBatch + " rows, in volume {} and bucket {} : ",
                    model.getTableNames().get(OptionsConverter.TableNames.OZONE_VOLUME),
                    model.getTableNames().get(OptionsConverter.TableNames.OZONE_BUCKET));
                if((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.ONE_FILE_PER_ITERATION)) {
                    log.info("        From :" + model.getTableNames()
                        .get(OptionsConverter.TableNames.OZONE_KEY_NAME) +
                        "-0000000000.parquet");
                    log.info("        to : " +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.OZONE_KEY_NAME) +
                        "-" +
                        String.format("%010d", numberOfBatches - 1) +
                        ".parquet");
                } else {
                    log.info("       In One file: " +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.OZONE_KEY_NAME) +
                        ".parquet");
                }
                break;
            case OZONE_ORC:
                log.info("   - Ozone as ORC files of " + rowPerBatch + " rows, in volume {} and bucket {} : ",
                    model.getTableNames().get(OptionsConverter.TableNames.OZONE_VOLUME),
                    model.getTableNames().get(OptionsConverter.TableNames.OZONE_BUCKET));
                if((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.ONE_FILE_PER_ITERATION)) {
                    log.info("        From :" + model.getTableNames()
                        .get(OptionsConverter.TableNames.OZONE_KEY_NAME) +
                        "-0000000000.orc");
                    log.info("        to : " +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.OZONE_KEY_NAME) +
                        "-" +
                        String.format("%010d", numberOfBatches - 1) +
                        ".orc");
                } else {
                    log.info("       In One file: " +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.OZONE_KEY_NAME) +
                        ".orc");
                }
                break;
            case OZONE_AVRO:
                log.info("   - Ozone as Avro files of " + rowPerBatch + " rows, in volume {} and bucket {} : ",
                    model.getTableNames().get(OptionsConverter.TableNames.OZONE_VOLUME),
                    model.getTableNames().get(OptionsConverter.TableNames.OZONE_BUCKET));
                if((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.ONE_FILE_PER_ITERATION)) {
                    log.info("        From :" + model.getTableNames()
                        .get(OptionsConverter.TableNames.OZONE_KEY_NAME) +
                        "-0000000000.avro");
                    log.info("        to : " +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.OZONE_KEY_NAME) +
                        "-" +
                        String.format("%010d", numberOfBatches - 1) +
                        ".avro");
                } else {
                    log.info("       In One file: " +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.OZONE_KEY_NAME) +
                        ".avro");
                }
                break;
            case OZONE_CSV:
                log.info("   - Ozone as CSV files of " + rowPerBatch + " rows, in volume {} and bucket {} : ",
                    model.getTableNames().get(OptionsConverter.TableNames.OZONE_VOLUME),
                    model.getTableNames().get(OptionsConverter.TableNames.OZONE_BUCKET));
                if((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.ONE_FILE_PER_ITERATION)) {
                    log.info("        From :" + model.getTableNames()
                        .get(OptionsConverter.TableNames.OZONE_KEY_NAME) +
                        "-0000000000.csv");
                    log.info("        to : " +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.OZONE_KEY_NAME) +
                        "-" +
                        String.format("%010d", numberOfBatches - 1) +
                        ".csv");
                } else {
                    log.info("       In One file: " +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.OZONE_KEY_NAME) +
                        ".csv");
                }
                break;
            case OZONE_JSON:
                log.info("   - Ozone as Json files of " + rowPerBatch + " rows, in volume {} and bucket {} : ",
                    model.getTableNames().get(OptionsConverter.TableNames.OZONE_VOLUME),
                    model.getTableNames().get(OptionsConverter.TableNames.OZONE_BUCKET));
                if((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.ONE_FILE_PER_ITERATION)) {
                    log.info("        From :" + model.getTableNames()
                        .get(OptionsConverter.TableNames.OZONE_KEY_NAME) +
                        "-0000000000.json");
                    log.info("        to : " +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.OZONE_KEY_NAME) +
                        "-" +
                        String.format("%010d", numberOfBatches - 1) +
                        ".json");
                } else {
                    log.info("       In One file: " +
                        model.getTableNames()
                            .get(OptionsConverter.TableNames.OZONE_KEY_NAME) +
                        ".json");
                }
                break;
            case SOLR:
                log.info("   - SolR in collection " + model.getTableNames().get(OptionsConverter.TableNames.SOLR_COLLECTION));
                break;
            case KAFKA:
                log.info("   - Kafka in topic " + model.getTableNames().get(OptionsConverter.TableNames.KAFKA_TOPIC));
                break;
            case KUDU:
                log.info("   - Kudu in table " + model.getTableNames().get(OptionsConverter.TableNames.KUDU_TABLE_NAME));
                break;
            case CSV:
                log.info("   - CSV files of " + rowPerBatch + " rows, from : " );
                log.info("       " +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_NAME) + "-0000000000.csv");
                log.info("       to : ");
                log.info("       " +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_NAME) + "-" +
                    String.format("%010d", numberOfBatches-1) + ".csv");
                break;
            case JSON:
                log.info("   - JSON files of " + rowPerBatch + " rows, from : " );
                log.info("       " +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_NAME) + "-0000000000.json");
                log.info("       to : ");
                log.info("       " +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_NAME) + "-" +
                    String.format("%010d", numberOfBatches-1) + ".json");
                break;
            case AVRO:
                log.info("   - Avro files of " + rowPerBatch + " rows, from : ");
                log.info("       " +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_NAME) + "-0000000000.avro");
                log.info("       to : ");
                log.info("       " +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_NAME) + "-" +
                    String.format("%010d", numberOfBatches-1) + ".avro");
                break;
            case PARQUET:
                log.info("   - Parquet files of " + rowPerBatch + " rows, from : " );
                log.info("       " +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_NAME) + "-0000000000.parquet");
                log.info("       to : ");
                log.info("       " +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_NAME) + "-" +
                    String.format("%010d", numberOfBatches-1) + ".parquet");
                break;
            case ORC:
                log.info("   - ORC files of " + rowPerBatch + " rows, from : " );
                log.info("       " +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_NAME) + "-0000000000.orc");
                log.info("       to : ");
                log.info("       " +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_NAME) + "-" +
                    String.format("%010d", numberOfBatches-1) + ".orc");
                break;
            default:
                log.info("The sink " + sink.toString() +
                    " provided has not been recognized as an expected sink");
                break;
            }

        });
        log.info("****************************************************************");
    }

    public static String formatNumber(long numberToformat) {
        DecimalFormat thousandsFormatter = new DecimalFormat("### ###");
        DecimalFormat millionsFormatter = new DecimalFormat("### ### ###");
        DecimalFormat billionFormatter = new DecimalFormat("### ### ### ###");
        DecimalFormat thousandBillionFormatter = new DecimalFormat("### ### ### ### ###");

        if(numberToformat>999999999999L) {
            return thousandBillionFormatter.format(numberToformat);
        } else if(numberToformat>999999999L) {
            return billionFormatter.format(numberToformat);
        } else if(numberToformat>999999L) {
            return millionsFormatter.format(numberToformat);
        } else {
            return thousandsFormatter.format(numberToformat);
        }
    }


    public static String getPropertyFromXMLFile(String filePath, String property) {
        String value = "";
        try {
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(new File(filePath));

            NodeList nodesList = doc.getElementsByTagName("property");
            for(int i=0; i<nodesList.getLength(); i++) {
                Node propertyNode = nodesList.item(i);
                if(propertyNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element propertyElement = (Element) propertyNode;
                    if(propertyElement.getElementsByTagName("name").item(0).getTextContent()
                        .equalsIgnoreCase(property)) {
                        value = propertyElement.getElementsByTagName("value").item(0).getTextContent();
                    }
                }
            }

        } catch (Exception e) {
            log.error("Could not inspect file: {} to find property: {} - returning empty value", filePath, property);
            log.error("Error: ", e);
        }

        log.debug("Return value: {} from file: {} for property: {}", value, filePath, property);
        return value;
    }


}
