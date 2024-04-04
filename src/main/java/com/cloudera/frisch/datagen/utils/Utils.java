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
package com.cloudera.frisch.datagen.utils;


import com.cloudera.frisch.datagen.config.ApplicationConfigs;
import com.cloudera.frisch.datagen.config.ConnectorParser;
import com.cloudera.frisch.datagen.model.Model;
import com.cloudera.frisch.datagen.model.OptionsConverter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

@Slf4j
public class Utils {

  private Utils() {
    throw new IllegalStateException("Could not initialize this class");
  }

  private static final long oneHour = 1000 * 60 * 60;
  private static final long oneMinute = 1000 * 60;

  private final static DocumentBuilderFactory factory =
      DocumentBuilderFactory.newInstance();

  /**
   * Generates a random AlphaNumeric string of specified length
   *
   * @param n      equals length of the string to generate
   * @param random Random object used to generate random string
   * @return
   */
  public static String getAlphaNumericString(int n, Random random) {
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
   * Setup haddop env by setting up needed Hadoop system property and adding to configuration required files
   *
   * @param config Hadoop configuration to set up
   */
  public static void setupHadoopEnv(Configuration config,
                                    Map<ApplicationConfigs, String> properties) {

    config.addResource(new Path(
        "file://" + properties.get(ApplicationConfigs.HADOOP_CORE_SITE_PATH)));
    config.addResource(new Path(
        "file://" + properties.get(ApplicationConfigs.HADOOP_HDFS_SITE_PATH)));
    config.addResource(new Path(
        "file://" + properties.get(ApplicationConfigs.HADOOP_OZONE_SITE_PATH)));
    config.addResource(new Path(
        "file://" + properties.get(ApplicationConfigs.HADOOP_HBASE_SITE_PATH)));

    System.setProperty("HADOOP_USER_NAME",
        properties.get(ApplicationConfigs.HADOOP_USER));
    System.setProperty("hadoop.home.dir",
        properties.get(ApplicationConfigs.HADOOP_HOME));
  }


  /**
   * Given a time in milliseconds, format it to a better human comprehensive way
   *
   * @param timeTaken
   * @return
   */
  public static String formatTimetaken(long timeTaken) {
    long timeTakenHere = timeTaken;
    String formattedTime = "";

    if (timeTakenHere >= oneHour) {
      formattedTime = (timeTakenHere / oneHour) + "h ";
      timeTakenHere = timeTakenHere % oneHour;
    }

    if (timeTakenHere >= oneMinute) {
      formattedTime += (timeTakenHere / oneMinute) + "m ";
      timeTakenHere = timeTakenHere % oneMinute;
    }

    if (timeTakenHere > 1000) {
      formattedTime += (timeTakenHere / 1000) + "s ";
      timeTakenHere = timeTakenHere % 1000;
    }

    formattedTime += timeTakenHere + "ms";

    return formattedTime;
  }

  /**
   * Print properly a number human readable
   * @param numberToformat as a long
   * @return formatted number with spaces to be more easily readable
   */
  public static String formatNumber(long numberToformat) {

    StringBuilder reversedStr = new StringBuilder();
    String numberTostring = String.valueOf(numberToformat);

    for (int i=1; i < numberTostring.length()+1; i++)
    {
      reversedStr.append(numberTostring.charAt(numberTostring.length()-i));
      if(i!=numberTostring.length() && i%3==0) {
        reversedStr.append(",");
      }
    }

    return new StringBuilder(reversedStr.toString()).reverse().toString().trim();
  }

  /**
   * Log in the recap of what's been generated
   */
  public static void recap(long numberOfBatches, long rowPerBatch,
                           List<ConnectorParser.Connector> connectors,
                           Model model) {
    log.info(
        " ************************* Recap of data generation ****************** ");
    log.info("Generated " + formatNumber(rowPerBatch * numberOfBatches) +
        " rows into : ");

    connectors.forEach(connector -> {
      switch (connector) {
      case HDFS_CSV:
        log.info("   - HDFS as CSV files of " + formatNumber(rowPerBatch) + " rows : ");
        if ((Boolean) model.getOptionsOrDefault(
            OptionsConverter.Options.ONE_FILE_PER_ITERATION)) {
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
        log.info("   - HDFS as JSON files of " + formatNumber(rowPerBatch) + " rows : ");
        if ((Boolean) model.getOptionsOrDefault(
            OptionsConverter.Options.ONE_FILE_PER_ITERATION)) {
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
        log.info("   - HDFS as Avro files of " + formatNumber(rowPerBatch) + " rows : ");
        if ((Boolean) model.getOptionsOrDefault(
            OptionsConverter.Options.ONE_FILE_PER_ITERATION)) {
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
        log.info("   - HDFS as ORC files of " + formatNumber(rowPerBatch) + " rows : ");
        if ((Boolean) model.getOptionsOrDefault(
            OptionsConverter.Options.ONE_FILE_PER_ITERATION)) {
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
        log.info("   - HDFS as Parquet files of " + formatNumber(rowPerBatch) + " rows : ");
        if ((Boolean) model.getOptionsOrDefault(
            OptionsConverter.Options.ONE_FILE_PER_ITERATION)) {
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
        log.info("   - HBase in namespace " + model.getTableNames()
            .get(OptionsConverter.TableNames.HBASE_NAMESPACE) +
            " in table : " + model.getTableNames()
            .get(OptionsConverter.TableNames.HBASE_TABLE_NAME));
        break;
      case HIVE:
        Model.HiveTableType ht = model.getHiveTableType();
        log.info("   - Hive in database: " + model.getTableNames()
            .get(OptionsConverter.TableNames.HIVE_DATABASE) +
            " in " + ht.toString() + " table : " + model.getTableNames()
            .get(OptionsConverter.TableNames.HIVE_TABLE_NAME));
        if ((Boolean) model.getOptionsOrDefault(
            OptionsConverter.Options.HIVE_ON_HDFS) &&
            ht == Model.HiveTableType.EXTERNAL) {
          log.info(" located in HDFS at: " + model.getTableNames()
              .get(OptionsConverter.TableNames.HIVE_HDFS_FILE_PATH));
        }
        break;
      case OZONE_PARQUET:
        log.info("   - Ozone as Parquet files of " + formatNumber(rowPerBatch) +
                " rows, in volume {} and bucket {} : ",
            model.getTableNames().get(OptionsConverter.TableNames.OZONE_VOLUME),
            model.getTableNames()
                .get(OptionsConverter.TableNames.OZONE_BUCKET));
        if ((Boolean) model.getOptionsOrDefault(
            OptionsConverter.Options.ONE_FILE_PER_ITERATION)) {
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
        log.info("   - Ozone as ORC files of " + formatNumber(rowPerBatch) +
                " rows, in volume {} and bucket {} : ",
            model.getTableNames().get(OptionsConverter.TableNames.OZONE_VOLUME),
            model.getTableNames()
                .get(OptionsConverter.TableNames.OZONE_BUCKET));
        if ((Boolean) model.getOptionsOrDefault(
            OptionsConverter.Options.ONE_FILE_PER_ITERATION)) {
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
        log.info("   - Ozone as Avro files of " + formatNumber(rowPerBatch) +
                " rows, in volume {} and bucket {} : ",
            model.getTableNames().get(OptionsConverter.TableNames.OZONE_VOLUME),
            model.getTableNames()
                .get(OptionsConverter.TableNames.OZONE_BUCKET));
        if ((Boolean) model.getOptionsOrDefault(
            OptionsConverter.Options.ONE_FILE_PER_ITERATION)) {
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
        log.info("   - Ozone as CSV files of " + formatNumber(rowPerBatch) +
                " rows, in volume {} and bucket {} : ",
            model.getTableNames().get(OptionsConverter.TableNames.OZONE_VOLUME),
            model.getTableNames()
                .get(OptionsConverter.TableNames.OZONE_BUCKET));
        if ((Boolean) model.getOptionsOrDefault(
            OptionsConverter.Options.ONE_FILE_PER_ITERATION)) {
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
        log.info("   - Ozone as Json files of " + formatNumber(rowPerBatch) +
                " rows, in volume {} and bucket {} : ",
            model.getTableNames().get(OptionsConverter.TableNames.OZONE_VOLUME),
            model.getTableNames()
                .get(OptionsConverter.TableNames.OZONE_BUCKET));
        if ((Boolean) model.getOptionsOrDefault(
            OptionsConverter.Options.ONE_FILE_PER_ITERATION)) {
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
      case S3_PARQUET:
        log.info("   - S3 as Parquet files of " + formatNumber(rowPerBatch) +
                " rows, in bucket {} : ",
            model.getTableNames()
                .get(OptionsConverter.TableNames.S3_BUCKET));
        if ((Boolean) model.getOptionsOrDefault(
            OptionsConverter.Options.ONE_FILE_PER_ITERATION)) {
          log.info("        From :" + model.getTableNames()
              .get(OptionsConverter.TableNames.S3_KEY_NAME) +
              "-0000000000.parquet");
          log.info("        to : " +
              model.getTableNames()
                  .get(OptionsConverter.TableNames.S3_KEY_NAME) +
              "-" +
              String.format("%010d", numberOfBatches - 1) +
              ".parquet");
        } else {
          log.info("       In One file: " +
              model.getTableNames()
                  .get(OptionsConverter.TableNames.S3_KEY_NAME) +
              ".parquet");
        }
        break;
      case S3_ORC:
        log.info("   - S3 as ORC files of " + formatNumber(rowPerBatch) +
                " rows, in bucket {} : ",
            model.getTableNames()
                .get(OptionsConverter.TableNames.S3_BUCKET));
        if ((Boolean) model.getOptionsOrDefault(
            OptionsConverter.Options.ONE_FILE_PER_ITERATION)) {
          log.info("        From :" + model.getTableNames()
              .get(OptionsConverter.TableNames.S3_KEY_NAME) +
              "-0000000000.orc");
          log.info("        to : " +
              model.getTableNames()
                  .get(OptionsConverter.TableNames.S3_KEY_NAME) +
              "-" +
              String.format("%010d", numberOfBatches - 1) +
              ".orc");
        } else {
          log.info("       In One file: " +
              model.getTableNames()
                  .get(OptionsConverter.TableNames.S3_KEY_NAME) +
              ".orc");
        }
        break;
      case S3_AVRO:
        log.info("   - S3 as Avro files of " + formatNumber(rowPerBatch) +
                " rows, in bucket {} : ",
            model.getTableNames()
                .get(OptionsConverter.TableNames.S3_BUCKET));
        if ((Boolean) model.getOptionsOrDefault(
            OptionsConverter.Options.ONE_FILE_PER_ITERATION)) {
          log.info("        From :" + model.getTableNames()
              .get(OptionsConverter.TableNames.S3_KEY_NAME) +
              "-0000000000.avro");
          log.info("        to : " +
              model.getTableNames()
                  .get(OptionsConverter.TableNames.S3_KEY_NAME) +
              "-" +
              String.format("%010d", numberOfBatches - 1) +
              ".avro");
        } else {
          log.info("       In One file: " +
              model.getTableNames()
                  .get(OptionsConverter.TableNames.S3_KEY_NAME) +
              ".avro");
        }
        break;
      case S3_CSV:
        log.info("   - S3 as CSV files of " + formatNumber(rowPerBatch) +
                " rows, in bucket {} : ",
            model.getTableNames()
                .get(OptionsConverter.TableNames.S3_BUCKET));
        if ((Boolean) model.getOptionsOrDefault(
            OptionsConverter.Options.ONE_FILE_PER_ITERATION)) {
          log.info("        From :" + model.getTableNames()
              .get(OptionsConverter.TableNames.S3_KEY_NAME) +
              "-0000000000.csv");
          log.info("        to : " +
              model.getTableNames()
                  .get(OptionsConverter.TableNames.S3_KEY_NAME) +
              "-" +
              String.format("%010d", numberOfBatches - 1) +
              ".csv");
        } else {
          log.info("       In One file: " +
              model.getTableNames()
                  .get(OptionsConverter.TableNames.S3_KEY_NAME) +
              ".csv");
        }
        break;
      case S3_JSON:
        log.info("   - S3 as Json files of " + formatNumber(rowPerBatch) +
                " rows, in bucket {} : ",
            model.getTableNames()
                .get(OptionsConverter.TableNames.S3_BUCKET));
        if ((Boolean) model.getOptionsOrDefault(
            OptionsConverter.Options.ONE_FILE_PER_ITERATION)) {
          log.info("        From :" + model.getTableNames()
              .get(OptionsConverter.TableNames.S3_KEY_NAME) +
              "-0000000000.json");
          log.info("        to : " +
              model.getTableNames()
                  .get(OptionsConverter.TableNames.S3_KEY_NAME) +
              "-" +
              String.format("%010d", numberOfBatches - 1) +
              ".json");
        } else {
          log.info("       In One file: " +
              model.getTableNames()
                  .get(OptionsConverter.TableNames.S3_KEY_NAME) +
              ".json");
        }
        break;
      case SOLR:
        log.info("   - SolR in collection " + model.getTableNames()
            .get(OptionsConverter.TableNames.SOLR_COLLECTION));
        break;
      case KAFKA:
        log.info("   - Kafka in topic " +
            model.getTableNames().get(OptionsConverter.TableNames.KAFKA_TOPIC));
        break;
      case KUDU:
        log.info("   - Kudu in table " + model.getTableNames()
            .get(OptionsConverter.TableNames.KUDU_TABLE_NAME));
        break;
      case CSV:
        log.info("   - CSV files of " + formatNumber(rowPerBatch) + " rows, from : ");
        log.info("       " +
            model.getTableNames()
                .get(OptionsConverter.TableNames.LOCAL_FILE_PATH) +
            model.getTableNames()
                .get(OptionsConverter.TableNames.LOCAL_FILE_NAME) +
            "-0000000000.csv");
        log.info("       to : ");
        log.info("       " +
            model.getTableNames()
                .get(OptionsConverter.TableNames.LOCAL_FILE_PATH) +
            model.getTableNames()
                .get(OptionsConverter.TableNames.LOCAL_FILE_NAME) + "-" +
            String.format("%010d", numberOfBatches - 1) + ".csv");
        break;
      case JSON:
        log.info("   - JSON files of " + formatNumber(rowPerBatch) + " rows, from : ");
        log.info("       " +
            model.getTableNames()
                .get(OptionsConverter.TableNames.LOCAL_FILE_PATH) +
            model.getTableNames()
                .get(OptionsConverter.TableNames.LOCAL_FILE_NAME) +
            "-0000000000.json");
        log.info("       to : ");
        log.info("       " +
            model.getTableNames()
                .get(OptionsConverter.TableNames.LOCAL_FILE_PATH) +
            model.getTableNames()
                .get(OptionsConverter.TableNames.LOCAL_FILE_NAME) + "-" +
            String.format("%010d", numberOfBatches - 1) + ".json");
        break;
      case AVRO:
        log.info("   - Avro files of " + formatNumber(rowPerBatch) + " rows, from : ");
        log.info("       " +
            model.getTableNames()
                .get(OptionsConverter.TableNames.LOCAL_FILE_PATH) +
            model.getTableNames()
                .get(OptionsConverter.TableNames.LOCAL_FILE_NAME) +
            "-0000000000.avro");
        log.info("       to : ");
        log.info("       " +
            model.getTableNames()
                .get(OptionsConverter.TableNames.LOCAL_FILE_PATH) +
            model.getTableNames()
                .get(OptionsConverter.TableNames.LOCAL_FILE_NAME) + "-" +
            String.format("%010d", numberOfBatches - 1) + ".avro");
        break;
      case PARQUET:
        log.info("   - Parquet files of " + formatNumber(rowPerBatch) + " rows, from : ");
        log.info("       " +
            model.getTableNames()
                .get(OptionsConverter.TableNames.LOCAL_FILE_PATH) +
            model.getTableNames()
                .get(OptionsConverter.TableNames.LOCAL_FILE_NAME) +
            "-0000000000.parquet");
        log.info("       to : ");
        log.info("       " +
            model.getTableNames()
                .get(OptionsConverter.TableNames.LOCAL_FILE_PATH) +
            model.getTableNames()
                .get(OptionsConverter.TableNames.LOCAL_FILE_NAME) + "-" +
            String.format("%010d", numberOfBatches - 1) + ".parquet");
        break;
      case ORC:
        log.info("   - ORC files of " + formatNumber(rowPerBatch) + " rows, from : ");
        log.info("       " +
            model.getTableNames()
                .get(OptionsConverter.TableNames.LOCAL_FILE_PATH) +
            model.getTableNames()
                .get(OptionsConverter.TableNames.LOCAL_FILE_NAME) +
            "-0000000000.orc");
        log.info("       to : ");
        log.info("       " +
            model.getTableNames()
                .get(OptionsConverter.TableNames.LOCAL_FILE_PATH) +
            model.getTableNames()
                .get(OptionsConverter.TableNames.LOCAL_FILE_NAME) + "-" +
            String.format("%010d", numberOfBatches - 1) + ".orc");
        break;
      default:
        log.info("The connector " + connector +
            " provided has not been recognized as an expected connector");
        break;
      }

    });
    log.info(
        "****************************************************************");
  }


  /**
   * Given a path to a local XML file, find the property and return its value as a string *
   * @param filePath to the XML file to parse
   * @param property to find in it
   * @return value found
   */
  public static String getPropertyFromXMLFile(String filePath,
                                              String property) {
    String value = "";
    try {
      DocumentBuilder builder = factory.newDocumentBuilder();
      Document doc = builder.parse(new File(filePath));

      NodeList nodesList = doc.getElementsByTagName("property");
      for (int i = 0; i < nodesList.getLength(); i++) {
        Node propertyNode = nodesList.item(i);
        if (propertyNode.getNodeType() == Node.ELEMENT_NODE) {
          Element propertyElement = (Element) propertyNode;
          if (propertyElement.getElementsByTagName("name").item(0)
              .getTextContent()
              .equalsIgnoreCase(property)) {
            value = propertyElement.getElementsByTagName("value").item(0)
                .getTextContent();
          }
        }
      }

    } catch (Exception e) {
      log.warn(
          "Could not inspect file: {} to find property: {} - returning empty value",
          filePath, property);
      log.debug("Error: ", e);
    }

    log.debug("Return value: {} from file: {} for property: {}", value,
        filePath, property);
    return value;
  }


  /**
   * Given a local SolR configuration file, retrieves and format the SOLR ZK URI*
   * @param filePath of solr configuration file
   * @return SOLR ZK URI value
   */
  public static String getSolrZKQuorumFromEnvsh(String filePath) {
    String value = "";
    try {
      BufferedReader reader;
      reader = new BufferedReader(new FileReader(filePath));
      String line = reader.readLine();
      while (line != null) {
        if (line.contains("SOLR_ZK_ENSEMBLE")) {
          value = line.split("=")[1].split("/")[0];
          break;
        }
        line = reader.readLine();
      }
      reader.close();

    } catch (Exception e) {
      log.warn(
          "Could not inspect file: {} to find solr info - returning empty value",
          filePath);
      log.debug("Error: ", e);
    }

    log.debug("Return value: {} from file: {} for solr", value, filePath);
    return value;
  }


  /**
   * Given a local SolR configuration file, retrieves and format the SOLR Znode *
   * @param filePath of solr configuration file
   * @return SOLR Znode
   */
  public static String getSolrZKznodeFromEnvsh(String filePath) {
    String value = "";
    try {
      BufferedReader reader;
      reader = new BufferedReader(new FileReader(filePath));
      String line = reader.readLine();
      while (line != null) {
        if (line.contains("SOLR_ZK_ENSEMBLE")) {
          value = line.split("=")[1].split("/")[1];
          break;
        }
        line = reader.readLine();
      }
      reader.close();

    } catch (Exception e) {
      log.warn(
          "Could not inspect file: {} to find solr info - returning empty value",
          filePath);
      log.warn("Error: ", e);
    }

    log.debug("Return value: {} from file: {} for solr", value, filePath);
    return value;
  }


  /**
   * Given a local properties file, get all properties that satisifies a prefix *
   * @param filePath of local properties file
   * @param property prefix to retrieve
   * @return a list of all properties that matches the prefix given
   */
  public static List<String> getAllPropertiesKeyFromPropertiesFile(
      String filePath, String property) {
    List<String> values = new ArrayList<>();
    try {
      BufferedReader reader;
      reader = new BufferedReader(new FileReader(filePath));
      String line = reader.readLine();
      while (line != null) {
        if (line.contains(property)) {
          values.add(line.split(":")[0]);
        }
        line = reader.readLine();
      }
      reader.close();

    } catch (Exception e) {
      log.warn(
          "Could not inspect file: {} to find info - returning empty value",
          filePath);
      log.warn("Error: ", e);
    }

    log.debug("Return value: {} from file: {}",
        values.stream().reduce((x, y) -> x + "," + y), filePath);
    return values;
  }

  /**
   * Given a local property file, return value of the property passed *
   * @param filePath to local property file
   * @param property to find
   * @return value of the property seek
   */
  public static String getOnePropertyValueFromPropertiesFile(String filePath,
                                                             String property) {
    String value = "";
    try {
      BufferedReader reader;
      reader = new BufferedReader(new FileReader(filePath));
      String line = reader.readLine();
      while (line != null) {
        if (line.contains(property)) {
          value = line.split("=")[1];
        }
        line = reader.readLine();
      }
      reader.close();

    } catch (Exception e) {
      log.warn(
          "Could not inspect file: {} to find info - returning empty value",
          filePath);
      log.warn("Error: ", e);
    }

    log.debug("Return value: {} from file: {} ", value, filePath);
    return value;
  }

  /**
   * Test on classpath
   * Run it with /opt/cloudera/parcels/CDH-7.0.3-1.cdh7.0.3.p0.1635019/bin/hadoop is different than /opt/cloudera/parcels/CDH/bin/hadoop
   */
  public static void testClasspath() {
    String classpath = System.getProperty("java.class.path");
    String[] paths = classpath.split(System.getProperty("path.separator"));
    log.info("Home is : " + System.getProperty("java.home"));

    try (FileWriter myWriter = new FileWriter("/home/frisch/classpath.txt")) {
      for (String p : paths) {
        myWriter.write(p + System.getProperty("line.separator"));
        log.info("This file is in path : " + p);
      }
      myWriter.flush();
    } catch (IOException e) {
      log.error("Could not write to file", e);
    }
  }


}
