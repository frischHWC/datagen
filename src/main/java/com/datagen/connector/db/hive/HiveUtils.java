package com.datagen.connector.db.hive;

import com.datagen.model.type.*;
import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
public class HiveUtils {

  public HiveUtils() {
    throw new IllegalStateException("Could not initialize this class");
  }

  /**
   * Retrieve Table metadata information such as format, location, bucket and sort cols etc...
   *
   * @param tableNames
   * @param options
   * @param descTableInfo
   */
  public static void setTableInfo(
      Map<String, String> tableNames, Map<String, String> options,
      ResultSet descTableInfo) {
    try {
      String hiveTableType = "";
      String serdeLib = "";
      String hiveTableFormat = "";
      String numBuckets = "";
      String location = "";
      String bucketCols = "";
      String partCols = "";

      // Data from a DESCRIBE FORMATTED is represented as 3 columns: col_name, data_type & comment, all of string type
      while (descTableInfo.next()) {
        String colName = descTableInfo.getString("col_name");
        switch (colName) {
        case "SerDe Library:      ":
          serdeLib = descTableInfo.getString("data_type");
          break;
        case "Table Type:         ":
          hiveTableType = descTableInfo.getString("data_type")
              .equalsIgnoreCase("EXTERNAL_TABLE") ? "EXTERNAL" : "MANAGED";
          break;
        case "Num Buckets:        ":
          numBuckets = descTableInfo.getString("data_type").strip();
          break;
        case "Bucket Columns:     ":
          // remove [ and ] from the string and keep only cols separated by a string (and remove whitespace also)
          bucketCols = descTableInfo.getString("data_type").substring(1,
                  descTableInfo.getString("data_type").lastIndexOf(']'))
              .replaceAll("\\\\s+", "");
          break;
        case "Sort Columns:       ":
          // remove [ and ] from the string and keep only cols separated by a string (and remove whitespace also)
          partCols = descTableInfo.getString("data_type").substring(1,
                  descTableInfo.getString("data_type").lastIndexOf(']'))
              .replaceAll("\\\\s+", "");
          break;
        case "Location:           ":
          // Remove hdfs:/ from Location and add '/' at the end
          location = descTableInfo.getString("data_type").substring(6) + "/";
          break;
        default:
          break;
        }
      }

      switch (serdeLib) {
      case "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe":
        hiveTableFormat = "parquet";
        break;
      case "org.apache.hadoop.hive.serde2.avro.AvroSerDe":
        hiveTableFormat = "avro";
        break;
      case "org.apache.hadoop.hive.serde2.JsonSerDe":
        hiveTableFormat = "json";
        break;
      case "org.apache.hadoop.mapred.TextInputFormat":
        hiveTableFormat = "csv";
        break;
      default:
        hiveTableFormat = "orc";
      }

      tableNames.put("HIVE_HDFS_FILE_PATH", location);
      options.put("HIVE_TABLE_TYPE", hiveTableType);
      options.put("HIVE_TABLE_FORMAT", hiveTableFormat);
      if (!partCols.isEmpty()) {
        options.put("HIVE_TABLE_PARTITIONS_COLS", partCols);
      }
      if (!bucketCols.isEmpty()) {
        options.put("HIVE_TABLE_BUCKETS_COLS", bucketCols);
      }
      if (!numBuckets.isEmpty() && !numBuckets.equalsIgnoreCase("-1")) {
        options.put("HIVE_TABLE_BUCKETS_NUMBER", numBuckets);
      }

    } catch (SQLException e) {
      log.warn("Cannot get information on table due to error", e);
    }

  }

  public static void setBasicFields(LinkedHashMap<String, Field> fields,
                                    ResultSet columnsInfo) {
    try {
      while (columnsInfo.next()) {
        String colName = columnsInfo.getString("COLUMN_NAME");
        String coltype = columnsInfo.getString("TYPE_NAME");
        Field f;
        switch (coltype.toUpperCase()) {
        case "INT":
        case "INTEGER":
          f = new IntegerField(colName, Collections.emptyList(),
              new LinkedHashMap<>(), null, null);
          break;
        case "BIGINT":
          f = new LongField(colName, Collections.emptyList(),
              new LinkedHashMap<>(), null, null);
          break;
        case "TINYINT":
          f = new IntegerField(colName, Collections.emptyList(),
              new LinkedHashMap<>(), "-128", "127");
          break;
        case "SMALLINT":
          f = new IntegerField(colName, Collections.emptyList(),
              new LinkedHashMap<>(), "-32768", "32767");
          break;
        case "FLOAT":
        case "DOUBLE":
        case "DOUBLE PRECISION":
        case "DECIMAL":
        case "NUMERIC":
          f = new FloatField(colName, Collections.emptyList(),
              new LinkedHashMap<>(), null, null);
          break;
        case "TIMESTAMP":
          f = new TimestampField(colName, Collections.emptyList());
          break;
        case "DATE":
        case "INTERVAL":
          f = new BirthdateField(colName, null, Collections.emptyList(), null,
              null);
          break;
        case "STRING":
          f = new StringField(colName, null, Collections.emptyList(),
              new LinkedHashMap<>());
          break;
        case "VARCHAR":
          f = new StringAZField(colName, null, Collections.emptyList());
          break;
        case "CHAR":
          f = new StringAZField(colName, 1, Collections.emptyList());
          break;
        case "BOOLEAN":
        case "BINARY":
          f = new BooleanField(colName, Collections.emptyList(),
              new LinkedHashMap<>());
          break;

        default:
          f = new StringField(colName, null, Collections.emptyList(),
              new LinkedHashMap<>());
          break;
        }

        fields.put(colName, f);
      }
    } catch (SQLException e) {
      log.warn("Cannot get info on columns of a table due to error", e);
    }

  }


  /* TODO: Goal here is to analyze each fields inside the parquet file by doing:
   *      - query to get metadata and statistics on the table
   *      - query the table to analyze sample data
   */
  public static void analyzeFields(LinkedHashMap<String, Field> fields,
                                   ResultSet columnsInfo) {
  }

}
