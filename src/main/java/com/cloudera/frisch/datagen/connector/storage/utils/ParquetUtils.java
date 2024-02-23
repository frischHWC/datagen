package com.cloudera.frisch.datagen.connector.storage.utils;

import com.cloudera.frisch.datagen.model.type.*;
import org.apache.parquet.hadoop.ParquetFileReader;

import java.util.Collections;
import java.util.LinkedHashMap;

public class ParquetUtils {

  public ParquetUtils() {
    throw new IllegalStateException("Could not initialize this class");
  }

  /**
   * Render Basically fields by just reading the schema
   *
   * @param fields
   * @param parquetReader
   */
  public static void setBasicFields(LinkedHashMap<String, Field> fields,
                                    ParquetFileReader parquetReader) {
    parquetReader.getFooter().getFileMetaData().getSchema().getColumns()
        .forEach(c -> {
          Field f;
          String colName = c.getPath()[c.getPath().length - 1];

          switch (c.getPrimitiveType().getPrimitiveTypeName()) {
          case INT32:
            f = new IntegerField(colName, Collections.emptyList(),
                new LinkedHashMap<>(), null, null);
            break;
          case INT64:
            f = new LongField(colName, Collections.emptyList(),
                new LinkedHashMap<>(), null, null);
            break;
          case INT96:
          case BINARY:
            f = new StringField(colName, null, Collections.emptyList(),
                new LinkedHashMap<>());
            break;
          case FIXED_LEN_BYTE_ARRAY:
            f = new BytesField(colName, null, Collections.emptyList());
            break;
          case DOUBLE:
          case FLOAT:
            f = new FloatField(colName, Collections.emptyList(),
                new LinkedHashMap<>(), null, null);
            break;
          case BOOLEAN:
            f = new BooleanField(colName, Collections.emptyList(),
                new LinkedHashMap<>());
            break;
          default:
            f = new StringField(colName, null, Collections.emptyList(),
                new LinkedHashMap<>());
          }
          fields.put(colName, f);
        });
  }

  /* TODO: Goal here is to analyze each fields inside the parquet file by doing:
   *      - check on dictionnaries, row groups etc..
   *      - make min/max and check possible values by analyzing values inside the file
   */
  public static void analyzeFields(LinkedHashMap<String, Field> fields,
                                   ParquetFileReader parquetReader) {
  }
}
