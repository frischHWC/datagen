package com.cloudera.frisch.datagen.connector.storage.utils;

import com.cloudera.frisch.datagen.model.type.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;

@Slf4j
public class AvroUtils {

  public AvroUtils() {
    throw new IllegalStateException("Could not initialize this class");
  }

  /**
   * Render Basically fields by just reading the schema
   *
   * @param fields
   * @param schema
   */
  public static void setBasicFields(LinkedHashMap<String, Field> fields,
                                    Schema schema) {
    schema.getFields().forEach(c -> {
      Field f;
      String colName = c.name();
      switch (c.schema().getType()) {
      case BOOLEAN:
        f = new BooleanField(colName, Collections.emptyList(),
            new LinkedHashMap<>());
        break;
      case DOUBLE:
      case FLOAT:
        f = new FloatField(colName, Collections.emptyList(),
            new LinkedHashMap<>(), null, null);
        break;
      case INT:
        f = new IntegerField(colName, Collections.emptyList(),
            new LinkedHashMap<>(), null, null);
        break;
      case LONG:
        f = new LongField(colName, Collections.emptyList(),
            new LinkedHashMap<>(), null, null);
        break;
      case FIXED:
      case BYTES:
        f = new BytesField(colName, null, Collections.emptyList());
        break;
      case STRING:
      case ENUM:
        f = new StringField(colName, null, Collections.emptyList(),
            new LinkedHashMap<>());
        break;
      case NULL:
        f = new StringField(colName, null, Collections.singletonList("NULL"),
            new LinkedHashMap<>());
        break;
      default:
        f = new StringField(colName, null, Collections.emptyList(),
            new LinkedHashMap<>());
      }
      fields.put(colName, f);
    });
  }

  public static DataFileWriter<GenericRecord> createFileWithOverwrite(String path, Schema schema, DatumWriter<GenericRecord> datumWriter) {
    log.info("Creating local file: {}", path);
    DataFileWriter<GenericRecord> dataFileWriter = null;
    try {
      File file = FileUtils.createLocalFileAsFile(path);
      dataFileWriter = new DataFileWriter<>(datumWriter);
      dataFileWriter.create(schema, file);
      log.info("Successfully created local file : " + path);
    } catch (IOException e) {
      log.error("Tried to create file : " + path + " with no success :", e);
    }
    return dataFileWriter;
  }

  /* TODO: Goal here is to analyze each fields inside the parquet file by doing:
   *      - make min/max and check possible values by analyzing values inside the file
   */
  public static void analyzeFields(LinkedHashMap<String, Field> fields,
                                   FileReader<GenericRecord> fileReader) {
  }
}
