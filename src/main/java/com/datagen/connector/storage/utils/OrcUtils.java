package com.datagen.connector.storage.utils;

import com.datagen.model.type.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;

@Slf4j
public class OrcUtils {

  public OrcUtils() {
    throw new IllegalStateException("Could not initialize this class");
  }

  /**
   * Create a local ORC File with a direct ORC Writer *
   * @param path to the local ORC file to create
   * @param orcWriter
   * @param schema
   * @return
   */
  public static Writer createLocalFileWithOverwrite(String path, Writer orcWriter, TypeDescription schema) {
    try {
      FileUtils.deleteLocalFile(path);
      orcWriter = OrcFile.createWriter(new Path(path),
          OrcFile.writerOptions(new Configuration())
              .setSchema(schema));

    } catch (IOException e) {
      log.error(
          "Tried to create ORC local file : " + path + " with no success :",
          e);
    }
    return orcWriter;
  }

  /**
   * Create a ORC File with a direct ORC Writer *
   * @param path to the local ORC file to create
   * @param orcWriter
   * @param schema
   * @return
   */
  public static Writer createWriter(String path, Writer orcWriter, TypeDescription schema, Configuration configuration) {
    try {
      orcWriter = OrcFile.createWriter(new Path(path),
          OrcFile.writerOptions(configuration)
              .setSchema(schema));

    } catch (IOException e) {
      log.error(
          "Tried to create ORC local file : " + path + " with no success :",
          e);
    }
    return orcWriter;
  }

  /**
   * Render Basically fields by just reading the schema
   *
   * @param fields
   * @param reader
   */
  public static void setBasicFields(LinkedHashMap<String, Field> fields,
                                    Reader reader) {
    TypeDescription schema = reader.getSchema();

    for (int i = 0; i < schema.getFieldNames().size(); i++) {
      String columnName = schema.getFieldNames().get(i);
      TypeDescription.Category columnType =
          schema.getChildren().get(i).getCategory();
      Field f;
      switch (columnType) {
      case BOOLEAN:
      case BINARY:
        f = new BooleanField(columnName, new HashMap<>());
        break;
      case INT:
        f = new IntegerField(columnName, new HashMap<>(), null, null);
        break;
      case LONG:
        f = new LongField(columnName, new HashMap<>(), null, null);
        break;
      case BYTE:
        f = new IntegerField(columnName, new HashMap<>(), -128L, 127L);
        break;
      case SHORT:
        f = new IntegerField(columnName, new HashMap<>(), -32768L, 32767L);
        break;
      case FLOAT:
      case DOUBLE:
      case DECIMAL:
        f = new FloatField(columnName, new HashMap<>(), null, null);
        break;
      case TIMESTAMP:
        f = new TimestampField(columnName, new HashMap<>());
        break;
      case DATE:
        f = new BirthdateField(columnName, null,
            null,
            null);
        break;
      case STRING:
        f = new StringField(columnName, null, new HashMap<>());
        break;
      case VARCHAR:
        f = new StringAZField(columnName, null, new HashMap<>());
        break;
      case CHAR:
        f = new StringAZField(columnName, 1, new HashMap<>());
        break;
      default:
        f = new StringField(columnName, null, new HashMap<>());
      }
      fields.put(columnName, f);
    }
  }

  /* TODO: Goal here is to analyze each fields inside the parquet file by doing:
   *      - check on dictionnaries, statistics etc..
   *      - make min/max and check possible values by analyzing values inside the file
   */
  public static void analyzeFields(LinkedHashMap<String, Field> fields,
                                   Reader reader) {
  }
}
