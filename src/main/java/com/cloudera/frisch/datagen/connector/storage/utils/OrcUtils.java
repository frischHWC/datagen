package com.cloudera.frisch.datagen.connector.storage.utils;

import com.cloudera.frisch.datagen.model.type.*;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;

import java.util.Collections;
import java.util.LinkedHashMap;

public class OrcUtils {

  public OrcUtils() {
    throw new IllegalStateException("Could not initialize this class");
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
        f = new BooleanField(columnName, Collections.emptyList(),
            new LinkedHashMap<>());
        break;
      case INT:
        f = new IntegerField(columnName, Collections.emptyList(),
            new LinkedHashMap<>(), null, null);
        break;
      case LONG:
        f = new LongField(columnName, Collections.emptyList(),
            new LinkedHashMap<>(), null, null);
        break;
      case BYTE:
        f = new IntegerField(columnName, Collections.emptyList(),
            new LinkedHashMap<>(), "-128", "127");
        break;
      case SHORT:
        f = new IntegerField(columnName, Collections.emptyList(),
            new LinkedHashMap<>(), "-32768", "32767");
        break;
      case FLOAT:
      case DOUBLE:
      case DECIMAL:
        f = new FloatField(columnName, Collections.emptyList(),
            new LinkedHashMap<>(), null, null);
        break;
      case TIMESTAMP:
        f = new TimestampField(columnName, null, Collections.emptyList());
        break;
      case DATE:
        f = new BirthdateField(columnName, null, Collections.emptyList(),
            null,
            null);
        break;
      case STRING:
        f = new StringField(columnName, null, Collections.emptyList(),
            new LinkedHashMap<>());
        break;
      case VARCHAR:
        f = new StringAZField(columnName, null, Collections.emptyList());
        break;
      case CHAR:
        f = new StringAZField(columnName, 1, Collections.emptyList());
        break;
      default:
        f = new StringField(columnName, null, Collections.emptyList(),
            new LinkedHashMap<>());
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
