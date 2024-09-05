package com.datagen.connector.storage.utils;

import com.datagen.model.Model;
import com.datagen.model.OptionsConverter;
import com.datagen.model.type.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;

@Slf4j
public class ParquetUtils {

  public ParquetUtils() {
    throw new IllegalStateException("Could not initialize this class");
  }

  /**
   * Create a local Parquet file using a direct parquet writer*
   * @param path
   * @param schema
   * @param writer
   * @param model
   * @return
   */
  public static ParquetWriter<GenericRecord> createLocalFileWithOverwrite(String path, Schema schema, ParquetWriter<GenericRecord> writer, Model model) {
    try {
      FileUtils.deleteLocalFile(path);
      writer = AvroParquetWriter
          .<GenericRecord>builder(new Path(path))
          .withSchema(schema)
          .withConf(new Configuration())
          .withCompressionCodec(CompressionCodecName.SNAPPY)
          .withPageSize((int) model.getOptionsOrDefault(
              OptionsConverter.Options.PARQUET_PAGE_SIZE))
          .withDictionaryEncoding((Boolean) model.getOptionsOrDefault(
              OptionsConverter.Options.PARQUET_DICTIONARY_ENCODING))
          .withDictionaryPageSize((int) model.getOptionsOrDefault(
              OptionsConverter.Options.PARQUET_DICTIONARY_PAGE_SIZE))
          .withRowGroupSize((int) model.getOptionsOrDefault(
              OptionsConverter.Options.PARQUET_ROW_GROUP_SIZE))
          .build();
      log.debug("Successfully created local Parquet file : " + path);

    } catch (IOException e) {
      log.error(
          "Tried to create Parquet local file : " + path + " with no success :",
          e);
    }
    return writer;
  }

  /**
   * Create a local Parquet file using a direct parquet writer*
   * @param path
   * @param schema
   * @param writer
   * @param model
   * @return
   */
  public static ParquetWriter<GenericRecord> createParquetWriter(String path, Schema schema, ParquetWriter<GenericRecord> writer, Model model, Configuration configuration) {
    try {
      writer = AvroParquetWriter
          .<GenericRecord>builder(new Path(path))
          .withSchema(schema)
          .withConf(configuration)
          .withCompressionCodec(CompressionCodecName.SNAPPY)
          .withPageSize((int) model.getOptionsOrDefault(
              OptionsConverter.Options.PARQUET_PAGE_SIZE))
          .withDictionaryEncoding((Boolean) model.getOptionsOrDefault(
              OptionsConverter.Options.PARQUET_DICTIONARY_ENCODING))
          .withDictionaryPageSize((int) model.getOptionsOrDefault(
              OptionsConverter.Options.PARQUET_DICTIONARY_PAGE_SIZE))
          .withRowGroupSize((int) model.getOptionsOrDefault(
              OptionsConverter.Options.PARQUET_ROW_GROUP_SIZE))
          .build();
      log.debug("Successfully created Parquet writer to : " + path);

    } catch (IOException e) {
      log.error(
          "Tried to create Parquet writer to : " + path + " with no success :",
          e);
    }
    return writer;
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
            f = new IntegerField(colName, new HashMap<>(), null, null);
            break;
          case INT64:
            f = new LongField(colName, new HashMap<>(), null, null);
            break;
          case INT96:
          case BINARY:
            f = new StringField(colName, null, new HashMap<>());
            break;
          case FIXED_LEN_BYTE_ARRAY:
            f = new BytesField(colName, null, new HashMap<>());
            break;
          case DOUBLE:
          case FLOAT:
            f = new FloatField(colName, new HashMap<>(), null, null);
            break;
          case BOOLEAN:
            f = new BooleanField(colName, new HashMap<>());
            break;
          default:
            f = new StringField(colName, null, new HashMap<>());
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
