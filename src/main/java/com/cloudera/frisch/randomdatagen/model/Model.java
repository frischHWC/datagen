package com.cloudera.frisch.randomdatagen.model;


import com.cloudera.frisch.randomdatagen.model.conditions.ConditionalEvaluator;
import com.cloudera.frisch.randomdatagen.model.type.Field;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.log4j.Logger;
import org.apache.orc.TypeDescription;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This class represents a model, that will be used to describe:
 * - all fields
 * - Primary keys & fields to bucket on
 * - Tables names
 * - Other options if needed
 * This class describes also how to generate random data
 * It also describe how to initialize certain systems for that model (i.e. table creation)
 */
@Slf4j
@Getter
@Setter
@SuppressWarnings("unchecked")
public class Model<T extends Field> {

    // This is to keep right order of fields
    @Getter @Setter
    private LinkedHashMap<String, T> fields;

    // This is for convenience when generating data
    @Getter @Setter
    private List<String> fieldsRandomName;
    @Getter @Setter
    private List<String> fieldsComputedName;

    @Getter @Setter
    private Map<OptionsConverter.PrimaryKeys, LinkedList<String>> primaryKeys;
    @Getter @Setter
    private Map<OptionsConverter.TableNames, String> tableNames;
    @Getter @Setter
    private Map<OptionsConverter.Options, Object> options;

    /**
     * Constructor that initializes the model and populates it completely
     * (it only assumes that Field objects are already created, probably using instantiateField() from Field class)
     *
     * @param fields      list of fields already instantiated
     * @param primaryKeys map of options of PKs to list of PKs
     * @param tableNames  map of options of Table names to their names
     * @param options     map of other options as String, String
     */
    public Model(LinkedHashMap<String, T> fields, Map<String, List<String>> primaryKeys, Map<String, String> tableNames, Map<String, String> options) {
        this.fields = fields;
        this.fieldsRandomName = fields.entrySet().stream().filter(f -> !f.getValue().computed).map(f -> f.getKey()).collect(Collectors.toList());
        this.fieldsComputedName = fields.entrySet().stream().filter(f -> f.getValue().computed).map(f -> f.getKey()).collect(Collectors.toList());
        this.primaryKeys = convertPrimaryKeys(primaryKeys);
        this.tableNames = convertTableNames(tableNames);
        this.options = convertOptions(options);

        // For all conditions passed, we need to check types used to prepare future comparisons
        this.fields.values().forEach(f -> {
            ConditionalEvaluator ce = f.getConditional();
            if(ce!=null) {
                ce.getConditionLines().forEach(cl -> {
                    if(cl.isLink()) {
                        cl.getLinkToEvaluate().setLinkedFieldType(this);
                    }
                    cl.getListOfConditions().forEach(c -> c.guessColumnType(this));
                });
            }
        });

        // Using Options passed, fields should be updated to take into account extra options passed
        setupFieldHbaseColQualifier((Map<T, String>) this.options.get(OptionsConverter.Options.HBASE_COLUMN_FAMILIES_MAPPING));

        // Verify model before going further
        verifyModel();

        if (log.isDebugEnabled()) {
            log.debug("Model created is : " + this);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(fields.toString());
        sb.append("Primary Keys : [");
        primaryKeys.forEach((pk, fl) -> {
            sb.append(pk);
            sb.append(" : { ");
            sb.append(fl.toString());
            sb.append(" }");
            sb.append(System.getProperty("line.separator"));
        });
        sb.append("]");
        sb.append(System.getProperty("line.separator"));

        sb.append("Table Names : [");
        tableNames.forEach((tb, value) -> {
            sb.append(tb);
            sb.append(" : ");
            sb.append(value);
            sb.append(System.getProperty("line.separator"));
        });
        sb.append(System.getProperty("line.separator"));

        sb.append("Options : [");
        options.forEach((op, value) -> {
            sb.append(op);
            sb.append(" : ");
            sb.append(value.toString());
            sb.append(System.getProperty("line.separator"));
        });
        sb.append(System.getProperty("line.separator"));

        return sb.toString();
    }

    /**
     * Generate random rows based on this model
     *
     * @param number of rows to generate
     * @return list of rows
     */
    public List<Row> generateRandomRows(long number, int threads) {
        List<Row> rows = new ArrayList<>();

        long numberPerThread = number / threads;
        long restOfRowsToCreate = number % threads;
        LinkedList<RowGeneratorThread> threadsStarted = new LinkedList<>();

        for (int i = 0; i < threads; i++){
            long numberOfRowsToGenerate = numberPerThread;
            if(i==0) {
                numberOfRowsToGenerate += restOfRowsToCreate;
            }
            RowGeneratorThread threadToStart =
                new RowGeneratorThread<>(numberOfRowsToGenerate, this, fieldsRandomName, fieldsComputedName, fields);
            threadToStart.start();
            threadsStarted.add(threadToStart);
            log.info("Started 1 thread to generate: " + numberOfRowsToGenerate + " rows ");
        }

        threadsStarted.forEach(t -> {
            try {
                t.join();
                rows.addAll(t.getRows());
            } catch (InterruptedException e) {
                log.warn("A thread was interrupted, its results will not be processed", e);
            }
        });

        return rows;
    }

    public T getFieldFromName(String name) {
        return fields.get(name);
    }

    /**
     * To convert the options passed in String format to a Java object with list of primary keys
     * @param pks
     * @return
     */
    private Map<OptionsConverter.PrimaryKeys, LinkedList<String>> convertPrimaryKeys(Map<String, List<String>> pks) {
        Map<OptionsConverter.PrimaryKeys, LinkedList<String>> pksConverted = new HashMap<>();
        pks.forEach((k, v) -> {
            OptionsConverter.PrimaryKeys pk = OptionsConverter.convertOptionToPrimaryKey(k);
            if (pk != null) {
                pksConverted.put(pk, new LinkedList<>(v));
            }
        });
        return pksConverted;
    }

    /**
     * To convert options passed in String format to Java object
     * @param tbs
     * @return
     */
    private Map<OptionsConverter.TableNames, String> convertTableNames(Map<String, String> tbs) {
        Map<OptionsConverter.TableNames, String> tbsConverted = new HashMap<>();
        tbs.forEach((k, v) -> {
            OptionsConverter.TableNames tb = OptionsConverter.convertOptionToTableNames(k);
            if (tb != null) {
                tbsConverted.put(tb, v);
            }
        });
        return tbsConverted;
    }

    /**
     * Depending on the option passed, option have special treatment to generate special required Object associated with
     *
     * @param ops passed from parsed file (as a map of string -> string)
     * @return options using Options enum and their associated object in form of a map
     */
    private Map<OptionsConverter.Options, Object> convertOptions(Map<String, String> ops) {
        Map<OptionsConverter.Options, Object> optionsFormatted = new HashMap<>();
        ops.forEach((k, v) -> {
            OptionsConverter.Options op = OptionsConverter.convertOptionToOption(k);
            if (op != null) {
                if (op == OptionsConverter.Options.HBASE_COLUMN_FAMILIES_MAPPING) {
                    optionsFormatted.put(op, convertHbaseColFamilyOption(v));
                } else if (op == OptionsConverter.Options.SOLR_REPLICAS || op == OptionsConverter.Options.SOLR_SHARDS
                    || op == OptionsConverter.Options.KUDU_REPLICAS || op == OptionsConverter.Options.HIVE_THREAD_NUMBER
                    || op == OptionsConverter.Options.PARQUET_PAGE_SIZE || op == OptionsConverter.Options.PARQUET_DICTIONARY_PAGE_SIZE
                    || op == OptionsConverter.Options.PARQUET_ROW_GROUP_SIZE || op == OptionsConverter.Options.KAFKA_RETRIES_CONFIG
                    || op == OptionsConverter.Options.OZONE_REPLICATION_FACTOR || op == OptionsConverter.Options.KUDU_BUCKETS
                    || op == OptionsConverter.Options.KUDU_BUFFER ) {
                    optionsFormatted.put(op, Integer.valueOf(v));
                } else if (op == OptionsConverter.Options.ONE_FILE_PER_ITERATION || op == OptionsConverter.Options.HIVE_ON_HDFS
                            || op == OptionsConverter.Options.CSV_HEADER || op == OptionsConverter.Options.PARQUET_DICTIONARY_ENCODING
                            || op == OptionsConverter.Options.DELETE_PREVIOUS) {
                    optionsFormatted.put(op, Boolean.valueOf(v));
                } else if (op == OptionsConverter.Options.HDFS_REPLICATION_FACTOR) {
                    optionsFormatted.put(op, Short.valueOf(v));
                } else {
                    optionsFormatted.put(op, v);
                }
            }
        });
        return optionsFormatted;
    }

    /**
     * Calls to get Options values should go through this method instead of getting the map in order to provide default values in case options is not set
     * Except for hbase column families mapping which is particurlaly handle by a function below
     * @param option
     * @return
     */
    public Object getOptionsOrDefault(OptionsConverter.Options option) {
        Object optionResult = this.options.get(option);
        // Handle default result here
        if( optionResult == null ) {
            switch (option) {
            case SOLR_SHARDS:
            case SOLR_REPLICAS:
            case KUDU_REPLICAS:
            case HIVE_THREAD_NUMBER:
                optionResult = 1;
                break;
            case CSV_HEADER:
            case PARQUET_DICTIONARY_ENCODING:
            case HIVE_ON_HDFS:
            case ONE_FILE_PER_ITERATION:
                optionResult = true;
                break;
            case KAFKA_ACKS_CONFIG:
                optionResult = "all";
                break;
            case KAFKA_MESSAGE_TYPE:
                optionResult = "json";
                break;
            case KAFKA_JAAS_FILE_PATH:
                optionResult = "/home/datagen/jaas/kafka.jaas";
                break;
            case SOLR_JAAS_FILE_PATH:
                optionResult = "/home/datagen/jaas/solr.jaas";
                break;
            case HIVE_TEZ_QUEUE_NAME:
                optionResult = "root.default";
                break;
            case DELETE_PREVIOUS:
                optionResult = false;
                break;
            case PARQUET_PAGE_SIZE:
            case PARQUET_DICTIONARY_PAGE_SIZE:
                optionResult = 1048576;
                break;
            case PARQUET_ROW_GROUP_SIZE:
                optionResult = 134217728;
                break;
            case KAFKA_RETRIES_CONFIG:
            case OZONE_REPLICATION_FACTOR:
                optionResult = 3;
                break;
            case HDFS_REPLICATION_FACTOR:
                optionResult = (short) 3;
                break;
            case KUDU_BUCKETS:
                optionResult= 32;
                break;
            case KUDU_BUFFER:
                optionResult = 100001;
                break;
            case KUDU_FLUSH:
                optionResult = "MANUAL_FLUSH";
                break;
            default: break;
            }
        }
        return optionResult;
    }

    /**
     * Format for HBase column family should be :
     * columnQualifier:ListOfColsSeparatedByAcomma;columnQualifier:ListOfColsSeparatedByAcomma ...
     * Goal is to Convert this option into a map of field to hbase column qualifier
     *
     * @param ops
     * @return
     */
    Map<T, String> convertHbaseColFamilyOption(String ops) {
        Map<T, String> hbaseFamilyColsMap = new HashMap<>();
        for (String s : ops.split(";")) {
            String cq = s.split(":")[0];
            for (String c : s.split(":")[1].split(",")) {
                T field = fields.get(c);
                if (field != null) {
                    hbaseFamilyColsMap.put(field, cq);
                }
            }
        }
        return hbaseFamilyColsMap;
    }

    public Set<String> getHBaseColumnFamilyList() {
        Map<T, String> colFamiliesMap = (Map<T, String>) options.get(OptionsConverter.Options.HBASE_COLUMN_FAMILIES_MAPPING);
        return new HashSet<>(colFamiliesMap.values());
    }

    private void setupFieldHbaseColQualifier(Map<T, String> fieldHbaseColMap) {
        if(fieldHbaseColMap!=null && !fieldHbaseColMap.isEmpty()) {
            fieldHbaseColMap.forEach(Field::setHbaseColumnQualifier);
        }
    }

    public Schema getKuduSchema() {
        List<ColumnSchema> columns = new LinkedList<>();
        fields.forEach((name, f) -> {
            boolean isaPK = false;
            for (String k : getKuduPrimaryKeys()) {
                if (k.equalsIgnoreCase(name)) {isaPK = true;}
            }
            if (isaPK) {
                columns.add(new ColumnSchema.ColumnSchemaBuilder(name, f.getKuduType())
                    .key(true)
                    .build());
            } else {
                columns.add(new ColumnSchema.ColumnSchemaBuilder(name, f.getKuduType())
                    .build());
            }
        });

        return new Schema(columns);
    }

    public List<String> getKuduPrimaryKeys() {
        return primaryKeys.get(OptionsConverter.PrimaryKeys.KUDU_PRIMARY_KEYS);
    }

    public List<String> getKuduRangeKeys() {
        return primaryKeys.get(OptionsConverter.PrimaryKeys.KUDU_RANGE_KEYS);
    }

    public List<String> getKuduHashKeys() {
        return primaryKeys.get(OptionsConverter.PrimaryKeys.KUDU_HASH_KEYS);
    }

    public String getSQLSchema() {
        StringBuilder sb = new StringBuilder();
        sb.append(" ( ");
        fields.forEach((name, f) -> {
            sb.append(name);
            sb.append(" ");
            sb.append(f.getHiveType());
            sb.append(", ");
        });
        sb.deleteCharAt(sb.length() - 2);
        sb.append(") ");
        log.debug("Schema is : " + sb.toString());
        return sb.toString();
    }

    public String getInsertSQLStatement() {
        StringBuilder sb = new StringBuilder();
        sb.append(" ( ");
        fields.forEach((name, f) -> {
            sb.append(name);
            sb.append(", ");
        });
        sb.deleteCharAt(sb.length() - 2);
        sb.append(") VALUES ( ");
        fields.forEach((name, f) -> sb.append("?, "));
        sb.deleteCharAt(sb.length() - 2);
        sb.append(" ) ");
        log.debug("Insert is : " + sb.toString());
        return sb.toString();
    }

    public String getCsvHeader() {
        StringBuilder sb = new StringBuilder();
        fields.forEach((name, f) -> {
            sb.append(name);
            sb.append(",");
        });
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    public org.apache.avro.Schema getAvroSchema() {
        SchemaBuilder.FieldAssembler<org.apache.avro.Schema> schemaBuilder = SchemaBuilder
                .record(tableNames.get(OptionsConverter.TableNames.AVRO_NAME))
                .namespace("org.apache.avro.ipc")
                .fields();

        for(T field: fields.values()) {
            schemaBuilder = schemaBuilder.name(field.name).type(field.getGenericRecordType()).noDefault();
        }

        return schemaBuilder.endRecord();
    }

    public TypeDescription getOrcSchema() {
        TypeDescription typeDescription = TypeDescription.createStruct();
        fields.forEach((name, f) -> typeDescription.addField(name, f.getTypeDescriptionOrc()));
        return typeDescription;
    }

    public Map<String, ColumnVector> createOrcVectors(VectorizedRowBatch batch) {
        LinkedHashMap<String, ColumnVector> hashMap = new LinkedHashMap<>();
        int cols = 0;
        for(T field: fields.values()) {
            hashMap.put(field.getName(), field.getOrcColumnVector(batch, cols));
            cols++;
        }
        return hashMap;
    }

    // TODO: Implement verifications on the model before starting (not two same names of field, primary keys defined)
    // Each field should have a unique name
    // Each field should have a know type
    // Some Fields cannot have length or possible values
    // Some Fields cannot have min/max
    // Possible values must be of same type than field
    // Depending on on what sink is launched, primary keys must be defined on existing columns
    // Ozone bucket and volume should be string between 3-63 characters (No upper case)
    // Kafka topic should not have special characters or "-"
    // Column comparison in conditionals made should be on same column type
    // Conditionals should be made on existing columns
    // Conditionals should not have "nested" conditions (meaning relying on a computed column)
    public void verifyModel() { }

}
