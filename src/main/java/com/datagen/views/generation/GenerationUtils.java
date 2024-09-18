package com.datagen.views.generation;

import com.datagen.model.Model;
import com.datagen.model.OptionsConverter;
import com.vaadin.flow.component.Component;
import com.vaadin.flow.component.combobox.ComboBox;
import com.vaadin.flow.component.details.Details;
import com.vaadin.flow.component.html.Span;
import com.vaadin.flow.component.orderedlayout.HorizontalLayout;
import com.vaadin.flow.component.orderedlayout.VerticalLayout;
import com.vaadin.flow.component.radiobutton.RadioButtonGroup;
import com.vaadin.flow.component.textfield.IntegerField;
import com.vaadin.flow.component.textfield.NumberField;
import com.vaadin.flow.component.textfield.PasswordField;
import com.vaadin.flow.component.textfield.TextField;
import com.vaadin.flow.data.binder.Binder;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

public class GenerationUtils {

  static TextField createGenericTextfield(Map<OptionsConverter.TableNames, String> tableNamesProps,
                                   OptionsConverter.TableNames parameter,
                                   String label,
                                   String helperText,
                                   Binder<Model> binderModel) {

    var pathText = new TextField(label);
    pathText.setClearButtonVisible(true);
    pathText.setRequired(true);
    pathText.setTooltipText(helperText);
    if (tableNamesProps.get(parameter) != null) {
      pathText.setValue(tableNamesProps.get(parameter));
    }
    binderModel.forField(pathText)
        .bind(
            param -> tableNamesProps.get(parameter) != null?
                tableNamesProps.get(parameter):null,
            (param, newparam) -> { if(newparam!=null) tableNamesProps.put(parameter, newparam);}
        );
    return pathText;
  }

  static TextField createGenericTextfield(Map<OptionsConverter.Options, Object> tableNamesProps,
                                          OptionsConverter.Options parameter,
                                          String label,
                                          String helperText,
                                          Binder<Model> binderModel) {

    var pathText = new TextField(label);
    pathText.setClearButtonVisible(true);
    pathText.setRequired(true);
    pathText.setTooltipText(helperText);
    if (tableNamesProps.get(parameter) != null) {
      pathText.setValue(tableNamesProps.get(parameter).toString());
    }
    binderModel.forField(pathText)
        .bind(
            param -> tableNamesProps.get(parameter) != null?
                tableNamesProps.get(parameter).toString():null,
            (param, newparam) -> { if(newparam!=null) tableNamesProps.put(parameter, newparam);}
        );
    return pathText;
  }

  static PasswordField createGenericPasswordfield(Map<OptionsConverter.TableNames, String> tableNamesProps,
                                              OptionsConverter.TableNames parameter,
                                              String label,
                                              String helperText,
                                              Binder<Model> binderModel) {

    var pathText = new PasswordField(label);
    pathText.setClearButtonVisible(true);
    pathText.setRequired(true);
    pathText.setTooltipText(helperText);
    if (tableNamesProps.get(parameter) != null) {
      pathText.setValue(tableNamesProps.get(parameter));
    }
    binderModel.forField(pathText)
        .bind(
            param -> tableNamesProps.get(parameter) != null?
                tableNamesProps.get(parameter):null,
            (param, newparam) -> { if(newparam!=null) tableNamesProps.put(parameter, newparam);}
        );
    return pathText;
  }

  static IntegerField createGenericIntegerfield(Map<OptionsConverter.Options, Object> tableNamesProps,
                                          OptionsConverter.Options parameter,
                                          String label,
                                          String helperText,
                                          Binder<Model> binderModel,
                                                int min,
                                                int max) {

    var integerField = new IntegerField(label);
    integerField.setClearButtonVisible(true);
    integerField.setRequired(false);
    integerField.setTooltipText(helperText);
    if (tableNamesProps.get(parameter) != null) {
      integerField.setValue((int) tableNamesProps.get(parameter));
    }
    binderModel.forField(integerField)
        .bind(
            param -> tableNamesProps.get(parameter) != null?
                (int) tableNamesProps.get(parameter):null,
            (param, newparam) -> { if(newparam!=null) tableNamesProps.put(parameter, newparam);}
        );
    return integerField;
  }

  static IntegerField createGenericIntegerfieldWithBindToShort(Map<OptionsConverter.Options, Object> tableNamesProps,
                                                OptionsConverter.Options parameter,
                                                String label,
                                                String helperText,
                                                Binder<Model> binderModel,
                                                int min,
                                                int max) {

    var integerField = new IntegerField(label);
    integerField.setClearButtonVisible(true);
    integerField.setRequired(false);
    integerField.setTooltipText(helperText);
    if (tableNamesProps.get(parameter) != null) {
      integerField.setValue((int) tableNamesProps.get(parameter));
    }
    binderModel.forField(integerField)
        .bind(
            param -> tableNamesProps.get(parameter) != null?
                Integer.valueOf((short) tableNamesProps.get(parameter)):null,
            (param, newparam) -> { if(newparam!=null) tableNamesProps.put(parameter, newparam.shortValue());}
        );
    return integerField;
  }



  static RadioButtonGroup createGenericBooleanRadio(
      Map<OptionsConverter.Options, Object> optionsProps,
      OptionsConverter.Options parameter,
      String label,
      Binder<Model> binderModel) {
    RadioButtonGroup<Boolean> radioGroup = new RadioButtonGroup<>();
    radioGroup.setLabel(label);
    radioGroup.setItems(true, false);
    radioGroup.setValue(true);
    binderModel.forField(radioGroup)
        .bind(
            c  -> optionsProps.get(parameter) == null ||
                Boolean.valueOf(optionsProps.get(parameter).toString()),
            (c,m) -> optionsProps.put(parameter, m)
        );
    return radioGroup;
  }

  static RadioButtonGroup createGenericBooleanRadio(
      Map<OptionsConverter.TableNames, String> optionsProps,
      OptionsConverter.TableNames parameter,
      String label,
      Binder<Model> binderModel) {
    RadioButtonGroup<Boolean> radioGroup = new RadioButtonGroup<>();
    radioGroup.setLabel(label);
    radioGroup.setItems(true, false);
    radioGroup.setValue(true);
    binderModel.forField(radioGroup)
        .bind(
            c  -> optionsProps.get(parameter) == null ||
                Boolean.valueOf(optionsProps.get(parameter)),
            (c,m) -> optionsProps.put(parameter, m.toString())
        );
    return radioGroup;
  }

  static RadioButtonGroup createGenericStringRadio(
      Map<OptionsConverter.TableNames, String> optionsProps,
      OptionsConverter.TableNames parameter,
      String label,
      Collection<String> items,
      String defaultValue,
      Binder<Model> binderModel) {
    RadioButtonGroup<String> radioGroup = new RadioButtonGroup<>();
    radioGroup.setLabel(label);
    radioGroup.setItems(items);
    radioGroup.setValue(defaultValue);
    binderModel.forField(radioGroup)
        .bind(
            c  -> optionsProps.get(parameter),
            (c,m) -> optionsProps.put(parameter, m.toString())
        );
    return radioGroup;
  }

  static RadioButtonGroup createGenericStringRadio(
      Map<OptionsConverter.Options, Object> optionsProps,
      OptionsConverter.Options parameter,
      String label,
      Collection<String> items,
      String defaultValue,
      Binder<Model> binderModel) {
    RadioButtonGroup<String> radioGroup = new RadioButtonGroup<>();
    radioGroup.setLabel(label);
    radioGroup.setItems(items);
    radioGroup.setValue(defaultValue);
    binderModel.forField(radioGroup)
        .bind(
            c  -> optionsProps.get(parameter).toString(),
            (c,m) -> optionsProps.put(parameter, m)
        );
    return radioGroup;
  }

  static ComboBox<String> createGenericComboBoxfield(Map<OptionsConverter.Options, Object> tableNamesProps,
                                             OptionsConverter.Options parameter,
                                             String label,
                                             String helperText,
                                             Binder<Model> binderModel,
                                             String defaultValue,
                                             String ... values) {

    var comboBoxField = new ComboBox<String>(label);
    comboBoxField.setTooltipText(helperText);
    comboBoxField.setItems(values);
    comboBoxField.setValue(defaultValue);
    if (tableNamesProps.get(parameter) != null) {
      comboBoxField.setValue(tableNamesProps.get(parameter).toString());
    }
    binderModel.forField(comboBoxField)
        .bind(
            param -> tableNamesProps.get(parameter) != null?
                tableNamesProps.get(parameter).toString():null,
            (param, newparam) -> { if(newparam!=null) tableNamesProps.put(parameter, newparam);}
        );
    return comboBoxField;
  }

  static ComboBox<String> createGenericComboBoxfield(Map<OptionsConverter.TableNames, String> tableNamesProps,
                                                     OptionsConverter.TableNames parameter,
                                                     String label,
                                                     String helperText,
                                                     Binder<Model> binderModel,
                                                     String defaultValue,
                                                     String ... values) {

    var comboBoxField = new ComboBox<String>(label);
    comboBoxField.setTooltipText(helperText);
    comboBoxField.setItems(values);
    comboBoxField.setValue(defaultValue);
    if (tableNamesProps.get(parameter) != null) {
      comboBoxField.setValue(tableNamesProps.get(parameter).toString());
    }
    binderModel.forField(comboBoxField)
        .bind(
            param -> tableNamesProps.get(parameter) != null?
                tableNamesProps.get(parameter).toString():null,
            (param, newparam) -> { if(newparam!=null) tableNamesProps.put(parameter, newparam);}
        );
    return comboBoxField;
  }


  static IntegerField createThreadsField(Binder<GenerationView.InternalCommandLaunch> binderCommand) {
    var intField = new IntegerField("Number of Threads");
    intField.setValue(1);
    intField.setMax(1000);
    intField.setTooltipText("Number of threads to use for data generation. This improve parallelism and makes generation quicker");
    binderCommand.forField(intField)
        .bind(GenerationView.InternalCommandLaunch::getThreads, GenerationView.InternalCommandLaunch::setThreads);
    return intField;
  }

  static NumberField createBatchesField(Binder<GenerationView.InternalCommandLaunch> binderCommand) {
    var intField = new NumberField("Number of Batches");
    intField.setValue(10d);
    intField.setTooltipText("Number of batches to run. Each batch consist of data generation and writes or push to the desired connector");
    binderCommand.forField(intField)
        .bind(
            c -> c!=null ? Double.valueOf(c.getNumberOfBatches()) : 1d,
            (c, newval) -> c.setNumberOfBatches(newval!=null ? newval.longValue() : 1L));
    return intField;
  }

  static NumberField createRowsField(Binder<GenerationView.InternalCommandLaunch> binderCommand) {
    var intField = new NumberField("Number of Rows per batch");
    intField.setValue(1000d);
    intField.setTooltipText("Number of rows to generate. Each Batch will produce this number of rows. So final number of rows generated is Batches x Rows.");
    binderCommand.forField(intField)
        .bind(
            c -> c!=null ? Double.valueOf(c.getRowsPerBatch()) : 1d,
            (c, newval) -> c.setRowsPerBatch(newval!=null ? newval.longValue() : 1L));
    return intField;
  }

  static Details createOptionalConfigs(Component ... components) {
    VerticalLayout content = new VerticalLayout();
    var componentsArray = Arrays.stream(components).toArray();
    for(var i=0;i<components.length;i+=2) {
      var firstComponent = (Component) componentsArray[i];
      var secondComponent = (i+1)==components.length? new Span(): (Component) componentsArray[i+1];
      content.add(new HorizontalLayout(firstComponent, secondComponent));
    }
    var details = new Details("Optional Configurations:", content);
    details.setOpened(false);
    return details;
  }

  static RadioButtonGroup createOneFilePerIteration(Binder<Model> binderModel, Map<OptionsConverter.Options, Object> tableNamesProps) {
    var field = createGenericBooleanRadio(tableNamesProps,
        OptionsConverter.Options.ONE_FILE_PER_ITERATION,
        "Generate one file per batch",
        binderModel);
    field.setRequired(false);
    return field;
  }

  static TextField createLocalPath(Binder<Model> binderModel, Map<OptionsConverter.TableNames, String> tableNamesProps) {
    return createGenericTextfield(tableNamesProps,
        OptionsConverter.TableNames.LOCAL_FILE_PATH,
        "Path: ",
        "Local Path where files will be generated in following format: /this/is/an/example/",
        binderModel);
  }

  static TextField createLocalName(Binder<Model> binderModel, Map<OptionsConverter.TableNames, String> tableNamesProps) {
    return createGenericTextfield(tableNamesProps,
        OptionsConverter.TableNames.LOCAL_FILE_NAME,
        "File Name: ",
        "Prefix to name the generated files",
        binderModel);
  }

  static TextField createHdfsPath(Binder<Model> binderModel, Map<OptionsConverter.TableNames, String> tableNamesProps) {
    return createGenericTextfield(tableNamesProps,
        OptionsConverter.TableNames.HDFS_FILE_PATH,
        "HDFS Path: ",
        "HDFS Path where files will be generated in following format: /this/is/an/example/",
        binderModel);
  }

  static TextField createHdfsName(Binder<Model> binderModel, Map<OptionsConverter.TableNames, String> tableNamesProps) {
    return createGenericTextfield(tableNamesProps,
        OptionsConverter.TableNames.HDFS_FILE_NAME,
        "HDFS File Name: ",
        "Prefix to name the generated files",
        binderModel);
  }

  static TextField createHBaseTableName(Binder<Model> binderModel, Map<OptionsConverter.TableNames, String> tableNamesProps) {
    return createGenericTextfield(tableNamesProps,
        OptionsConverter.TableNames.HBASE_TABLE_NAME,
          "HBase Table: ",
        "Name of the Hbase table",
        binderModel);
  }

  static TextField createHBaseNamespace(Binder<Model> binderModel, Map<OptionsConverter.TableNames, String> tableNamesProps) {
    return createGenericTextfield(tableNamesProps,
        OptionsConverter.TableNames.HBASE_NAMESPACE,
        "HBase Namespace: ",
        "Name of the Hbase namespace",
        binderModel);
  }

  static TextField createKafkaTopic(Binder<Model> binderModel, Map<OptionsConverter.TableNames, String> tableNamesProps) {
    return createGenericTextfield(tableNamesProps,
        OptionsConverter.TableNames.KAFKA_TOPIC,
        "Kafka Topic: ",
        "Name of the Kafka topic",
        binderModel);
  }

  static TextField createOzoneVolume(Binder<Model> binderModel, Map<OptionsConverter.TableNames, String> tableNamesProps) {
    return createGenericTextfield(tableNamesProps,
        OptionsConverter.TableNames.OZONE_VOLUME,
        "Ozone Volume: ",
        "Name of the Ozone Volume",
        binderModel);
  }

  static TextField createOzoneBucket(Binder<Model> binderModel, Map<OptionsConverter.TableNames, String> tableNamesProps) {
    return createGenericTextfield(tableNamesProps,
        OptionsConverter.TableNames.OZONE_BUCKET,
        "Ozone Bucket: ",
        "Name of the Ozone Bucket",
        binderModel);
  }

  static TextField createOzoneKey(Binder<Model> binderModel, Map<OptionsConverter.TableNames, String> tableNamesProps) {
    return createGenericTextfield(tableNamesProps,
        OptionsConverter.TableNames.OZONE_BUCKET,
        "Ozone Key Name: ",
        "Prefix for the Ozone keys generated",
        binderModel);
  }

  static TextField createOzoneLocalFile(Binder<Model> binderModel, Map<OptionsConverter.TableNames, String> tableNamesProps) {
    return createGenericTextfield(tableNamesProps,
        OptionsConverter.TableNames.OZONE_LOCAL_FILE_PATH,
        "Ozone Local File Path: ",
        "Local File Path used temporarily to create files before pushing them",
        binderModel);
  }

  static TextField createSolRCollection(Binder<Model> binderModel, Map<OptionsConverter.TableNames, String> tableNamesProps) {
    return createGenericTextfield(tableNamesProps,
        OptionsConverter.TableNames.SOLR_COLLECTION,
        "SolR Collection: ",
        "Name of the SolR Collection",
        binderModel);
  }

  static TextField createHiveDatabase(Binder<Model> binderModel, Map<OptionsConverter.TableNames, String> tableNamesProps) {
    return createGenericTextfield(tableNamesProps,
        OptionsConverter.TableNames.HIVE_DATABASE,
        "Hive Database: ",
        "Database in Hive (if not present, will be created)",
        binderModel);
  }

  static TextField createHiveTableName(Binder<Model> binderModel, Map<OptionsConverter.TableNames, String> tableNamesProps) {
    return createGenericTextfield(tableNamesProps,
        OptionsConverter.TableNames.HIVE_TABLE_NAME,
        "Hive Table Name: ",
        "Table in Hive",
        binderModel);
  }

  static TextField createHiveTempTable(Binder<Model> binderModel, Map<OptionsConverter.TableNames, String> tableNamesProps) {
    var field = createGenericTextfield(tableNamesProps,
        OptionsConverter.TableNames.HIVE_TEMPORARY_TABLE_NAME,
        "Hive Temporary Table: ",
        "Name of the temporary table used to load data if table is Managed. (if not set, it will defautl to table name + _tmp",
        binderModel);
    field.setRequired(false);
    return field;
  }

  static TextField createHiveHdfsFilePath(Binder<Model> binderModel, Map<OptionsConverter.TableNames, String> tableNamesProps) {
    return createGenericTextfield(tableNamesProps,
        OptionsConverter.TableNames.HIVE_HDFS_FILE_PATH,
        "Hive HDFS File Path: ",
        "Path in HDFS where external table data is stored. (if table is managed, it is used as location for temporary table)",
        binderModel);
  }

  static TextField createKuduTableName(Binder<Model> binderModel, Map<OptionsConverter.TableNames, String> tableNamesProps) {
    return createGenericTextfield(tableNamesProps,
        OptionsConverter.TableNames.KUDU_TABLE_NAME,
        "Kudu Table Name: ",
        "Table in Kudu",
        binderModel);
  }

  static TextField createS3Bucket(Binder<Model> binderModel, Map<OptionsConverter.TableNames, String> tableNamesProps) {
    return createGenericTextfield(tableNamesProps,
        OptionsConverter.TableNames.S3_BUCKET,
        "S3 Bucket: ",
        "Bucket in S3",
        binderModel);
  }

  static TextField createS3Directory(Binder<Model> binderModel, Map<OptionsConverter.TableNames, String> tableNamesProps) {
    return createGenericTextfield(tableNamesProps,
        OptionsConverter.TableNames.S3_DIRECTORY,
        "S3 Directory: ",
        "Directory in S3 in format /this/is/an/example/",
        binderModel);
  }

  static TextField createS3Key(Binder<Model> binderModel, Map<OptionsConverter.TableNames, String> tableNamesProps) {
    return createGenericTextfield(tableNamesProps,
        OptionsConverter.TableNames.S3_KEY_NAME,
        "S3 Key Name: ",
        "Prefix to name keys generated",
        binderModel);
  }

  static TextField createS3Region(Binder<Model> binderModel, Map<OptionsConverter.TableNames, String> tableNamesProps) {
    if(tableNamesProps.get(OptionsConverter.TableNames.S3_REGION)==null || tableNamesProps.get(OptionsConverter.TableNames.S3_REGION).isEmpty()){
      tableNamesProps.put(OptionsConverter.TableNames.S3_REGION, "us-east-1");
    }
    return createGenericTextfield(tableNamesProps,
        OptionsConverter.TableNames.S3_REGION,
        "S3 Region: ",
        "Region in AWS",
        binderModel);
  }

  static TextField createS3AccessKeyId(Binder<Model> binderModel, Map<OptionsConverter.TableNames, String> tableNamesProps) {
    return createGenericTextfield(tableNamesProps,
        OptionsConverter.TableNames.S3_ACCESS_KEY_ID,
        "AWS Access Key Id: ",
        "Access Key ID for S3, with enough rights to write data in buckets specified.",
        binderModel);
  }

  static PasswordField createS3AccessKeySecret(Binder<Model> binderModel, Map<OptionsConverter.TableNames, String> tableNamesProps) {
    return createGenericPasswordfield(tableNamesProps,
        OptionsConverter.TableNames.S3_ACCESS_KEY_SECRET,
        "AWS Access Key Secret: ",
        "Access Key Secret for S3, with enough rights to write data in buckets specified.",
        binderModel);
  }

  static TextField createS3LocalFile(Binder<Model> binderModel, Map<OptionsConverter.TableNames, String> tableNamesProps) {
    return createGenericTextfield(tableNamesProps,
        OptionsConverter.TableNames.S3_LOCAL_FILE_PATH,
        "S3 Local File: ",
        "Local File Path used temporarily to create files before pushing them",
        binderModel);
  }

  static TextField createADLSContainer(Binder<Model> binderModel, Map<OptionsConverter.TableNames, String> tableNamesProps) {
    return createGenericTextfield(tableNamesProps,
        OptionsConverter.TableNames.ADLS_CONTAINER,
        "ADLS Container: ",
        "Container in ADLS",
        binderModel);
  }

  static TextField createADLSDirectory(Binder<Model> binderModel, Map<OptionsConverter.TableNames, String> tableNamesProps) {
    return createGenericTextfield(tableNamesProps,
        OptionsConverter.TableNames.ADLS_DIRECTORY,
        "ADLS Directory: ",
        "Directory in ADLS in format /this/is/an/example/",
        binderModel);
  }

  static TextField createADLSFileName(Binder<Model> binderModel, Map<OptionsConverter.TableNames, String> tableNamesProps) {
    return createGenericTextfield(tableNamesProps,
        OptionsConverter.TableNames.ADLS_FILE_NAME,
        "ADLS File Name: ",
        "Prefix to name files generated",
        binderModel);
  }

  static TextField createADLSLocalFile(Binder<Model> binderModel, Map<OptionsConverter.TableNames, String> tableNamesProps) {
    return createGenericTextfield(tableNamesProps,
        OptionsConverter.TableNames.ADLS_LOCAL_FILE_PATH,
        "ADLS Local File: ",
        "Local File Path used temporarily to create files before pushing them",
        binderModel);
  }

  static TextField createADLSAccountName(Binder<Model> binderModel, Map<OptionsConverter.TableNames, String> tableNamesProps) {
    return createGenericTextfield(tableNamesProps,
        OptionsConverter.TableNames.ADLS_ACCOUNT_NAME,
        "ADLS Account Name: ",
        "Name of the account",
        binderModel);
  }

  static ComboBox<String> createADLSAccountType(Binder<Model> binderModel, Map<OptionsConverter.TableNames, String> tableNamesProps) {
    return createGenericComboBoxfield(tableNamesProps,
        OptionsConverter.TableNames.ADLS_ACCOUNT_TYPE,
        "ADLS Account Type: ",
        "Type of the account determines storage type (either dfs or blob)",
        binderModel,
        "dfs",
        "dfs", "blob");
  }

  static PasswordField createADLSSasToken(Binder<Model> binderModel, Map<OptionsConverter.TableNames, String> tableNamesProps) {
    return createGenericPasswordfield(tableNamesProps,
        OptionsConverter.TableNames.ADLS_SAS_TOKEN,
        "ADLS SAS Token: ",
        "ADLS SAS token with enough rights to read/write data to container specified",
        binderModel);
  }

  static TextField createGCSBucket(Binder<Model> binderModel, Map<OptionsConverter.TableNames, String> tableNamesProps) {
    return createGenericTextfield(tableNamesProps,
        OptionsConverter.TableNames.GCS_BUCKET,
        "GCS Bucket: ",
        "Bucket in GCS",
        binderModel);
  }

  static TextField createGCSDirectory(Binder<Model> binderModel, Map<OptionsConverter.TableNames, String> tableNamesProps) {
    return createGenericTextfield(tableNamesProps,
        OptionsConverter.TableNames.GCS_DIRECTORY,
        "GCS Directory: ",
        "Directory in GCS in format /this/is/an/example/",
        binderModel);
  }

  static TextField createGCSObjectName(Binder<Model> binderModel, Map<OptionsConverter.TableNames, String> tableNamesProps) {
    return createGenericTextfield(tableNamesProps,
        OptionsConverter.TableNames.GCS_OBJECT_NAME,
        "GCS Key Name: ",
        "Prefix to name keys generated",
        binderModel);
  }

  static TextField createGCSLocalFile(Binder<Model> binderModel, Map<OptionsConverter.TableNames, String> tableNamesProps) {
    return createGenericTextfield(tableNamesProps,
        OptionsConverter.TableNames.GCS_LOCAL_FILE_PATH,
        "GCS Local File: ",
        "Local File Path used temporarily to create files before pushing them",
        binderModel);
  }

  static TextField createGCSRegion(Binder<Model> binderModel, Map<OptionsConverter.TableNames, String> tableNamesProps) {
    if(tableNamesProps.get(OptionsConverter.TableNames.GCS_REGION)==null || tableNamesProps.get(OptionsConverter.TableNames.GCS_REGION).isEmpty()){
      tableNamesProps.put(OptionsConverter.TableNames.GCS_REGION, "EUROPE-WEST9");
    }
    return createGenericTextfield(tableNamesProps,
        OptionsConverter.TableNames.GCS_REGION,
        "GCS Region: ",
        "Google cloud Region according to GCS standards",
        binderModel);
  }

  static TextField createGCSProjectId(Binder<Model> binderModel, Map<OptionsConverter.TableNames, String> tableNamesProps) {
    return createGenericTextfield(tableNamesProps,
        OptionsConverter.TableNames.GCS_PROJECT_ID,
        "GCS Project ID: ",
        "GCS Project ID",
        binderModel);
  }

  static TextField createGCSAccountKeyPath(Binder<Model> binderModel, Map<OptionsConverter.TableNames, String> tableNamesProps) {
    return createGenericTextfield(tableNamesProps,
        OptionsConverter.TableNames.GCS_ACCOUNT_KEY_PATH,
        "GCS Account Key Path: ",
        "Path to the local machine where Datagen runs to GCS account key in json format is stored",
        binderModel);
  }


  static TextField createHbasePrimaryKey(Binder<Model> binderModel, Map<OptionsConverter.Options, Object> tableNamesProps) {
    var field = createGenericTextfield(tableNamesProps,
        OptionsConverter.Options.HBASE_PRIMARY_KEY,
        "HBase Primary Key",
        "Name of the column that will be used as a primary key. (Make sure of the values uniqueness)",
        binderModel);
    return field;
  }

  /**
   * WARNING: Currently this would need to make a conversion of HBase column family mapping before generation so it is not used.
   * @param binderModel
   * @param tableNamesProps
   * @return
   */
  static TextField createHbaseColumnFamiliesMapping(Binder<Model> binderModel, Map<OptionsConverter.Options, Object> tableNamesProps) {
    var field = createGenericTextfield(tableNamesProps,
        OptionsConverter.Options.HBASE_COLUMN_FAMILIES_MAPPING,
        "HBase Column Family Mapping",
        "Mapping of column family to columns in that format: cf:col1,col2,col3;cf2:col4 . (Default is cq)",
        binderModel);
    field.setRequired(false);
    return field;
  }

  static IntegerField createSolrShards(Binder<Model> binderModel, Map<OptionsConverter.Options, Object> tableNamesProps) {
    var field = createGenericIntegerfield(tableNamesProps,
        OptionsConverter.Options.SOLR_SHARDS,
        "SolR Shards",
        "Number of shards in SolR for this collection",
        binderModel,
        1,
        1000);
    field.setRequired(false);
    return field;
  }

  static IntegerField createSolrReplicas(Binder<Model> binderModel, Map<OptionsConverter.Options, Object> tableNamesProps) {
    var field = createGenericIntegerfield(tableNamesProps,
        OptionsConverter.Options.SOLR_REPLICAS,
        "SolR Replicas",
        "Number of replicas for this SolR Collection",
        binderModel,
        1,
        1000);
    field.setRequired(false);
    return field;
  }

  static TextField createSolrJaasFilePath(Binder<Model> binderModel, Map<OptionsConverter.Options, Object> tableNamesProps) {
    var field = createGenericTextfield(tableNamesProps,
        OptionsConverter.Options.SOLR_JAAS_FILE_PATH,
        "SolR JaaS File Path",
        "For Kerberos authentication a JaaS file is created at this specified location",
        binderModel);
    field.setRequired(false);
    return field;
  }

  static IntegerField createHiveThreadNumber(Binder<Model> binderModel, Map<OptionsConverter.Options, Object> tableNamesProps) {
    var field = createGenericIntegerfield(tableNamesProps,
        OptionsConverter.Options.HIVE_THREAD_NUMBER,
        "Hive Thread Number",
        "In case of using direct Hive insert statements (and not HDFS files), parallelize insert statements by this number of threads",
        binderModel,
        1,
        100);
    field.setRequired(false);
    return field;
  }

  static ComboBox<String> createHiveTableType(Binder<Model> binderModel, Map<OptionsConverter.Options, Object> tableNamesProps) {
    var field = createGenericComboBoxfield(tableNamesProps,
        OptionsConverter.Options.HIVE_TABLE_TYPE,
        "Table Type",
        "Hive Table type among External, Managed or Iceberg",
        binderModel,
        "EXTERNAL",
        "EXTERNAL", "MANAGED", "ICEBERG");
    field.setRequired(true);
    return field;
  }

  static ComboBox<String> createHiveTableFormat(Binder<Model> binderModel, Map<OptionsConverter.Options, Object> tableNamesProps) {
    var field = createGenericComboBoxfield(tableNamesProps,
        OptionsConverter.Options.HIVE_TABLE_FORMAT,
        "Table Format",
        "Hive Table Format among ORC, PARQUET, AVRO, JSON, CSV",
        binderModel,
        "ORC",
        "ORC", "PARQUET", "AVRO", "JSON", "CSV");
    field.setRequired(true);
    return field;
  }

  static RadioButtonGroup createHiveOnHdfs(Binder<Model> binderModel, Map<OptionsConverter.Options, Object> tableNamesProps) {
    var field = createGenericBooleanRadio(tableNamesProps,
        OptionsConverter.Options.HIVE_ON_HDFS,
        "Use HDFS Files to generate Hive Data",
        binderModel);
    field.setRequired(false);
    return field;
  }

  static TextField createHiveTezQueueName(Binder<Model> binderModel, Map<OptionsConverter.Options, Object> tableNamesProps) {
    var field = createGenericTextfield(tableNamesProps,
        OptionsConverter.Options.HIVE_TEZ_QUEUE_NAME,
        "YARN Queue Name",
        "Name of the YARN Queue, used by Tez containers to load data into the Hive table",
        binderModel);
    field.setRequired(false);
    return field;
  }

  static TextField createHiveTablePartitionsCols(Binder<Model> binderModel, Map<OptionsConverter.Options, Object> tableNamesProps) {
    var field = createGenericTextfield(tableNamesProps,
        OptionsConverter.Options.HIVE_TABLE_PARTITIONS_COLS,
        "Partition Columns",
        "A comma separated list of existing columns to use as partition columns (ex: col1,column2)",
        binderModel);
    field.setRequired(false);
    return field;
  }

  static TextField createHiveTableBucketsCols(Binder<Model> binderModel, Map<OptionsConverter.Options, Object> tableNamesProps) {
    var field = createGenericTextfield(tableNamesProps,
        OptionsConverter.Options.HIVE_TABLE_BUCKETS_COLS,
        "Bucketing Columns",
        "A comma separated list of existing columns to use as bucketing columns (ex: col1,column2)",
        binderModel);
    field.setRequired(false);
    return field;
  }

  static IntegerField createHiveTableBucketsNumber(Binder<Model> binderModel, Map<OptionsConverter.Options, Object> tableNamesProps) {
    var field = createGenericIntegerfield(tableNamesProps,
        OptionsConverter.Options.HIVE_TABLE_BUCKETS_NUMBER,
        "Table Buckets Number",
        "Number of buckets to use for the table",
        binderModel,
        1,
        100000);
    field.setRequired(false);
    return field;
  }


  static RadioButtonGroup createCsvHeader(Binder<Model> binderModel, Map<OptionsConverter.Options, Object> tableNamesProps) {
    var field = createGenericBooleanRadio(tableNamesProps,
        OptionsConverter.Options.CSV_HEADER,
        "Append a CSV header",
        binderModel);
    field.setRequired(false);
    return field;
  }

  static RadioButtonGroup createDeletePrevious(Binder<Model> binderModel, Map<OptionsConverter.Options, Object> tableNamesProps) {
    var field = createGenericBooleanRadio(tableNamesProps,
        OptionsConverter.Options.DELETE_PREVIOUS,
        "Delete Previous Data",
        binderModel);
    field.setRequired(false);
    return field;
  }

  static IntegerField createParquetPageSize(Binder<Model> binderModel, Map<OptionsConverter.Options, Object> tableNamesProps) {
    var field = createGenericIntegerfield(tableNamesProps,
        OptionsConverter.Options.PARQUET_PAGE_SIZE,
        "Parquet Page Size",
        "Parquet Page Size to set for Parquet files",
        binderModel,
        1024,
        Integer.MAX_VALUE);
    field.setRequired(false);
    return field;
  }

  static IntegerField createParquetRowGroupSize(Binder<Model> binderModel, Map<OptionsConverter.Options, Object> tableNamesProps) {
    var field = createGenericIntegerfield(tableNamesProps,
        OptionsConverter.Options.PARQUET_ROW_GROUP_SIZE,
        "Parquet Row Group Size",
        "Parquet Row Group Size to set for Parquet files",
        binderModel,
        1024,
        Integer.MAX_VALUE);
    field.setRequired(false);
    return field;
  }

  static IntegerField createParquetDictionaryPageSize(Binder<Model> binderModel, Map<OptionsConverter.Options, Object> tableNamesProps) {
    var field = createGenericIntegerfield(tableNamesProps,
        OptionsConverter.Options.PARQUET_DICTIONARY_PAGE_SIZE,
        "Parquet Dictionary Size",
        "Parquet Dictionary Size to set for Parquet files",
        binderModel,
        1024,
        Integer.MAX_VALUE);
    field.setRequired(false);
    return field;
  }

  static RadioButtonGroup createParquetDictionaryEncoding(Binder<Model> binderModel, Map<OptionsConverter.Options, Object> tableNamesProps) {
    var field = createGenericBooleanRadio(tableNamesProps,
        OptionsConverter.Options.PARQUET_DICTIONARY_ENCODING,
        "Parquet Dictionary Encoding",
        binderModel);
    field.setRequired(false);
    return field;
  }

  static TextField createKafkaMsgKey(Binder<Model> binderModel, Map<OptionsConverter.Options, Object> tableNamesProps) {
    var field = createGenericTextfield(tableNamesProps,
        OptionsConverter.Options.KAFKA_MSG_KEY,
        "Kafka Message Key",
        "Name of the column to use as key for the Kafka Message",
        binderModel);
    field.setRequired(false);
    return field;
  }

  static ComboBox<String> createKafkaAcksConfig(Binder<Model> binderModel, Map<OptionsConverter.Options, Object> tableNamesProps) {
    var field = createGenericComboBoxfield(tableNamesProps,
        OptionsConverter.Options.KAFKA_ACKS_CONFIG,
        "Kafka Acks",
        "Number of acks to certify that a message has been sent. All means all replicas have been written, 0 no acknowledgment. 1, 2 means number of replicas to be synced.",
        binderModel,
        "all",
        "all", "-1", "1", "0"
        );
    field.setRequired(false);
    return field;
  }

  static IntegerField createKafkaRetriesConfig(Binder<Model> binderModel, Map<OptionsConverter.Options, Object> tableNamesProps) {
    var field = createGenericIntegerfield(tableNamesProps,
        OptionsConverter.Options.KAFKA_RETRIES_CONFIG,
        "Kafka Retries",
        "Number of retries to make in case of failure",
        binderModel,
        0,
        1000);
    field.setRequired(false);
    return field;
  }

  static TextField createKafkaJaasFilePath(Binder<Model> binderModel, Map<OptionsConverter.Options, Object> tableNamesProps) {
    var field = createGenericTextfield(tableNamesProps,
        OptionsConverter.Options.KAFKA_JAAS_FILE_PATH,
        "Kafka JaaS File Path",
        "For Kerberos authentication a JaaS file is created at this specified location",
        binderModel);
    field.setRequired(false);
    return field;
  }

  static IntegerField createKafkaReplicationFactor(Binder<Model> binderModel, Map<OptionsConverter.Options, Object> tableNamesProps) {
    var field = createGenericIntegerfieldWithBindToShort(tableNamesProps,
        OptionsConverter.Options.KAFKA_REPLICATION_FACTOR,
        "Kafka Replication Factor",
        "Replication Factor in Kafka for messages sent",
        binderModel,
        1,
        100);
    field.setRequired(false);
    return field;
  }

  static IntegerField createKafkaPartitionsNumber(Binder<Model> binderModel, Map<OptionsConverter.Options, Object> tableNamesProps) {
    var field = createGenericIntegerfield(tableNamesProps,
        OptionsConverter.Options.KAFKA_PARTITIONS_NUMBER,
        "Kafka partitions",
        "Number of Partitions for the topic where messages will be sent",
        binderModel,
        1,
        100000);
    field.setRequired(false);
    return field;
  }

  static ComboBox<String> createKafkaMessageType(Binder<Model> binderModel, Map<OptionsConverter.Options, Object> tableNamesProps) {
    var field = createGenericComboBoxfield(tableNamesProps,
        OptionsConverter.Options.KAFKA_MESSAGE_TYPE,
        "Kafka Message Format",
        "Message Format for Kafka. Avro will require to use a schema registry",
        binderModel,
        "JSON",
        "JSON", "AVRO", "CSV");
    field.setRequired(false);
    return field;
  }

  static IntegerField createKuduBuckets(Binder<Model> binderModel, Map<OptionsConverter.Options, Object> tableNamesProps) {
    var field = createGenericIntegerfield(tableNamesProps,
        OptionsConverter.Options.KUDU_BUCKETS,
        "Buckets Number",
        "Number of buckets for the Kudu table",
        binderModel,
        1,
        10000);
    field.setRequired(false);
    return field;
  }

  static IntegerField createKuduBuffer(Binder<Model> binderModel, Map<OptionsConverter.Options, Object> tableNamesProps) {
    var field = createGenericIntegerfield(tableNamesProps,
        OptionsConverter.Options.KUDU_BUFFER,
        "Buffer Size",
        "Size of the Kudu Buffer",
        binderModel,
        1,
        100000000);
    field.setRequired(false);
    return field;
  }

  static ComboBox<String> createKuduFlush(Binder<Model> binderModel, Map<OptionsConverter.Options, Object> tableNamesProps) {
    var field = createGenericComboBoxfield(tableNamesProps,
        OptionsConverter.Options.KUDU_FLUSH,
        "Flush Type",
        "Type of the flush to apply for Kudu saving among MANUAL_FLUSH, AUTO_FLUSH_SYNC, AUTO_FLUSH_BACKGROUND.",
        binderModel,
        "MANUAL_FLUSH",
        "AUTO_FLUSH_SYNC", "AUTO_FLUSH_BACKGROUND", "MANUAL_FLUSH");
    field.setRequired(false);
    return field;
  }

  static IntegerField createKuduReplicas(Binder<Model> binderModel, Map<OptionsConverter.Options, Object> tableNamesProps) {
    var field = createGenericIntegerfield(tableNamesProps,
        OptionsConverter.Options.KUDU_REPLICAS,
        "Number of Replicas",
        "Number of replicas for the Kudu table.",
        binderModel,
        1,
        1000);
    field.setRequired(false);
    return field;
  }

  static TextField createKuduHashKeys(Binder<Model> binderModel, Map<OptionsConverter.Options, Object> tableNamesProps) {
    var field = createGenericTextfield(tableNamesProps,
        OptionsConverter.Options.KUDU_HASH_KEYS,
        "Hash Keys",
        "A comma separated list of columns names to use as Hash keys for Kudu table.",
        binderModel);
    field.setRequired(false);
    return field;
  }

  static TextField createKuduRangeKeys(Binder<Model> binderModel, Map<OptionsConverter.Options, Object> tableNamesProps) {
    var field = createGenericTextfield(tableNamesProps,
        OptionsConverter.Options.KUDU_RANGE_KEYS,
        "Range Keys",
        "A comma separated list of columns names to use as Range keys for Kudu table.",
        binderModel);
    field.setRequired(false);
    return field;
  }

  static TextField createKuduPrimaryKeys(Binder<Model> binderModel, Map<OptionsConverter.Options, Object> tableNamesProps) {
    var field = createGenericTextfield(tableNamesProps,
        OptionsConverter.Options.KUDU_PRIMARY_KEYS,
        "Primary Keys",
        "A comma separated list of columns names to use as Primary keys for Kudu table.",
        binderModel);
    field.setRequired(false);
    return field;
  }

  static IntegerField createOzoneReplicationFactor(Binder<Model> binderModel, Map<OptionsConverter.Options, Object> tableNamesProps) {
    var field = createGenericIntegerfield(tableNamesProps,
        OptionsConverter.Options.OZONE_REPLICATION_FACTOR,
        "Replication Factor",
        "Replication Factor for Ozone files created",
        binderModel,
        1,
        1000);
    field.setRequired(false);
    return field;
  }

  static IntegerField createHdfsReplicationFactor(Binder<Model> binderModel, Map<OptionsConverter.Options, Object> tableNamesProps) {
    var field = createGenericIntegerfield(tableNamesProps,
        OptionsConverter.Options.HDFS_REPLICATION_FACTOR,
        "Replication Factor",
        "Replication Factor for Ozone files created",
        binderModel,
        1,
        1000);
    field.setRequired(false);
    return field;
  }

  static IntegerField createAdlsBlockSize(Binder<Model> binderModel, Map<OptionsConverter.Options, Object> tableNamesProps) {
    var field = createGenericIntegerfield(tableNamesProps,
        OptionsConverter.Options.ADLS_BLOCK_SIZE,
        "Block Size",
        "ADLS Block size in Bytes for created files",
        binderModel,
        1,
        Integer.MAX_VALUE);
    field.setRequired(false);
    return field;
  }

  static IntegerField createAdlsMaxUploadSize(Binder<Model> binderModel, Map<OptionsConverter.Options, Object> tableNamesProps) {
    var field = createGenericIntegerfield(tableNamesProps,
        OptionsConverter.Options.ADLS_MAX_UPLOAD_SIZE,
        "Maximum Upload Size",
        "Maximum Upload Size in Bytes to upload files to ADLS",
        binderModel,
        1,
        Integer.MAX_VALUE);
    field.setRequired(false);
    return field;
  }

  static IntegerField createAdlsMaxConcurrency(Binder<Model> binderModel, Map<OptionsConverter.Options, Object> tableNamesProps) {
    var field = createGenericIntegerfield(tableNamesProps,
        OptionsConverter.Options.ADLS_MAX_CONCURRENCY,
        "Maximum Concurrency",
        "Maximum Concurrency to upload files to ADLS",
        binderModel,
        1,
        100000);
    field.setRequired(false);
    return field;
  }


}
