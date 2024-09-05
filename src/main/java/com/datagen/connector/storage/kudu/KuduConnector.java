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
package com.datagen.connector.storage.kudu;

import com.datagen.config.ApplicationConfigs;
import com.datagen.connector.ConnectorInterface;
import com.datagen.model.Model;
import com.datagen.model.OptionsConverter;
import com.datagen.model.Row;
import com.datagen.model.type.Field;
import com.datagen.utils.KerberosUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kudu.client.*;

import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * This is a kudu connector based on Kudu 1.11.0 API
 */
@SuppressWarnings("unchecked")
@Slf4j
public class KuduConnector implements ConnectorInterface {

  private KuduTable table;
  private KuduSession session;
  private KuduClient client;
  private final String tableName;
  private final Model model;
  private Boolean useKerberos;

  // TODO: Cannot invoke "com.datagen.model.type.Field.getPossibleValuesProvided()" because "field" is null

  public KuduConnector(Model model,
                       Map<ApplicationConfigs, String> properties) {
    this.tableName = (String) model.getTableNames()
        .get(OptionsConverter.TableNames.KUDU_TABLE_NAME);
    this.model = model;
    this.useKerberos = model.getTableNames().get(OptionsConverter.TableNames.KUDU_USE_KERBEROS)==null ?
        Boolean.parseBoolean(properties.get(ApplicationConfigs.KUDU_AUTH_KERBEROS)) :
        Boolean.parseBoolean(model.getTableNames().get(OptionsConverter.TableNames.KUDU_USE_KERBEROS).toString());

    try {

      System.setProperty("javax.net.ssl.trustStore",
          properties.get(ApplicationConfigs.KUDU_TRUSTSTORE_LOCATION));
      System.setProperty("javax.net.ssl.trustStorePassword",
          properties.get(ApplicationConfigs.KUDU_TRUSTSTORE_PASSWORD));

      if (useKerberos) {
        KerberosUtils.loginUserWithKerberos(
            model.getTableNames().get(OptionsConverter.TableNames.KUDU_USER)==null ?
                properties.get(ApplicationConfigs.KUDU_SECURITY_USER) :
                model.getTableNames().get(OptionsConverter.TableNames.KUDU_USER).toString(),
            model.getTableNames().get(OptionsConverter.TableNames.KUDU_KEYTAB)==null ?
                properties.get(ApplicationConfigs.KUDU_SECURITY_KEYTAB) :
                model.getTableNames().get(OptionsConverter.TableNames.KUDU_KEYTAB).toString(),
            new Configuration());

        UserGroupInformation.getLoginUser().doAs(
            new PrivilegedExceptionAction<KuduClient>() {
              @Override
              public KuduClient run() throws Exception {
                client = new KuduClient.KuduClientBuilder(
                    properties.get(
                        ApplicationConfigs.KUDU_URL)).build();
                return client;
              }
            });
      } else {
        this.client = new KuduClient.KuduClientBuilder(
            properties.get(ApplicationConfigs.KUDU_URL)).build();
      }

      this.session = client.newSession();

    } catch (Exception e) {
      log.error("Could not connect to Kudu due to error: ", e);
    }
  }

  @Override
  public void init(Model model, boolean writer) {
    if (writer) {
      try {

        // If some columns are range keys or partitions keys, we need to re-order them to be first
        if (model.getKuduHashKeys() != null &&
            !model.getKuduHashKeys().isEmpty()) {
          model.reorderColumnsWithKeyCols(
              model.getKuduHashKeys());
        }
        if (model.getKuduRangeKeys() != null &&
            !model.getKuduRangeKeys().isEmpty()) {
          model.reorderColumnsWithKeyCols(
              model.getKuduRangeKeys());
        }

        // We should then set primary keys columns as first
        model.reorderColumnsWithKeyCols(model.getKuduPrimaryKeys());

        switch ((String) model.getOptionsOrDefault(
            OptionsConverter.Options.KUDU_FLUSH)) {
        case "AUTO_FLUSH_SYNC":
          session.setFlushMode(
              SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
          break;
        case "AUTO_FLUSH_BACKGROUND":
          session.setFlushMode(
              SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
          break;
        case "MANUAL_FLUSH":
          session.setFlushMode(
              SessionConfiguration.FlushMode.MANUAL_FLUSH);
          break;
        }
        session.setMutationBufferSpace(
            (int) model.getOptionsOrDefault(
                OptionsConverter.Options.KUDU_BUFFER));

        if ((Boolean) model.getOptionsOrDefault(
            OptionsConverter.Options.DELETE_PREVIOUS)) {
          client.deleteTable(tableName);
        }

        createTableIfNotExists();

        this.table = client.openTable(tableName);

      } catch (Exception e) {
        log.error("Could not create table in Kudu due to error: ", e);
      }
    }
  }

  @Override
  public void terminate() {
    try {
      session.close();
      client.shutdown();
      if (useKerberos) {
        KerberosUtils.logoutUserWithKerberos();
      }
    } catch (Exception e) {
      log.error("Could not close connection to Kudu due to error: ", e);
    }
  }

  @Override
  public void sendOneBatchOfRows(List<Row> rows) {
    try {
      rows.parallelStream().map(row -> row.toKuduInsert(table))
          .forEach(insert -> {
            try {
              session.apply(insert);
            } catch (KuduException e) {
              log.error("Could not insert row for kudu in table : " + table +
                  " due to error:", e);
            }
          });
      session.flush();
    } catch (Exception e) {
      log.error("Could not send rows to kudu due to error: ", e);
    }

  }

  @Override
  public Model generateModel(Boolean deepAnalysis) {
    LinkedHashMap<String, Field> fields = new LinkedHashMap<String, Field>();
    Map<String, List<String>> primaryKeys = new HashMap<>();
    Map<String, String> tableNames = new HashMap<>();
    Map<String, String> options = new HashMap<>();
    // TODO : Implement logic to create a model with at least names, pk, options and column names/types
    return new Model("",fields, primaryKeys, tableNames, options, null);
  }

  private void createTableIfNotExists() {
    CreateTableOptions cto = new CreateTableOptions();
    cto.setNumReplicas((int) model.getOptionsOrDefault(
        OptionsConverter.Options.KUDU_REPLICAS));

    if (model.getKuduRangeKeys() != null &&
        !model.getKuduRangeKeys().isEmpty()) {
      cto.setRangePartitionColumns(model.getKuduRangeKeys());
      // Foreach Kudu range col, we need to identify its possible values and partition with it or split it between min and max
      model.getKuduRangeKeys().forEach(colname -> {
        Field field = model.getFieldFromName((String) colname);

        if (field!=null &&
            field.getPossibleValuesProvided() != null &&
            !field.getPossibleValuesProvided().isEmpty()) {
          log.info(
              "For column: {}, found non-empty possible_values to use for range partitions",
              (String) colname);
          createPartitionsFromListOfValues((String) colname,
              field.getPossibleValuesProvided(), cto);
        } else if (field!=null && field.getMin() != null && field.getMax() != null) {
          log.info("For column: {}, will use minimum and maximum",
              (String) colname);
          createPartitionsFromMinAndMax((String) colname, field.getMin(),
              field.getMax(), cto);
        } else {
          log.warn("You should NOT PARTITION BY RANGE this column: {}",
              (String) colname);
        }
      });
    }

    if (model.getKuduHashKeys() != null && !model.getKuduHashKeys().isEmpty()) {
      cto.addHashPartitions(model.getKuduHashKeys(),
          (int) model.getOptionsOrDefault(
              OptionsConverter.Options.KUDU_BUCKETS));
    }

    try {
      if(!client.getTablesList(tableName).getTablesList().contains(tableName)) {
        client.createTable(tableName, model.getKuduSchema(), cto);
      } else {
        log.info("Table {} already exists so not creating it", tableName);
      }
    } catch (KuduException e) {
      if (e.getMessage().contains("already exists")) {
        log.info("Table Kudu : " + tableName +
            " already exists, hence it will not be created");
      } else {
        log.error("Could not create table due to error", e);
      }
    }

  }

  private void createPartitionsFromListOfValues(String colName,
                                                List<String> possibleValues,
                                                CreateTableOptions cto) {
    possibleValues.forEach(value -> {
          log.debug("Create Part col: {} with value: {}", colName, value);
          PartialRow partialRowLower = new PartialRow(model.getKuduSchema());
          partialRowLower.addString(colName, value);
          PartialRow partialRowUpper = new PartialRow(model.getKuduSchema());
          partialRowUpper.addString(colName, value);
          // From Kudu 0.17, it seems that adding RangePartition is not working anymore, so use the splitRow which is working.
          try {
            cto.addRangePartition(partialRowLower, partialRowUpper,
                RangePartitionBound.INCLUSIVE_BOUND,
                RangePartitionBound.INCLUSIVE_BOUND);
          } catch (Exception e) {
            cto.addSplitRow(partialRowLower);
          }
        }
    );
  }

  private void createPartitionsFromMinAndMax(String colName, Long min, Long max,
                                             CreateTableOptions cto) {
    // By default, we will try to split by 32, if not, by the difference between min & max
    Long difference = max - min;
    Long numOfPartitions = difference > 32 ? 32 : difference;
    Long step = difference / numOfPartitions;

    for (int i = 0; i < numOfPartitions - 1; i++) {
      log.debug(
          "Create Part col: {} with value min: {} (Inclusive) ; and value max: {} (Exclusive)",
          colName, min + (i * step), min + ((i + 1) * step));
      PartialRow partialRowLower = new PartialRow(model.getKuduSchema());
      partialRowLower.addLong(colName, min + (i * step));
      PartialRow partialRowUpper = new PartialRow(model.getKuduSchema());
      partialRowUpper.addLong(colName, min + ((i + 1) * step));
      // From Kudu 0.17, it seems that adding RangePartition is not working anymore, so use the splitRow which is working.
      try {
        cto.addRangePartition(partialRowLower, partialRowUpper);
      } catch (Exception e) {
        cto.addSplitRow(partialRowLower);
      }
    }
    // Last Partition should be until max (max being included)
    log.debug(
        "Create Part col: {} with value min: {} (Inclusive) ; and value max: {} (Inclusive)",
        colName, min + ((numOfPartitions - 1) * step), max);
    PartialRow partialRowLower = new PartialRow(model.getKuduSchema());
    partialRowLower.addLong(colName, min + ((numOfPartitions - 1) * step));
    PartialRow partialRowUpper = new PartialRow(model.getKuduSchema());
    partialRowUpper.addLong(colName, max);
    // From Kudu 0.17, it seems that adding RangePartition is not working anymore, so use the splitRow which is working.
    try {
      cto.addRangePartition(partialRowLower, partialRowUpper,
          RangePartitionBound.INCLUSIVE_BOUND,
          RangePartitionBound.INCLUSIVE_BOUND);
    } catch (Exception e) {
      cto.addSplitRow(partialRowLower);
    }
  }

}
