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
package com.datagen.model;

import com.datagen.model.type.Field;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;


@Slf4j
public class RowGeneratorThread<T extends Field> extends Thread {

  @Getter
  private List<Row> rows;

  private final long numberofRows;
  private final Model model;
  private final List<String> fieldsRandomName;
  private final List<String> fieldsComputedName;
  private final LinkedHashMap<String, T> fields;

  RowGeneratorThread(long numberOfRows, Model model,
                     List<String> fieldsRandomName,
                     List<String> fieldsComputedName,
                     LinkedHashMap<String, T> fields) {
    this.rows = new ArrayList<>();
    this.numberofRows = numberOfRows;
    this.model = model;
    this.fieldsRandomName = fieldsRandomName;
    this.fieldsComputedName = fieldsComputedName;
    this.fields = fields;
    log.debug("Prepared a new Thread to run generation of data");
  }

  @Override
  public void run() {
    for (long i = 0; i < numberofRows; i++) {
      Row row = new Row();
      row.setModel(model);
      fieldsRandomName.forEach(f -> row.getValues()
          .put(f, fields.get(f).generateRandomValue()));
      fieldsComputedName.forEach(f -> row.getValues()
          .put(f, fields.get(f).generateComputedValue(row)));

      if (log.isDebugEnabled()) {
        log.debug("Created random row: " + row);
      }
      rows.add(row);
    }
  }


}
