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
package com.datagen.model.conditions;

import com.datagen.model.Model;
import com.datagen.model.Row;
import com.datagen.model.type.Field;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Condition {

  @Getter
  @Setter
  String columnName1;

  @Getter
  @Setter
  String columnName2;

  @Getter
  @Setter
  String value2;

  @Getter
  @Setter
  Operators operator;


  @Getter
  @Setter
  String columnType;

  @Getter
  @Setter
  Integer value2AsInt;

  @Getter
  @Setter
  Long value2AsLong;

  @Getter
  @Setter
  Float value2AsFloat;


  Condition(String columnName1, String columnName2, String value2,
            String operator) {
    this.columnName1 = columnName1;
    this.columnName2 = columnName2;
    this.value2 = value2;
    switch (operator) {
    case "=":
      this.operator = Operators.EQUALS;
      break;
    case "!":
      this.operator = Operators.UNEQUALS;
      break;
    case ">":
      this.operator = Operators.SUPERIOR;
      break;
    case "<":
      this.operator = Operators.INFERIOR;
      break;
    }

    log.debug("Comparison will be made between two columns: " + columnName1 +
        " and " + columnName2);

    if (value2 != null) {
      log.debug("Comparison will be made between column: " + columnName1 +
          " and value: " + value2);
    }

  }

  // After creating the whole model, this function must be called to guess the column type and speed up future comparisons/evaluations
  public void guessColumnType(Model model) {
    Field field = (Field) model.getFields().get(columnName1);
    if (field == null) {
      log.error("Could not find column: " + columnName1 + " in list of fields");
    }

    switch (field.getClass().getSimpleName()) {
    case "LongField":
      if (value2 != null) {
        this.value2AsLong = Long.valueOf(value2);
      }
      this.columnType = "Long";
      break;
    case "IntegerField":
      if (value2 != null) {
        this.value2AsInt = Integer.valueOf(value2);
      }
      this.columnType = "Integer";
      break;
    case "FloatField":
      if (value2 != null) {
        this.value2AsFloat = Float.valueOf(value2);
      }
      this.columnType = "Float";
      break;
    default:
      this.columnType = "String";
    }

  }

  public boolean evaluateCondition(Row row) {
    boolean result;
    String firstValue = row.getValues().get(columnName1).toString().trim();
    String secondValue = columnName2 == null ? value2.trim() :
        row.getValues().get(columnName2).toString().trim();

    switch (this.operator) {
    case EQUALS:
      result = firstValue.equalsIgnoreCase(secondValue);
      break;

    case UNEQUALS:
      result = !firstValue.equalsIgnoreCase(secondValue);
      break;

    case INFERIOR:
      result = !isSuperior(firstValue, secondValue);
      break;

    case SUPERIOR:
      result = isSuperior(firstValue, secondValue);
      break;

    default:
      result = false;
      log.warn("Could not get any operator to evaluate condition");
      break;
    }

    log.debug(
        "Evaluated condition between {} and {} using operator {} and result is {}",
        firstValue, secondValue, operator, result);

    return result;
  }

  private boolean isSuperior(String firstValue, String secondValue) {
    switch (this.columnType) {
    case "Long":
      return value2AsLong == null ?
          Long.parseLong(firstValue) > Long.parseLong(secondValue) :
          Long.parseLong(firstValue) > value2AsLong;
    case "Integer":
      return value2AsInt == null ?
          Integer.parseInt(firstValue) > Integer.parseInt(secondValue) :
          Long.parseLong(firstValue) > value2AsInt;
    case "Float":
      return value2AsFloat == null ?
          Float.parseFloat(firstValue) > Float.parseFloat(secondValue) :
          Float.parseFloat(firstValue) > value2AsFloat;
    default:
      return firstValue.compareTo(secondValue) > 0;
    }
  }

  public enum Operators {
    EQUALS,
    UNEQUALS,
    SUPERIOR,
    INFERIOR
  }

  public enum ConditionOperators {
    AND,
    OR
  }

}
