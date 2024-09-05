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


import com.datagen.model.Row;
import com.datagen.utils.ParsingUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedList;


/**
 * A Conditions line is a line of 1 or multiple conditions evaluated in order
 */
@Slf4j
public class ConditionsLine {

  @Getter
  @Setter
  private LinkedList<Condition> listOfConditions;

  @Getter
  @Setter
  private LinkedList<Condition.ConditionOperators> listOfConditionsOperators;


  // To indicate if there are multiple conditions on this line or only one
  private boolean combinedCondition = false;

  @Getter
  private String valueToReturn;
  @Getter
  private String rawValueToReturn;
  @Getter
  private final String rawOperatorValue;

  @Getter
  private Link linkToEvaluate;
  @Getter
  private boolean link = false;
  @Getter
  private boolean formula = false;
  @Getter
  private boolean injection = false;

  private LinkedList<ParsingUtils.StringFragment> stringFragments;
  private JsEvaluator jsEvaluator;

  public ConditionsLine(String conditionLine, String valueToReturn) {
    this.valueToReturn = valueToReturn;
    this.rawValueToReturn = valueToReturn;
    this.rawOperatorValue = conditionLine;
    this.listOfConditionsOperators = new LinkedList<>();
    this.listOfConditions = new LinkedList<>();

    // 1st: break using space => That will isolate if there are multiple parts
    String[] conditionSplitted = conditionLine.trim().split(" ");

    if (conditionSplitted.length > 1) {
      log.debug("Found a combined condition on this line");
      this.combinedCondition = true;
    } else if (conditionSplitted[0].equalsIgnoreCase("formula")) {
      log.debug("Found a formula, that will need to be evaluated");
      this.formula = true;
      this.jsEvaluator = new JsEvaluator();
      this.stringFragments = ParsingUtils.parseStringWithVars(valueToReturn);
      return;
    } else if (conditionSplitted[0].equalsIgnoreCase("link")) {
      log.debug("Found a link, that will need to be evaluated");
      this.link = true;
      this.linkToEvaluate = new Link(valueToReturn);
      return;
    } else if (conditionSplitted[0].equalsIgnoreCase("injection")) {
      log.debug("Found an injection, that will need to be evaluated");
      this.injection = true;
      this.stringFragments = ParsingUtils.parseStringWithVars(valueToReturn);
      return;
    } else if (conditionSplitted[0].equalsIgnoreCase("default")) {
      log.debug("Found a default, No evaluation needed");
      boolean defaultValue = true;
      return;
    }

    int index = 0;
    for (String s : conditionSplitted) {
      if (index % 2 == 0) {
        log.debug("This is an expression that will create a condition");
        listOfConditions.add(createConditionFromExpression(s));
      } else {
        log.debug(
            "This is an expression that will create an operator between conditions");
        listOfConditionsOperators.add(createOperatorFromExpression(s));
      }
      index++;
    }

  }

  private Condition.ConditionOperators createOperatorFromExpression(
      String operatorExpression) {
    if (operatorExpression.trim().equalsIgnoreCase("|")) {
      return Condition.ConditionOperators.OR;
    } else {
      return Condition.ConditionOperators.AND;
    }
  }

  private Condition createConditionFromExpression(String conditionExpression) {
    String[] conditionVals = null;
    String operator = "=";
    if (conditionExpression.contains("=")) {
      conditionVals = conditionExpression.trim().split("=");
    } else if (conditionExpression.contains("!")) {
      conditionVals = conditionExpression.trim().split("!");
      operator = "!";
    } else if (conditionExpression.contains("<")) {
      conditionVals = conditionExpression.trim().split("<");
      operator = "<";
    } else if (conditionExpression.contains(">")) {
      conditionVals = conditionExpression.trim().split(">");
      operator = ">";
    }

    if (conditionVals != null) {
      if (conditionVals[1].matches("[$].*")) {
        log.debug("2nd option is a column name, not a value");
        return new Condition(conditionVals[0].substring(1),
            conditionVals[1].substring(1), null, operator);
      } else {
        return new Condition(conditionVals[0].substring(1), null,
            conditionVals[1], operator);
      }
    }

    return null;
  }

  public boolean isLineSatisfied(Row row) {
    if (!combinedCondition) {
      if (!listOfConditions.isEmpty()) {
        return listOfConditions.get(0).evaluateCondition(row);
      } else if (this.formula) {
        // Formula case
        this.valueToReturn = jsEvaluator.evaluateJsExpression(
            ParsingUtils.injectRowValuesToAString(row, this.stringFragments)
        );
        return true;
      } else if (this.link) {
        // Link case
        this.valueToReturn = linkToEvaluate.evaluateLink(row);
        return true;
      } else if (this.injection) {
        // Injection case
        this.valueToReturn = ParsingUtils.injectRowValuesToAString(row, this.stringFragments);
        return true;
      } else {
        // Default case
        return true;
      }

    } else {
      // To evaluate a condition assuming AND has precedence over OR, we should:
      // 1. Isolate groups of AND
      // 2. Evaluate each AND group and return true if all are true, false else
      Boolean isConditionSatisfied = false;
      boolean previousResult = listOfConditions.get(0).evaluateCondition(row);

      for (int i = 1; i < listOfConditions.size(); i++) {

        if (listOfConditionsOperators.get(i - 1) == Condition.ConditionOperators.AND) {
          log.debug(
              "The operator between previous condition and this one is AND");
          if (previousResult) {
            log.debug("Previous condition was true, need to evaluate this one");
            previousResult = listOfConditions.get(i).evaluateCondition(row);
            isConditionSatisfied = previousResult;
          } else {
            log.debug(
                "Previous condition was false, no need to evaluate this one");
            break;
          }
        } else {
          log.debug(
              "The operator between previous condition and this one is OR, if previous result is true, escapes otherwise continue to evaluate conditions");
          if (previousResult) {
            isConditionSatisfied = true;
            break;
          } else {
            previousResult = listOfConditions.get(i).evaluateCondition(row);
          }
        }
      }

      return isConditionSatisfied;
    }

  }

}
