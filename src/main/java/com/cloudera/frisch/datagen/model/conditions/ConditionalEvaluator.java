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
package com.cloudera.frisch.datagen.model.conditions;


import com.cloudera.frisch.datagen.model.Row;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
public class ConditionalEvaluator {

  @Getter
  @Setter
  public LinkedList<ConditionsLine> conditionLines = new LinkedList<>();

  /*
  A conditionalEvaluator is responsible for preparing conditions evaluations set on a field by parsing the list of conditions to met
   */
  public ConditionalEvaluator(LinkedHashMap<String, String> conditionals) {
    Iterator<Map.Entry<String, String>> condIterator =
        conditionals.entrySet().iterator();
    while (condIterator.hasNext()) {
      Map.Entry<String, String> condLine = condIterator.next();
      conditionLines.add(
          new ConditionsLine(condLine.getKey(), condLine.getValue()));
      log.debug(" Added condition line: " + condLine.getKey() + " : " +
          condLine.getValue());
    }
  }

  public String evaluateConditions(Row row) {
    for (ConditionsLine cl : conditionLines) {
      if (cl.isLineSatisfied(row)) {
        return cl.getValueToReturn();
      }
    }
    return "";
  }


}
