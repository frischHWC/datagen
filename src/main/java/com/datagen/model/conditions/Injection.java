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
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;


import java.util.LinkedList;
import java.util.Map;

@Slf4j
public class Injection {

  @AllArgsConstructor
  private class InjectedField {
    String stringToPrint;
    Boolean toReplace;
  }

  private final LinkedList<InjectedField> injectedFieldNames =
      new LinkedList<>();

  Injection(String injection) {
    for (String s : injection.split("[$]")) {
      if (s.length() != 0) {
        if (s.charAt(0) != '{') {
          log.debug(s + " is not a variable name");
          injectedFieldNames.add(new InjectedField(s, false));
        } else {
          String fieldToAdd = s.substring(1, s.indexOf('}'));
          log.debug(fieldToAdd + " is found as a variable name");
          injectedFieldNames.add(new InjectedField(fieldToAdd, true));
          if (s.length() > s.indexOf('}')) {
            log.debug(
                s.substring(s.indexOf('}') + 1) + " is not a variable name");
            injectedFieldNames.add(
                new InjectedField(s.substring(s.indexOf('}') + 1), false));
          }
        }
      }
    }
  }

  public String evaluateInjection(Row row) {
    Map<String, Object> rowValues = row.getValues();
    StringBuilder sb = new StringBuilder();
    try {
      for (InjectedField fieldNameToReplace : injectedFieldNames) {
        if (fieldNameToReplace.toReplace) {
          sb.append(
              row.getModel().getFieldFromName(fieldNameToReplace.stringToPrint)
                  .toStringValue(
                      rowValues.get(fieldNameToReplace.stringToPrint)));
        } else {
          sb.append(fieldNameToReplace.stringToPrint);
        }
      }
    } catch (Exception e) {
      log.error("Can not evaluate injection so returning empty value, see: ",
          e);
    }

    return sb.toString();
  }


}
