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

import lombok.extern.slf4j.Slf4j;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.PolyglotException;

@Slf4j
public class JsEvaluator {

  private final Context context;

  JsEvaluator() {
    this.context = Context.newBuilder()
        .allowAllAccess(true)
        .build();
    context.initialize("js");
  }

  synchronized String evaluateJsExpression(String expression) {
    var toReturn = "";
    synchronized (context) {
      Object value = 0f;
      try {
        value = context.eval("js", expression);
        log.debug("Evaluating formula: " + expression + " to: " + value);
      } catch (PolyglotException e) {
        log.warn("Could not evaluate expression: " + expression + " due to error: ",
                e);
      }
      toReturn = value.toString();
    }
    return toReturn;
  }


}
