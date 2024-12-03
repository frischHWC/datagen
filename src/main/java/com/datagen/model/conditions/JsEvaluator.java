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

import com.datagen.config.ApplicationConfigs;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.PolyglotException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class JsEvaluator {

  private class ContextEvaluator {
    private Context context;
    @Getter
    @Setter
    private boolean used;

    public ContextEvaluator(String language) {
      this.context = Context.newBuilder()
              .allowAllAccess(true)
              .build();
      context.initialize(language);
      this.used = false;
    }
  }

  private final List<ContextEvaluator> contexts;

  JsEvaluator(Map<ApplicationConfigs, String> properties) {
    this.contexts = new ArrayList<>();
    int numberOfContext = Integer.parseInt(properties.getOrDefault(ApplicationConfigs.GENERATION_JS_EVALUATOR_CONTEXT_NUMBER, "1"));
    for(int i=0;i<numberOfContext;i++){
      this.contexts.add(new ContextEvaluator(properties.getOrDefault(ApplicationConfigs.GENERATION_JS_EVALUATOR_CONTEXT_LANGUAGE, "js")));
    }
  }

  synchronized String evaluateJsExpression(String expression) {
    var toReturn = "";
    Object value = 0f;
    ContextEvaluator contextToUse;

    // Loop until one context is found available
    synchronized (this.contexts) {
      outerloop:
      while(true) {
        for (ContextEvaluator contextEvaluator : this.contexts) {
          if (!contextEvaluator.isUsed()) {
            contextToUse = contextEvaluator;
            contextToUse.setUsed(true);
            break outerloop;
          }
        }
      }
    }

    try {
        value = contextToUse.context.eval("js", expression);
        contextToUse.setUsed(false);
        log.debug("Evaluating formula: {} to: {}", expression, value);
    } catch (PolyglotException e) {
        log.warn("Could not evaluate expression: {} due to error: ", expression, e);
    }
    toReturn = value.toString();

    return toReturn;
  }


}
