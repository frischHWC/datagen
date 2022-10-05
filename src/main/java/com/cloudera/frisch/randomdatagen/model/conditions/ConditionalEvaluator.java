package com.cloudera.frisch.randomdatagen.model.conditions;


import com.cloudera.frisch.randomdatagen.model.Row;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.log4j.Logger;

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
        Iterator<Map.Entry<String, String>> condIterator = conditionals.entrySet().iterator();
        while(condIterator.hasNext()) {
          Map.Entry<String, String> condLine = condIterator.next();
          conditionLines.add(new ConditionsLine(condLine.getKey(), condLine.getValue()));
          log.debug(" Added condition line: " + condLine.getKey() + " : " + condLine.getValue());
        }
  }

  public String evaluateConditions(Row row) {
    for(ConditionsLine cl: conditionLines){
      if(cl.isLineSatisfied(row)) {
        return cl.getValueToReturn();
      }
    }
    return "";
  }


}
