package com.cloudera.frisch.randomdatagen.model.conditions;

import com.cloudera.frisch.randomdatagen.model.Row;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.log4j.Logger;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.LinkedList;

@Slf4j
public class Formula {
  

  // for all cols name existing in model, try to find which one are involved in the formula and put them in a list
  @Getter @Setter
  private LinkedList<String> listOfColsToEvaluate;

  @Getter @Setter
  private String formulaToEvaluate;

  private final ScriptEngineManager scriptEngineManager;
  private final ScriptEngine scriptEngine;

  Formula(String formula) {
    // fill in the listOfColsToEvaluate + Create formula string with no $
    listOfColsToEvaluate = new LinkedList<>();
    for(String field: formula.substring(formula.indexOf("$")+1).split("[$]")) {
        listOfColsToEvaluate.add(field.split("\\s+")[0]);
        log.debug("Add Field : " + field.split("\\s+")[0] + " to be in the formula");
    }
    formulaToEvaluate = formula.replaceAll("[$]", "");
    scriptEngineManager = new ScriptEngineManager();
    scriptEngine = scriptEngineManager.getEngineByName("JavaScript");
  }

  public String evaluateFormula(Row row) {
    // Evaluate formula using an evaluator (or built this evaluator)
    String formulaReplaced = formulaToEvaluate;
    for(String colName: listOfColsToEvaluate) {
      log.debug(formulaReplaced);
      formulaReplaced = formulaReplaced.replaceAll("(^| )" + colName + "($| )", row.getValues().get(colName).toString());
    }
    log.debug(formulaReplaced);
    return computeFormula(formulaReplaced);
  }

  private String computeFormula(String formula) {
    Object value = 0f;
    try {
       value = scriptEngine.eval(formula);
       log.debug("Evaluating formula: " + formula + " to: " + value);
    } catch (ScriptException e) {
      log.warn("Could not evaluate expression: " + formula + " due to error: ", e);
    }
    return value.toString();
  }



}
