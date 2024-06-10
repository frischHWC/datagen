package com.datagen.utils;

import com.datagen.model.Model;
import com.datagen.model.Row;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedList;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


@Slf4j
public class ParsingUtils {

  @AllArgsConstructor
  public static class StringFragment {
    String stringToPrint;
    String variableName;
    Boolean isAVariableToReplace;
  }

  private static final Pattern patternToIdentifyInjections = Pattern.compile("(\\$\\{)([a-zA-Z]*)(\\})");

  /**
   * Parse a String containing column references to other fields
   * And prepare it for future evaluation during generation
   * @param stringToParse
   * @return a linked list of string to either print or compute (get its value from other columns)
   */
  public static LinkedList<StringFragment> parseStringWithVars(String stringToParse) {

    LinkedList<StringFragment> stringParsed = new LinkedList<>();

    Matcher matcher = patternToIdentifyInjections.matcher(stringToParse);

    // Find all places in the regex string where there are column names to replace
    int cursorPosition = 0;
    while (matcher.find()) {
      if(matcher.start()>cursorPosition) {
        // Add string before match
        log.debug("Found string to let as is: {}", stringToParse.substring(cursorPosition,matcher.start()));
        stringParsed.add(new StringFragment(stringToParse.substring(cursorPosition,matcher.start()),null,false));
      }
      // Add match itself
      log.debug("Found column to substitute: {}", matcher.group(2));
      stringParsed.add(new StringFragment(null,matcher.group(2),true));
      cursorPosition = matcher.end();
    }

    // If there are still characters left after last match, add it
    if(cursorPosition<stringToParse.length()) {
      log.debug("Found string to let as is: {}", stringToParse.substring(cursorPosition));
      stringParsed.add(new StringFragment(stringToParse.substring(cursorPosition), null, false));
    }

  return stringParsed;
  }

  // Evaluate fragments by injecting in it
  public static String injectRowValuesToAString(Row row, LinkedList<StringFragment> fragments) {
    Map<String, Object> rowValues = row.getValues();
    Model model = row.getModel();

    return fragments.stream().map(f -> {
      if(f.isAVariableToReplace) {
       return model.getFieldFromName(f.variableName).toStringValue(rowValues.get(f.variableName));
      } else  {
       return f.stringToPrint;
      }
    }).reduce("", String::concat);
  }



}
