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
package com.datagen.model.type;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hive.jdbc.HivePreparedStatement;
import org.apache.kudu.Type;
import org.apache.kudu.client.PartialRow;
import org.apache.orc.TypeDescription;

import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class StringRegexField extends Field<String> {

  private final LinkedList<List<String>> possibleCharsList = new LinkedList();

  private final String alphaNumericString =
      "abcdefghijklmnopqrstuvxyz" +
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
      "0123456789";

  // pattern to match all groups that needs to be replaced by something (all rest should be as is)
  private final Pattern pattern = Pattern.compile("(\\[[^]]*\\])(\\{[0-9]*\\})");
  private final Pattern patternReadRegexInterval = Pattern.compile("[A-Za-z0-9]-[A-Za-z0-9]");

  @AllArgsConstructor
  @Getter
  private class matchedString {
    String regexToEvaluate;
    String occurrencesToEvaluate;
    int start;
    int end;
  }

  public StringRegexField(String name, String regex) {
    this.name = name;

    Matcher matcher = pattern.matcher(regex);

    // Find all places in the regex string where there are replacing groups
    LinkedList<matchedString> matchedStrings = new LinkedList<>();
    while (matcher.find()) {
      if (matcher.groupCount() == 2) {
        matchedStrings.add(
            new matchedString(matcher.group(1), matcher.group(2),
                matcher.start(),
                matcher.end())
        );
        log.debug(
            "Found regex to analyze {} with occurrences {} at index {} until index {}",
            matcher.group(1), matcher.group(2), matcher.start(), matcher.end());
      } else {
        log.warn("Regex found is not formatted correctly: {}", matcher.group(0));
      }
    }

    int cursorPosition = 0;
    for (int i = 0; i < matchedStrings.size(); i++) {
      if (cursorPosition < matchedStrings.get(i).getStart()) {
        // This is the case where this is just strings to add (nothing to compute as a regex)
        // Add to possibleCharsList the strings from cursor to start (not included) + set cursor to start position
        for (int j = cursorPosition; j < matchedStrings.get(i).getStart(); j++) {
          log.debug("Normal string (not interpreted as regex): {}", regex.charAt(j));
          possibleCharsList.add(Collections.singletonList(String.valueOf(regex.charAt(j))));
        }
      }
        // Analyze the string to add smth to possibleCharsList + set cursor to end position
        log.debug("Regex to analyze: {}",
            matchedStrings.get(i).getRegexToEvaluate());
        possibleCharsList.addAll(
            analyzeRegex(matchedStrings.get(i).getRegexToEvaluate(), matchedStrings.get(i).getOccurrencesToEvaluate())
        );
        cursorPosition = matchedStrings.get(i).getEnd();
    }
    // Terminate until end of regex if needed
    if(cursorPosition < regex.length()) {
      for (int j = cursorPosition; j < regex.length(); j++) {
        log.debug("Normal string (not interpreted as regex): {}", regex.charAt(j));
        possibleCharsList.add(Collections.singletonList(
            String.valueOf(regex.charAt(j))));
      }
    }

  }

  /**
   * Analyze a regex and return foreach character to generate a list of possible charachters, all being wrapped into a linkedlist (to keep order)
   *
   * @param readRegexVal that looks like: [possibleCharacters either as interval separated by a '-' or list separated by a ',']
   * @param readRegexOcc that looks like: ( a number )
   * @return
   */
  private LinkedList<List<String>> analyzeRegex(String readRegexVal,
                                                String readRegexOcc) {
    LinkedList<List<String>> regexList = new LinkedList<>();

    int occurences =
        Integer.parseInt(readRegexOcc.substring(1, readRegexOcc.length() - 1));
    log.debug("Found {} occurrences to add", occurences);

    List<String> regexUniqueList = new ArrayList<>();
    String regexToAnalyze =
        readRegexVal.substring(1, readRegexVal.length() - 1);
    log.debug("Choices of the regex to analyze are: {}", regexToAnalyze);

    // Analyze regex to extract possible values
    String remainingExpressionsToAdd = regexToAnalyze;
    Matcher matcher = patternReadRegexInterval.matcher(regexToAnalyze);
    while (matcher.find()) {
      log.debug("Found an interval to add: {}", matcher.group());
      String[] interval = matcher.group().split("-");
      String charsMatchedByRegex =
          alphaNumericString.substring(alphaNumericString.indexOf(interval[0]),
              alphaNumericString.indexOf(interval[1]) + 1);
      charsMatchedByRegex.chars().forEach(c ->
          regexUniqueList.add(String.valueOf((char) c))
      );
      remainingExpressionsToAdd = matcher.replaceAll("");
    }

    // N.B: Juts before it replaces interval, so only retrieving values separated by a comma
    log.debug("Remaining choices to add are: {}", remainingExpressionsToAdd);
    regexUniqueList.addAll(
        Arrays.stream(remainingExpressionsToAdd.split(","))
            .filter(t -> !t.isEmpty())
            .collect(Collectors.toList())
    );

    // Add list as many times as occurrences are found
    for (int o = 0; o < occurences; o++) {
      regexList.add(regexUniqueList);
    }

    return regexList;
  }

  public String generateRandomValue() {
    StringBuilder sb = new StringBuilder();
    possibleCharsList.forEach(c -> {
      if(c.size()==1) {
        sb.append(c.get(0));
      } else {
        sb.append(c.get(random.nextInt(c.size())));
      }
    });
    return sb.toString();
  }

    /*
     Override if needed Field function to insert into special connectors
     */

  @Override
  public Put toHbasePut(String value, Put hbasePut) {
    hbasePut.addColumn(Bytes.toBytes(hbaseColumnQualifier), Bytes.toBytes(name),
        Bytes.toBytes(value));
    return hbasePut;
  }

  @Override
  public PartialRow toKudu(String value, PartialRow partialRow) {
    partialRow.addString(name, value);
    return partialRow;
  }

  @Override
  public Type getKuduType() {
    return Type.STRING;
  }

  @Override
  public HivePreparedStatement toHive(String value, int index,
                                      HivePreparedStatement hivePreparedStatement) {
    try {
      hivePreparedStatement.setString(index, value);
    } catch (SQLException e) {
      log.warn("Could not set value : " + value.toString() +
          " into hive statement due to error :", e);
    }
    return hivePreparedStatement;
  }

  @Override
  public String getHiveType() {
    return "STRING";
  }

  @Override
  public String getGenericRecordType() {
    return "string";
  }

  @Override
  public ColumnVector getOrcColumnVector(VectorizedRowBatch batch, int cols) {
    return batch.cols[cols];
  }

  @Override
  public TypeDescription getTypeDescriptionOrc() {
    return TypeDescription.createString();
  }
}
