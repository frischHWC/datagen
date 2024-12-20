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
import com.datagen.model.type.CityField;
import com.datagen.model.type.NameField;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;


@Slf4j
public class Link {

  private final String linkedFieldName;
  private final String linkedFieldAttribute;
  private String linkedFieldType;

  Link(String link) {
    String[] linkSplitted = link.replaceAll("[$]", "").split("[.]");
    this.linkedFieldName = linkSplitted[0];
    this.linkedFieldAttribute = linkSplitted[1];
  }

  // This is called post setup of model to register the type of the field which is referenced
  public void setLinkedFieldType(Model model) {
    this.linkedFieldType =
        model.getFields().get(linkedFieldName).getClass().getSimpleName();
    log.debug("Set field type for " + linkedFieldName + " as type : " +
        linkedFieldType);
  }

  public String evaluateLink(Row row) {
    Object linkedField = row.getValues().get(this.linkedFieldName);
    try {
      switch (linkedFieldType) {
        case "NameField":
          return evaluateLinkedName((NameField.Name) linkedField);
        case "CityField":
          return evaluateLinkedCity((CityField.City) linkedField);
        case "CsvField":
          return evaluateLinkedCsv((Map<String, String>) linkedField);
      default:
        log.warn("Not able to find any link for FieldType: " + linkedFieldType +
            " for row: " + row);
        break;
      }

    } catch (Exception e) {
      log.error("Can not evaluate link so returning empty value, see: ", e);
    }

    return "";
  }

  public String evaluateLinkedCity(CityField.City city) {
      return switch (linkedFieldAttribute) {
          case "lat" -> city.getLatitude();
          case "long" -> city.getLongitude();
          case "country" -> city.getCountry();
          default -> {
              log.warn(
                      "Cannot find attribute, returning empty value for city: " + city.getName());
              yield "";
          }
      };
  }

  public String evaluateLinkedCsv(Map<String, String> csvRow) {
    return csvRow.get(linkedFieldAttribute);
  }

  public String evaluateLinkedName(NameField.Name name) {
      return switch (linkedFieldAttribute) {
          case "sex" -> name.getUnisex() ? "UNKNOWN" : name.getMale() ? "MALE" : "FEMALE";
          case "male" -> name.getMale().toString();
          case "female" -> name.getFemale().toString();
          case "unisex" -> name.getUnisex().toString();
          default -> {
              log.warn(
                      "Cannot find attribute, returning empty value for name: " + name.getFirst_name());
              yield "";
          }
      };
  }

}
