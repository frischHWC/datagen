package com.cloudera.frisch.randomdatagen.model.conditions;

import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.Row;
import com.cloudera.frisch.randomdatagen.model.type.CityField;
import lombok.extern.slf4j.Slf4j;
import org.apache.log4j.Logger;

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
  public void setLinkedFieldType(Model model){
    this.linkedFieldType = model.getFields().get(linkedFieldName).getClass().getSimpleName();
    log.debug("Set field type for " + linkedFieldName + " as type : " + linkedFieldType);
  }

  public String evaluateLink(Row row) {
    Object linkedField = row.getValues().get(this.linkedFieldName);
    try {
      switch(linkedFieldType) {
      case "CityField":
        return evaluateLinkedCity((CityField.City) linkedField);
      case "CsvField":
        return evaluateLinkedCsv((Map<String, String>) linkedField);
      default:
        log.warn("Not able to find any link for FieldType: " + linkedFieldType + " for row: " + row );
        break;
      }

    } catch (Exception e) {
      log.error("Can not evaluate link so returning empty value, see: ", e);
    }

    return "";
  }

  public String evaluateLinkedCity(CityField.City city) {
    switch (linkedFieldAttribute) {
    case "lat":
      return city.getLatitude();
    case "long":
      return city.getLongitude();
    case "country":
      return city.getCountry();
    default:
      log.warn("Cannot find attribute, returning empty value for city: " + city);
      return "";
    }
  }

  public String evaluateLinkedCsv(Map<String, String> csvRow) {
    return csvRow.get(linkedFieldAttribute);
  }



}
