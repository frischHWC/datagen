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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class PhoneField extends Field<String> {

  public class Phone {
    @Getter
    String indicator;
    @Getter
    String country;

    public Phone(String indicator, String country) {
      this.indicator = indicator;
      this.country = country;
    }

    @Override
    public String toString() {
      return "Name{" +
          "indicator='" + indicator + '\'' +
          ", country='" + country + '\'' +
          '}';
    }
  }

  private final List<Phone> phoneIndicatorDico;

  public PhoneField(String name, Integer length, List<String> filters) {
    this.name = name;
    this.length = length;
    this.filters = filters;
    this.phoneIndicatorDico = loadPhoneDico();

    this.possibleValuesInternal = new ArrayList<>();

    filters.forEach(filterOnCountry -> {
      this.possibleValuesInternal.addAll(
          phoneIndicatorDico.stream()
              .filter(n -> n.country.equalsIgnoreCase(filterOnCountry))
              .map(n -> n.indicator)
              .toList());
    });
    this.possibleValueSize = this.possibleValuesInternal.size();
    log.debug("There are {} possible values for phone indicators", this.possibleValueSize);
  }

  public String generateRandomValue() {
    String indicator =
        this.possibleValuesInternal.get(random.nextInt(this.possibleValueSize));
    StringBuffer sb = new StringBuffer();
    sb.append("+");
    sb.append(indicator);
    sb.append(" ");
    int counter = 1;
    while (counter <= (11 - indicator.length())) {
      sb.append(random.nextInt(10));
      counter++;
    }

    return sb.toString();
  }

  private List<Phone> loadPhoneDico() {
    try {
      InputStream is = this.getClass().getClassLoader().getResourceAsStream(
          "dictionaries/phone-country-codes.csv");
      return new BufferedReader(
          new InputStreamReader(is, StandardCharsets.UTF_8))
          .lines()
          .map(l -> {
            String[] lineSplitted = l.split(";");
            return new PhoneField.Phone(lineSplitted[0], lineSplitted[1]);
          })
          .collect(Collectors.toList());
    } catch (Exception e) {
      log.warn("Could not load names-dico with error : " + e);
      return Collections.singletonList(new PhoneField.Phone("00", ""));
    }
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

  public static final List<String> listOfAvailableCountries =
      List.of(
          "Afghanistan",
          "Albania",
          "Algeria",
          "American Samoa",
          "Andorra",
          "Angola",
          "Anguilla",
          "Antarctica",
          "Antigua and Barbuda",
          "Argentina",
          "Armenia",
          "Aruba",
          "Australia",
          "Austria",
          "Azerbaijan",
          "Bahamas",
          "Bahrain",
          "Bangladesh",
          "Barbados",
          "Belarus",
          "Belgium",
          "Belize",
          "Benin",
          "Bermuda",
          "Bhutan",
          "Bolivia",
          "Bonaire",
          "Bosnia and Herzegovina",
          "Botswana",
          "Bouvet Island",
          "Brazil",
          "British Indian Ocean Territory",
          "British Virgin Islands",
          "Brunei Darussalam",
          "Bulgaria",
          "Burkina Faso",
          "Burundi",
          "Cabo Verde",
          "Cambodia",
          "Cameroon",
          "Canada",
          "Cayman Islands",
          "Central African Republic",
          "Chad",
          "Chile",
          "China",
          "China, Hong Kong Special Administrative Region",
          "China, Macao Special Administrative Region",
          "Christmas Island",
          "Cocos (Keeling) Islands",
          "Colombia",
          "Comoros",
          "Congo",
          "Cook Islands",
          "Costa Rica",
          "Croatia",
          "Cuba",
          "Curaçao",
          "Cyprus",
          "Czechia",
          "Côte d'Ivoire",
          "Democratic People's Republic of Korea",
          "Democratic Republic of the Congo",
          "Denmark",
          "Djibouti",
          "Dominica",
          "Dominican Republic",
          "Ecuador",
          "Egypt",
          "El Salvador",
          "Equatorial Guinea",
          "Eritrea",
          "Estonia",
          "Eswatini",
          "Ethiopia",
          "Falkland Islands (Malvinas)",
          "Faroe Islands",
          "Fiji",
          "Finland",
          "France",
          "French Guiana",
          "French Polynesia",
          "French Southern Territories",
          "Gabon",
          "Gambia",
          "Georgia",
          "Germany",
          "Ghana",
          "Gibraltar",
          "Greece",
          "Greenland",
          "Grenada",
          "Guadeloupe",
          "Guam",
          "Guatemala",
          "Guernsey",
          "Guinea",
          "Guinea-Bissau",
          "Guyana",
          "Haiti",
          "Heard Island and McDonald Islands",
          "Holy See",
          "Honduras",
          "Hungary",
          "Iceland",
          "India",
          "Indonesia",
          "Iran (Islamic Republic of)",
          "Iraq",
          "Ireland",
          "Isle of Man",
          "Israel",
          "Italy",
          "Jamaica",
          "Japan",
          "Jersey",
          "Jordan",
          "Kazakhstan",
          "Kenya",
          "Kiribati",
          "Kuwait",
          "Kyrgyzstan",
          "Lao People's Democratic Republic",
          "Latvia",
          "Lebanon",
          "Lesotho",
          "Liberia",
          "Libya",
          "Liechtenstein",
          "Lithuania",
          "Luxembourg",
          "Madagascar",
          "Malawi",
          "Malaysia",
          "Maldives",
          "Mali",
          "Malta",
          "Marshall Islands",
          "Martinique",
          "Mauritania",
          "Mauritius",
          "Mayotte",
          "Mexico",
          "Micronesia",
          "Monaco",
          "Mongolia",
          "Montenegro",
          "Montserrat",
          "Morocco",
          "Mozambique",
          "Myanmar",
          "Namibia",
          "Nauru",
          "Nepal",
          "Netherlands",
          "New Caledonia",
          "New Zealand",
          "Nicaragua",
          "Niger",
          "Nigeria",
          "Niue",
          "Norfolk Island",
          "Northern Mariana Islands",
          "Norway",
          "Oman",
          "Pakistan",
          "Palau",
          "Panama",
          "Papua New Guinea",
          "Paraguay",
          "Peru",
          "Philippines",
          "Pitcairn",
          "Poland",
          "Portugal",
          "Puerto Rico",
          "Qatar",
          "Republic of Korea",
          "Republic of Moldova",
          "Romania",
          "Russian Federation",
          "Rwanda",
          "Réunion",
          "Saint Barthélemy",
          "Saint Helena",
          "Saint Kitts and Nevis",
          "Saint Lucia",
          "Saint Martin",
          "Saint Pierre and Miquelon",
          "Saint Vincent and the Grenadines",
          "Samoa",
          "San Marino",
          "Sao Tome and Principe",
          "Sark",
          "Saudi Arabia",
          "Senegal",
          "Serbia",
          "Seychelles",
          "Sierra Leone",
          "Singapore",
          "Sint Maarten",
          "Slovakia",
          "Slovenia",
          "Solomon Islands",
          "Somalia",
          "South Africa",
          "South Georgia and the South Sandwich Islands",
          "South Sudan",
          "Spain",
          "Sri Lanka",
          "State of Palestine",
          "Sudan",
          "Suriname",
          "Svalbard and Jan Mayen Islands",
          "Sweden",
          "Switzerland",
          "Syrian Arab Republic",
          "Tajikistan",
          "Thailand",
          "The former Yugoslav Republic of Macedonia",
          "Timor-Leste",
          "Togo",
          "Tokelau",
          "Tonga",
          "Trinidad and Tobago",
          "Tunisia",
          "Turkey",
          "Turkmenistan",
          "Turks and Caicos Islands",
          "Tuvalu",
          "UK",
          "USA",
          "Uganda",
          "Ukraine",
          "United Arab Emirates",
          "United Republic of Tanzania",
          "United States Virgin Islands",
          "Uruguay",
          "Uzbekistan",
          "Vanuatu",
          "Venezuela (Bolivarian Republic of)",
          "Viet Nam",
          "Wallis and Futuna Islands",
          "Western Sahara",
          "Yemen",
          "Zambia",
          "Zimbabwe",
          "voland Islands"
      );

}