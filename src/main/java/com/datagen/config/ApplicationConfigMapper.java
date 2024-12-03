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
package com.datagen.config;


import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;


@Slf4j
@Component
public class ApplicationConfigMapper {

  public static ApplicationConfigs getApplicationConfigFromProperty(
      String propertyName) {
    ApplicationConfigs propValue=null;
    var propParsed=propertyName.toUpperCase(Locale.ROOT)
        .replaceAll("\\.", "_")
        .replaceAll("-", "_");
    try {
      propValue = ApplicationConfigs.valueOf(propParsed);
    } catch (Exception e) {
      log.warn("Cannot find property: {}", propParsed);
    }
    return propValue;
  }

  /**
   * Parse a map of properties into a map of application configs to values
   * @param extraProperties
   * @return
   */
  public static Map<ApplicationConfigs, String> parsePropertiesMap(Map<String, String> extraProperties){
        return extraProperties!=null?
            extraProperties.entrySet().stream().collect(
        Collectors.toMap(
            e -> ApplicationConfigs.valueOf(e.getKey()),
            Map.Entry::getValue
        )) : Collections.emptyMap();
  }
}
