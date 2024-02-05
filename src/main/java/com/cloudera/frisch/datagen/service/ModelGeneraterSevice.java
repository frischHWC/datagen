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
package com.cloudera.frisch.datagen.service;


import com.cloudera.frisch.datagen.config.ApplicationConfigs;
import com.cloudera.frisch.datagen.config.PropertiesLoader;
import com.cloudera.frisch.datagen.config.SinkParser;
import com.cloudera.frisch.datagen.connector.ConnectorInterface;
import com.cloudera.frisch.datagen.connector.ConnectorsUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;


@Service
@Slf4j
public class ModelGeneraterSevice {


  @Autowired
  private PropertiesLoader propertiesLoader;

  public String generateModel(
      String source,
      @Nullable String database,
      @Nullable String table
  ) {
    Map<ApplicationConfigs, String> properties =
        propertiesLoader.getPropertiesCopy();

    ConnectorInterface sink = ConnectorsUtils
        .sinksInit(null, properties,
            Collections.singletonList(SinkParser.stringToSink(source)), false)
        .get(0);

    // TODO: Call generate models
    // => Refactor Sink to be "connectors" and have init/terminate/sendRows/GenModel

    // TODO : Implement logic foreach connector to get model

    return "";
  }
}
