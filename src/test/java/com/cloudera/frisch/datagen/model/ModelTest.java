/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.frisch.datagen.model;

import org.apache.log4j.Logger;
import org.junit.Test;

public class ModelTest {

    private final static Logger logger = Logger.getLogger(ModelTest.class);


    @Test
    public void checkMapHbasecolMapping() {
        /*JsonParser jsonParser = new JsonParser("/Users/frisch/Documents/CodeProjects/Professional/random-datagen/src/main/resources/full-model.json");
        Model model = jsonParser.renderModelFromFile();
        model.convertHbaseColFamilyOption("c:col1,col2,col3;d:col4,col5,col6")
                .forEach((k,v) -> logger.warn("key : " + k + " - value: " + v)); */

    }
}
