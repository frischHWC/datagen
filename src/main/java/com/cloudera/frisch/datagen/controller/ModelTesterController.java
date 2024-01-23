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
package com.cloudera.frisch.datagen.controller;


import com.cloudera.frisch.datagen.service.ModelTesterSevice;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;


@Slf4j
@RestController
@RequestMapping("/model")
public class ModelTesterController {

  @Autowired
  private ModelTesterSevice modelTesterSevice;

  @PostMapping(value = "/test", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  public String testModel(
      @RequestPart(required = false, name = "model_file")
          MultipartFile modelFile,
      @RequestParam(required = false, name = "model") String modelFilePath
  ) {
    log.debug("Received request to test model: {} ,", modelFilePath);
    return modelTesterSevice.generateData(modelFile, modelFilePath);
  }
}
