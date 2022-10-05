package com.cloudera.frisch.randomdatagen.controller;


import com.cloudera.frisch.randomdatagen.service.ModelTesterSevice;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;



@Slf4j
@RestController
@RequestMapping("/model")
public class ModelTesterController {

  @Autowired
  private ModelTesterSevice modelTesterSevice;

  @PostMapping(value = "/test")
  public String generateIntoMultipleSinks(
      @RequestParam(required = false, name = "model") String modelFilePath
  ) {
    log.debug("Received request to test model: {} ,", modelFilePath);
    return modelTesterSevice.generateData(modelFilePath);
  }
}
