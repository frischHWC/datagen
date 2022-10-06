package com.cloudera.frisch.randomdatagen.controller;


import com.cloudera.frisch.randomdatagen.service.ModelTesterSevice;
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
