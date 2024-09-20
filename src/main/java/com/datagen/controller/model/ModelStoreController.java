package com.datagen.controller.model;

import com.datagen.service.model.ModelStoreService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("/api/v1/model_store")
public class ModelStoreController {

  @Autowired
  private ModelStoreService modelStoreService;

  @PostMapping(value = "/add", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  public String addModel(
      @RequestPart(required = false, name = "model_file")
      MultipartFile modelFile
  ) {
    log.debug("Received request to add model");
    return modelStoreService.addModel(modelFile, false).getName();
  }

  @GetMapping(value = "/get")
  public String getModel(
      @RequestParam(required = false, name = "model") String modelName
  ) {
    log.debug("Received request to get model: {} ,", modelName);
    return modelStoreService.getModelAsJson(modelName);
  }

  @GetMapping(value = "/list")
  public List<String> listModel(
  ) {
    log.debug("Received request to list model");
    return modelStoreService.listModels();
  }

  @DeleteMapping(value = "/delete")
  public Boolean deleteModel(
      @RequestParam(required = false, name = "model") String modelName
  ) {
    log.debug("Received request to delete model: {} ,", modelName);
    modelStoreService.deleteModel(modelName);
    return modelStoreService.checkModelExists(modelName);
  }

}
