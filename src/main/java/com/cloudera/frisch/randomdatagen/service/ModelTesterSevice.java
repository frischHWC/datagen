package com.cloudera.frisch.randomdatagen.service;


import com.cloudera.frisch.randomdatagen.config.ApplicationConfigs;
import com.cloudera.frisch.randomdatagen.config.PropertiesLoader;
import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.Row;
import com.cloudera.frisch.randomdatagen.parsers.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;


@Service
@Slf4j
public class ModelTesterSevice {


  @Autowired
  private PropertiesLoader propertiesLoader;

  public String generateData(@Nullable MultipartFile modelFileAsFile, @Nullable String modelFilePath)
  {
    log.info("Starting Generation");
    long start = System.currentTimeMillis();

    // Get default values if some are not set
    Map<ApplicationConfigs, String> properties = propertiesLoader.getPropertiesCopy();

    String modelFile = modelFilePath;
    if(modelFilePath==null) {
      log.info("No model file passed, will default to custom data model or default defined one in configuration");
      if(properties.get(ApplicationConfigs.CUSTOM_DATA_MODEL_DEFAULT)!=null) {
        modelFile = properties.get(ApplicationConfigs.CUSTOM_DATA_MODEL_DEFAULT);
      } else {
        modelFile = properties.get(ApplicationConfigs.DATA_MODEL_PATH_DEFAULT) +
            properties.get(ApplicationConfigs.DATA_MODEL_DEFAULT);
      }
    }
    if(modelFilePath!=null && !modelFilePath.contains("/")){
      log.info("Model file passed is identified as one of the one provided, so will look for it in data model path: {} ",
          properties.get(ApplicationConfigs.DATA_MODEL_PATH_DEFAULT));
      modelFile = properties.get(ApplicationConfigs.DATA_MODEL_PATH_DEFAULT) + modelFilePath;
    }
    if(modelFileAsFile!=null && !modelFileAsFile.isEmpty()) {
      log.info("Model passed is an uploaded file");
      modelFile = properties.get(ApplicationConfigs.DATA_MODEL_RECEIVED_PATH) + "/model-test-" + new Random().nextInt() + ".json";
      try {
        modelFileAsFile.transferTo(new File(modelFile));
      } catch (IOException e) {
        log.error("Could not save model file passed in request locally, due to error:", e);
        return "{ \"commandUuid\": \"\" , \"error\": \"Error with Model File - Cannot save it locally\" }";
      }
    }

    // Parsing model
    log.info("Parsing of model file: {}", modelFile);
    JsonParser parser = new JsonParser(modelFile);
    if(parser.getRoot()==null) {
      log.warn("Error when parsing model file");
      return "{ \"commandUuid\": \"\" , \"error\": \"Error with Model File - Verify its path and structure\" }";
    }
    Model model = parser.renderModelFromFile();

    List<Row> randomDataList = model.generateRandomRows(1, 1);

    return randomDataList.get(0).toJSON();
  }
}
