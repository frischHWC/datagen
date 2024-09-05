package com.datagen.service.model;


import com.datagen.Main;
import com.datagen.config.ApplicationConfigs;
import com.datagen.config.PropertiesLoader;
import com.datagen.connector.storage.utils.FileUtils;
import com.datagen.model.Model;
import com.datagen.parsers.JsonParser;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 Models are stored on disk but are prepared to switch to another store (db?)
 - On start-up, models are listed and loaded into memory (until a limit of 10K)
 - All models are listed in a hash map (with model wrapper object pointing to model, file path and other infos)
 - Currently, it supposes all models are retained in-memory
 - Currently it supports and assumes that official internal Models representation is JSON

 Methods allows to:
 - List models
 - Add a model (by saving on disk & add it to in-memory)
 - Delete a model (by deleting on-disk & in-memory)
 - Get a model as a JSON file or as a Model object or as a File
 **/
@Service
@Slf4j
public class ModelStoreService {

  @Getter
  @Setter
  @AllArgsConstructor
  public class ModelStored {
    String name;
    Model model;
    String owner;
    // Depend on storage
    String path;
  }

  private final Map<ApplicationConfigs, String> properties;
  private final HashMap<String, ModelStored> storedModels;
  private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy_HH-mm-ss");

  // TODO: Load default models
  @Autowired
  public ModelStoreService(PropertiesLoader propertiesLoader) {
    this.properties = propertiesLoader.getPropertiesCopy();
    this.storedModels = new HashMap<>();

    // Dependent on storage
    FileUtils.createLocalDirectoryWithStrongRights(properties
        .get(ApplicationConfigs.DATAGEN_MODEL_STORE_PATH));

    // Take in charge of the received models ?
    FileUtils.createLocalDirectory(properties
        .get(ApplicationConfigs.DATAGEN_MODEL_RECEIVED_PATH));

    // Take in charge of the generated models?
    FileUtils.createLocalDirectory(properties
        .get(ApplicationConfigs.DATAGEN_MODEL_GENERATED_PATH));

    // Load default models
    if(properties.get(ApplicationConfigs.DATAGEN_LOAD_DEFAULT_MODELS).equalsIgnoreCase("true")) {
      var files = List.of(
          "/models/customer/customer-uk-model.json",
          "/models/customer/customer-japan-model.json",
          "/models/customer/customer-usa-model.json",
          "/models/public_service/intervention-team-model.json",
          "/models/public_service/weather-model.json",
          "/models/industry/plant-model.json",
          "/models/industry/sensor-model.json",
          "/models/industry/sensor-data-model.json"
          );
      for(String file: files) {
        var tempFilePath = properties.get(ApplicationConfigs.DATAGEN_MODEL_RECEIVED_PATH) + "/"
            + file.substring(file.lastIndexOf('/')+1) + ".tmp";
        try {
          var fileAsStream = Main.class.getResourceAsStream(file);
          if(fileAsStream!=null) {
            var fos = new FileOutputStream(tempFilePath);
            fos.write(fileAsStream.readAllBytes());
            fos.close();
            fileAsStream.close();
            addModel(tempFilePath, false);
          } else {
            log.info("Cannot load default model: {} as it is not found", file);
          }
        } catch (Exception e) {
          log.warn("Could not load default model: {} with error: ", file, e);
        }
      }
    }

    // Load existing models
    var files = FileUtils.listLocalFiles(properties
        .get(ApplicationConfigs.DATAGEN_MODEL_STORE_PATH));
    if(files!=null && files.length>0){
      if(files.length>10000) {
        log.warn("Unable to load models, because there are too much, do some manual cleaning in folder: {} and restart server",
            properties
                .get(ApplicationConfigs.DATAGEN_MODEL_STORE_PATH));
      }
      List.of(files).forEach(f -> {
        try {
          var model = new JsonParser<>(f.getAbsolutePath()).renderModelFromFile(properties);
          synchronized (storedModels) {
            storedModels.put(f.getName(),
                new ModelStored(f.getName().split("[.]")[0], model, null,
                    f.getAbsolutePath()));
          }
        } catch (Exception e) {
          log.warn("Could not parse model file in : {} ; Verify its structure and delete it if not conform", f.getAbsolutePath());
        }
      });
    }

  }

  /**
   * List all models remained in-memory
   * @return
   */
  public List<String> listModels() {
    return storedModels.keySet().stream().toList();
  }

  /**
   * List all models remained in-memory
   * @return
   */
  public List<ModelStored> listModelsAsModelStored() {
    return storedModels.values().stream().toList();
  }

  /**
   * List all models remained in-memory
   * @return
   */
  public List<Model> listModelsAsModels() {
    return storedModels.values().stream().map(ModelStored::getModel).toList();
  }

  /**
   * Check if a model already exists
   * @param modelName
   * @return
   */
  public boolean checkModelExists(String modelName) {
    return storedModels.values().stream().anyMatch(m -> m.getName().equalsIgnoreCase(modelName));
  }

  /**
   * This function assumes that model name is correctly set as it used for its file name
   * @param model
   * @return the model
   */
  public Model addModel(Model model, boolean keepExisting) {
    // Check if model is not already existing
    if(storedModels.containsKey(model.getName()) && keepExisting) {
      log.warn("Model: {} already exists, so adding a date to uniquely identifies it", model.getName());
      model.setName(model.getName()+"-"+formatter.format(LocalDateTime.now()));
    }
    var filePath = properties.get(ApplicationConfigs.DATAGEN_MODEL_STORE_PATH)+"/"+model.getName();
    // Write JSON representation of model
    model.toJsonSchema(filePath);
    synchronized (storedModels) {
      storedModels.put(model.getName(),
          new ModelStored(model.getName(), model, "admin", filePath));
    }
    log.debug("Added successfully model: {}", model.getName());
    return model;
  }

  /**
   * This function parses and creates the model from a file
   * @param filepath
   * @return name to identify the model
   */
  public Model addModel(String filepath, boolean keepExisting) {
    var parser = new JsonParser(filepath);
    if (parser.getRoot() == null) {
      log.warn("Error when parsing model file");
      return null;
    }
    var model = parser.renderModelFromFile(properties);
    // Check if model name exists, otherwise use file name
    if(model.getName()==null || model.getName().isEmpty()){
      model.setName(filepath.substring(filepath.lastIndexOf('/')));
    }
    return addModel(model, keepExisting);
  }

  /**
   * This function parses and creates the model from a file
   * @param file
   * @return name to identify the model
   */
  public Model addModel(File file, boolean keepExisting) {
    return addModel(file.getAbsolutePath(), keepExisting);
  }

  /**
   * This function parses and creates the model from a file
   * @param file
   * @return name to identify the model
   */
  public Model addModel(MultipartFile file, boolean keepExisting) {
    var modelFilePath = properties.get(ApplicationConfigs.DATAGEN_MODEL_RECEIVED_PATH) +
        "/model-" + System.currentTimeMillis() + "-" + String.format("%06d",new Random().nextInt(100000)) + ".json";
    try {
      file.transferTo(new File(modelFilePath));
    } catch (IOException e) {
      log.error("Cannot write model to file: {} with error: ", modelFilePath, e);
    }
    return addModel(modelFilePath, keepExisting);
  }

  /**
   * This function parses and creates the model from json data passed as an input stream directly
   * @param json
   * @return name to identify the model
   */
  public Model addModel(InputStream json, boolean keepExisting) {
    var parser = new JsonParser(json);
    if (parser.getRoot() == null) {
      log.warn("Error when parsing model as input");
      return null;
    }
    var model = parser.renderModelFromFile(properties);
    return addModel(model, keepExisting);
  }

  /**
   * This function assumes that model name is correctly set as it used for its file name
   */
  public void deleteModel(String modelName) {
    // Check if model exists
    if(!storedModels.containsKey(modelName)) {
      log.warn("Model: {} does not exists in-memory", modelName);
    }
    var filePath = properties.get(ApplicationConfigs.DATAGEN_MODEL_STORE_PATH)+"/"+modelName;
    // Write JSON representation of model
    FileUtils.deleteLocalFile(filePath);
    synchronized (storedModels) {
      storedModels.remove(modelName);
    }
    log.debug("Deleted successfully model: {}", modelName);
  }

  public Model getModel(String modelName) {
    if(storedModels.containsKey(modelName)) {
      return storedModels.get(modelName).getModel();
    } else {
      return null;
    }
  }

  public ModelStored getModelAsModelStored(String modelName) {
    return storedModels.getOrDefault(modelName, null);
  }

  public List<String> getModelAsStringList(String modelName) {
    if (storedModels.containsKey(modelName)) {
      return FileUtils.getfileContent(storedModels.get(modelName).getPath());
    } else {
      return Collections.emptyList();
    }
  }

  public File getModelAsFile(String modelName) {
    if(storedModels.containsKey(modelName)) {
      return new File(storedModels.get(modelName).getPath());
    } else {
      return null;
    }
  }

  public String getModelAsJson(String modelName) {
    var modelAsJson = "";
    if(storedModels.containsKey(modelName)) {
      var storedModel = storedModels.get(modelName);
      try {
        modelAsJson = FileUtils.getfileContent(storedModel.getPath()).stream().collect(
            Collectors.joining("\n"));
      } catch (Exception e) {
        log.warn("Could not get model as json from file: {}", storedModel.getPath());
      }
      if(modelAsJson.isEmpty()) {
        modelAsJson = storedModel.getModel().toJsonSchema();
      }
    }
    return modelAsJson;
  }





}
