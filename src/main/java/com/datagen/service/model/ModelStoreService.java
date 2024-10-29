package com.datagen.service.model;


import com.datagen.Main;
import com.datagen.config.ApplicationConfigs;
import com.datagen.config.PropertiesLoader;
import com.datagen.connector.storage.utils.FileUtils;
import com.datagen.model.Model;
import com.datagen.parsers.JsonModelMetaParser;
import com.datagen.parsers.JsonModelMetaUnparser;
import com.datagen.parsers.JsonModelParser;
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
    // Come from metadata file
    ModelMetaStored modelMeta;
    // Depend on storage
    String path;
    public String getOwner() {
      return this.modelMeta.getOwner();
    }
  }

  @Getter
  @Setter
  public static class ModelMetaStored {
    String owner;
    Set<String> allowedUsers;
    Set<String> allowedGroups;
    Set<String> allowedAdminUsers;
    Set<String> allowedAdminGroups;
    public ModelMetaStored(String owner) {
      this.owner = owner;
      this.allowedUsers = new HashSet<>();
      this.allowedGroups = new HashSet<>();
      this.allowedAdminUsers = new HashSet<>();
      this.allowedAdminGroups = new HashSet<>();
    }
  }

  private final Map<ApplicationConfigs, String> properties;
  private final HashMap<String, ModelStored> storedModels;
  private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy_HH-mm-ss");

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
            + file.substring(file.lastIndexOf('/')+1);
        var tempFilePathMeta = properties.get(ApplicationConfigs.DATAGEN_MODEL_RECEIVED_PATH) + "/"
            + file.substring(file.lastIndexOf('/')+1) + ".meta";
        try {
          var fileAsStream = Main.class.getResourceAsStream(file);
          var fileMetaAsStream = Main.class.getResourceAsStream(file+".meta");
          if(fileAsStream!=null) {
            // move model and meta file to a temporary location before being integrated
            var fos = new FileOutputStream(tempFilePath);
            fos.write(fileAsStream.readAllBytes());
            fos.close();
            fileAsStream.close();
            var fosmeta = new FileOutputStream(tempFilePathMeta);

            if(fileMetaAsStream != null) {
              fosmeta.write(fileMetaAsStream.readAllBytes());
            }
            fosmeta.close();
            fileMetaAsStream.close();

            addModel(tempFilePath, false, tempFilePathMeta);

            FileUtils.deleteLocalFile(tempFilePath);
            FileUtils.deleteLocalFile(tempFilePathMeta);
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
      if(files.length>100000) {
        log.warn("Unable to load models, because there are too much, do some manual cleaning in folder: {} and restart server",
            properties
                .get(ApplicationConfigs.DATAGEN_MODEL_STORE_PATH));
      }
      List.of(files).stream().filter(f -> f.getName().endsWith(".json")).forEach(f -> {
        try {
          var model = new JsonModelParser<>(f.getAbsolutePath()).renderModelFromFile(properties);
          var modelMeta = JsonModelMetaParser.generateModelMetaFromFile(f.getName()+".meta");
          addModel(model, false, modelMeta);
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
   * List all models remained in-memory owned by a user
   * @return
   */
  public List<ModelStored> listModelsAsModelStoredOwned(String owner) {
    return storedModels.values().stream()
        .filter(m -> m.getModelMeta()!=null && m.getModelMeta().getOwner()!=null && m.getModelMeta().getOwner().equalsIgnoreCase(owner))
        .toList();
  }

  /**
   * List all models remained in-memory authorized for a user
   * @return
   */
  public List<ModelStored> listModelsAsModelStoredAuthorized(String user) {
    return storedModels.values().stream()
        .filter(m -> {
          if(m.getModelMeta()!=null && m.getModelMeta().getOwner()!=null && m.getModelMeta().getOwner().equalsIgnoreCase(user)){
            return true;
          } else if(m.getModelMeta()!=null && m.getModelMeta().getAllowedAdminUsers()!=null && m.getModelMeta().getAllowedAdminUsers().contains(user)){
            return true;
          } else if (m.getModelMeta()!=null && m.getModelMeta().getAllowedUsers()!=null && m.getModelMeta().getAllowedUsers().contains(user)){
            return true;
          }
          return false;
        })
        .toList();
  }

  /**
   * List all models remained in-memory authorized for a user and its groups
   * @return
   */
  public List<ModelStored> listModelsAsModelStoredAuthorized(String user, Set<String> groups) {
    return storedModels.values().stream()
        .filter(m -> {
          if(m.getModelMeta().getOwner().equalsIgnoreCase(user)){
            log.debug("Model: {} authorized because it is owner of it", m.getName());
            return true;
          } else if(m.getModelMeta().getAllowedAdminUsers().contains(user)){
            log.debug("Model: {} authorized because it is admin user of it", m.getName());
            return true;
          } else if(m.getModelMeta().getAllowedUsers().contains(user)){
            log.debug("Model: {} authorized because it is user of it", m.getName());
            return true;
          } else if(!m.getModelMeta().getAllowedAdminGroups().isEmpty()) {
            var authorized = false;
            for(String g: m.getModelMeta().getAllowedAdminGroups()) {
              if(groups.contains(g)) {
                log.debug("Model: {} authorized because it is part of admin groups of it", m.getName());
                return true;
              }
              return authorized;
            }
          } else if (!m.getModelMeta().getAllowedGroups().isEmpty()){
            var authorized = false;
            for(String g: m.getModelMeta().getAllowedGroups()) {
              if(groups.contains(g)) {
                log.debug("Model: {} authorized because it is part of user groups of it", m.getName());
                return true;
              }
              return authorized;
            }
          }
          return false;
        })
        .toList();
  }

  /**
   * List all models remained in-memory with admin rights for a user and its groups
   * @return
   */
  public List<ModelStored> listModelsAsModelStoredAdministered(String user, Set<String> groups) {
    return storedModels.values().stream()
        .filter(m -> {
          if(m.getModelMeta()!=null && m.getModelMeta().getOwner()!=null && m.getModelMeta().getOwner().equalsIgnoreCase(user)){
            log.debug("Model: {} authorized because it is owner of it", m);
            return true;
          } else if(m.getModelMeta()!=null && m.getModelMeta().getAllowedAdminUsers()!=null && m.getModelMeta().getAllowedAdminUsers().contains(user)){
            log.debug("Model: {} authorized because it is admin user of it", m);
            return true;
          } else if (m.getModelMeta()!=null && m.getModelMeta().getAllowedAdminGroups()!=null){
            var authorized = false;
            for(String g: m.getModelMeta().getAllowedAdminGroups()) {
              if(groups.contains(g)) {
                log.debug("Model: {} authorized because it is part of admin groups of it", m);
                authorized = true;
                break;
              }
              return authorized;
            }
          }
          return false;
        })
        .toList();
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
   * This THE main function to add a Model, all other should delegate their calls to it at the end
   * @param model
   * @return the model
   */
  public Model addModel(Model model, boolean keepExistingModel, ModelMetaStored metaModelGiven) {
    // Check if model is not already existing
    if(storedModels.containsKey(model.getName()) && keepExistingModel) {
      log.warn("Model: {} already exists, so adding a date to uniquely identifies it", model.getName());
      model.setName(model.getName()+"-"+formatter.format(LocalDateTime.now()));
    }

    var modelFilePath = properties.get(ApplicationConfigs.DATAGEN_MODEL_STORE_PATH)+"/"+model.getName()+".json";
    var modelMetaFilePath = properties.get(ApplicationConfigs.DATAGEN_MODEL_STORE_PATH)+"/"+model.getName()+".json.meta";

    var modelMetaRetrieved = JsonModelMetaParser.generateModelMetaFromFile(modelMetaFilePath);
    // From the retrieved (potentially already existing) meta file, add if not specified all allowed users and groups
    if(metaModelGiven.getOwner().isEmpty() && !modelMetaRetrieved.getOwner().isEmpty()){
      metaModelGiven.setOwner(modelMetaRetrieved.getOwner());
    }
    if(metaModelGiven.getAllowedUsers().isEmpty()){
      metaModelGiven.setAllowedUsers(modelMetaRetrieved.getAllowedUsers());
    }
    if(metaModelGiven.getAllowedGroups().isEmpty()){
      metaModelGiven.setAllowedGroups(modelMetaRetrieved.getAllowedGroups());
    }
    if(metaModelGiven.getAllowedAdminUsers().isEmpty()){
      metaModelGiven.setAllowedAdminUsers(modelMetaRetrieved.getAllowedAdminUsers());
    }
    if(metaModelGiven.getAllowedAdminGroups().isEmpty()){
      metaModelGiven.setAllowedAdminGroups(modelMetaRetrieved.getAllowedAdminGroups());
    }

    // Write JSON representation of model & meta
    model.toJsonSchema(modelFilePath);
    log.debug("Meta Model rendered: {}",
        JsonModelMetaUnparser.renderFileFromModelMeta(metaModelGiven, modelMetaFilePath, model.getName()));


    synchronized (storedModels) {
      storedModels.put(model.getName(),
          new ModelStored(model.getName(), model, metaModelGiven, modelFilePath));
    }
    log.debug("Added successfully model: {}", model.getName());
    return model;
  }

  public Model addModel(String filepath, boolean keepExisting, String metaFilepath){
    var model = new JsonModelParser<>(filepath).renderModelFromFile(properties);
    var modelMeta = JsonModelMetaParser.generateModelMetaFromFile(metaFilepath);
    return addModel(model, keepExisting, modelMeta);
  }

  public Model addModel(Model model, boolean keepExisting) {
    return addModel(model, keepExisting, new ModelMetaStored("anonymous"));
  }

  public Model addModel(String filepath, boolean keepExisting) {
    var model = new JsonModelParser<>(filepath).renderModelFromFile(properties);
    return addModel(model, keepExisting, new ModelMetaStored("anonymous"));
  }

  public Model addModel(Model model, boolean keepExisting, String owner) {
    return addModel(model, keepExisting, new ModelMetaStored(owner));
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
  public Model addModel(InputStream json, boolean keepExisting, String owner) {
    var parser = new JsonModelParser(json);
    if (parser.getRoot() == null) {
      log.warn("Error when parsing model as input");
      return null;
    }
    var model = parser.renderModelFromFile(properties);
    return addModel(model, keepExisting, owner);
  }

  /**
   * This function assumes that model name is correctly set as it used for its file name
   */
  public void deleteModel(String modelName) {
    // Check if model exists
    if(!storedModels.containsKey(modelName)) {
      log.warn("Model: {} does not exists in-memory", modelName);
    }
    var modelFilePath = properties.get(ApplicationConfigs.DATAGEN_MODEL_STORE_PATH)+"/"+modelName+".json";
    var modelMetaFilePath = properties.get(ApplicationConfigs.DATAGEN_MODEL_STORE_PATH)+"/"+modelName+".json.meta";
    // Delete JSON representation of model
    FileUtils.deleteLocalFile(modelFilePath);
    FileUtils.deleteLocalFile(modelMetaFilePath);
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

  /**
   * Using modelMeta of a model and check admin rights using user and its set of groups
   * @param user
   * @param groups
   * @param modelName
   * @return
   */
  public boolean isUserAllowedToAdminModel(String user, Set<String> groups, String modelName) {
    var model = this.getModelAsModelStored(modelName);
    if(model!=null){
      var modelmeta = model.getModelMeta();
      if(modelmeta!=null){
        if(modelmeta.getOwner().equalsIgnoreCase(user)){
          return true;
        } else if(modelmeta.getAllowedAdminUsers().contains(user) || modelmeta.getAllowedAdminUsers().contains("public")) {
          return true;
        } else if(!modelmeta.getAllowedAdminGroups().isEmpty()) {
          for(String group: modelmeta.getAllowedAdminGroups()){
            if(groups.contains(group) || group.equalsIgnoreCase("public")){
              return true;
            }
          }
        }
      }
    }
    return false;
  }


  /**
   * Using modelMeta of a model and check admin rights using user and its set of groups
   * @param user
   * @param groups
   * @param modelName
   * @return
   */
  public boolean isUserAllowedToSeeModel(String user, Set<String> groups, String modelName) {
    var model = this.getModelAsModelStored(modelName);
    if(model!=null){
      var modelmeta = model.getModelMeta();
      if(modelmeta!=null){
        if(modelmeta.getOwner().equalsIgnoreCase(user)){
          return true;
        } else if(modelmeta.getAllowedAdminUsers().contains(user)|| modelmeta.getAllowedAdminUsers().contains("public")) {
          return true;
        } else if(modelmeta.getAllowedUsers().contains(user)|| modelmeta.getAllowedUsers().contains("public")) {
          return true;
        } else if(!modelmeta.getAllowedGroups().isEmpty()) {
          for(String group: modelmeta.getAllowedGroups()){
            if(groups.contains(group) || group.equalsIgnoreCase("public")){
              return true;
            }
          }
        } else if(!modelmeta.getAllowedAdminGroups().isEmpty()) {
          for(String group: modelmeta.getAllowedAdminGroups()){
            if(groups.contains(group) || group.equalsIgnoreCase("public")){
              return true;
            }
          }
        }
      }
    }
    return false;
  }


  public Set<String> getUsersAllowedForAModel(String modelName) {
    var hashSet = new HashSet<String>();
    var model = storedModels.get(modelName);
    if(model!=null) {
      var modelmeta = model.getModelMeta();
      if (modelmeta!=null){
        hashSet.add(modelmeta.getOwner());
        hashSet.addAll(modelmeta.getAllowedUsers());
        hashSet.addAll(modelmeta.getAllowedAdminUsers());
        hashSet.addAll(modelmeta.getAllowedGroups());
        hashSet.addAll(modelmeta.getAllowedAdminGroups());
      }
    }
    return hashSet;
  }

  public Set<String> getUsersAdminForAModel(String modelName) {
    var hashSet = new HashSet<String>();
    var model = storedModels.get(modelName);
    if(model!=null) {
      var modelmeta = model.getModelMeta();
      if (modelmeta!=null){
        hashSet.add(modelmeta.getOwner());
        hashSet.addAll(modelmeta.getAllowedAdminUsers());
        hashSet.addAll(modelmeta.getAllowedAdminGroups());
      }
    }
    return hashSet;
  }


  public boolean addUsersAllowedForModel(String modelName, String users) {
    return addUsersAllowedForModel(modelName, Set.of(users));
  }
  public boolean addUsersAllowedForModel(String modelName, Set<String> users) {
    var model = storedModels.get(modelName);
    if(model!=null) {
      var modelmeta = model.getModelMeta();
      if(modelmeta!=null) {
        modelmeta.getAllowedUsers().addAll(users);
        persistsMetadata(modelName);
        return true;
      }
    }
    return false;
  }

  public boolean addGroupsAllowedForModel(String modelName, String groups) {
    return addGroupsAllowedForModel(modelName, Set.of(groups));
  }
  public boolean addGroupsAllowedForModel(String modelName, Set<String> groups) {
    var model = storedModels.get(modelName);
    if(model!=null) {
      var modelmeta = model.getModelMeta();
      if(modelmeta!=null) {
        modelmeta.getAllowedGroups().addAll(groups);
        persistsMetadata(modelName);
        return true;
      }
    }
    return false;
  }

  public boolean addUsersAdminForModel(String modelName, String users) {
    return addUsersAdminForModel(modelName, Set.of(users));
  }
  public boolean addUsersAdminForModel(String modelName, Set<String> users) {
    var model = storedModels.get(modelName);
    if(model!=null) {
      var modelmeta = model.getModelMeta();
      if(modelmeta!=null) {
        modelmeta.getAllowedAdminUsers().addAll(users);
        persistsMetadata(modelName);
        return true;
      }
    }
    return false;
  }

  public boolean addGroupsAdminForModel(String modelName, String groups) {
    return addGroupsAdminForModel(modelName, Set.of(groups));
  }
  public boolean addGroupsAdminForModel(String modelName, Set<String> groups) {
    var model = storedModels.get(modelName);
    if(model!=null) {
      var modelmeta = model.getModelMeta();
      if(modelmeta!=null) {
        modelmeta.getAllowedAdminGroups().addAll(groups);
        persistsMetadata(modelName);
        return true;
      }
    }
    return false;
  }

  public boolean removeUsersAllowedForModel(String modelName, String groups) {
    return removeUsersAllowedForModel(modelName, Set.of(groups));
  }
  public boolean removeUsersAllowedForModel(String modelName, Set<String> groups) {
    var model = storedModels.get(modelName);
    if(model!=null) {
      var modelmeta = model.getModelMeta();
      if(modelmeta!=null) {
        modelmeta.getAllowedUsers().removeAll(groups);
        persistsMetadata(modelName);
        return true;
      }
    }
    return false;
  }

  public boolean removeGroupsAllowedForModel(String modelName, String groups) {
    return removeGroupsAllowedForModel(modelName, Set.of(groups));
  }
  public boolean removeGroupsAllowedForModel(String modelName, Set<String> groups) {
    var model = storedModels.get(modelName);
    if(model!=null) {
      var modelmeta = model.getModelMeta();
      if(modelmeta!=null) {
        modelmeta.getAllowedGroups().removeAll(groups);
        persistsMetadata(modelName);
        return true;
      }
    }
    return false;
  }


  public boolean removeUsersAdminForModel(String modelName, String groups) {
    return removeUsersAdminForModel(modelName, Set.of(groups));
  }
  public boolean removeUsersAdminForModel(String modelName, Set<String> groups) {
    var model = storedModels.get(modelName);
    if(model!=null) {
      var modelmeta = model.getModelMeta();
      if(modelmeta!=null) {
        modelmeta.getAllowedAdminUsers().removeAll(groups);
        persistsMetadata(modelName);
        return true;
      }
    }
    return false;
  }

  public boolean removeGroupsAdminForModel(String modelName, String groups) {
    return removeGroupsAdminForModel(modelName, Set.of(groups));
  }
  public boolean removeGroupsAdminForModel(String modelName, Set<String> groups) {
    var model = storedModels.get(modelName);
    if(model!=null) {
      var modelmeta = model.getModelMeta();
      if(modelmeta!=null) {
        modelmeta.getAllowedAdminGroups().removeAll(groups);
        persistsMetadata(modelName);
        return true;
      }
    }
    return false;
  }

  /**
   *  For a given model, replace totally its metadata
   * @param modelName
   * @param groups
   * @param users
   * @param adminGroups
   * @param adminUsers
   * @return
   */
  public boolean changeModelMeta(String modelName, Set<String> users, Set<String> groups,
                                 Set<String> adminUsers, Set<String> adminGroups) {
    var model = storedModels.get(modelName);
    if(model!=null) {
      var modelmeta = model.getModelMeta();
      if(modelmeta!=null) {
        modelmeta.setAllowedUsers(users);
        modelmeta.setAllowedGroups(groups);
        modelmeta.setAllowedAdminUsers(adminUsers);
        modelmeta.setAllowedAdminGroups(adminGroups);
      }
      persistsMetadata(modelName);
      return true;
    }
    return false;
  }

  /**
   * For a given model, replace totally its metadata by giving all
   * @param modelName
   * @param newModelMeta
   * @return
   */
  public boolean changeModelMeta(String modelName, ModelMetaStored newModelMeta) {
    var model = storedModels.get(modelName);
    if(model!=null) {
      model.setModelMeta(newModelMeta);
      persistsMetadata(modelName);
      return true;
    }
    return false;
  }

  /**
   * For a given model, persist its metadata
   * Should be called each time there is a change on metadata
   * @param modelName
   */
  public void persistsMetadata(String modelName) {
    var modelMetaFilePath = properties.get(ApplicationConfigs.DATAGEN_MODEL_STORE_PATH)+"/"+modelName+".json.meta";
    JsonModelMetaUnparser.renderFileFromModelMeta(storedModels.get(modelName).getModelMeta(), modelMetaFilePath, modelName);
  }


}
