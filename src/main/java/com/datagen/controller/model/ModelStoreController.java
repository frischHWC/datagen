package com.datagen.controller.model;

import com.datagen.service.model.ModelStoreService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.Set;

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

  @GetMapping(value = "/list/owner")
  public String listModelOwner(
      @RequestParam(required = false, name = "model") String modelName
  ) {
    return modelStoreService.getModelAsModelStored(modelName).getOwner();
  }

  @GetMapping(value = "/list/users")
  public Set<String> listModelUser(
      @RequestParam(required = false, name = "model") String modelName
  ) {
    return modelStoreService.getModelAsModelStored(modelName).getModelMeta().getAllowedUsers();
  }

  @GetMapping(value = "/list/groups")
  public Set<String> listModelGroup(
      @RequestParam(required = false, name = "model") String modelName
  ) {
    return modelStoreService.getModelAsModelStored(modelName).getModelMeta().getAllowedGroups();
  }

  @GetMapping(value = "/list/admin_users")
  public Set<String> listModelAdminUser(
      @RequestParam(required = false, name = "model") String modelName
  ) {
    return modelStoreService.getModelAsModelStored(modelName).getModelMeta().getAllowedAdminUsers();
  }

  @GetMapping(value = "/list/admin_groups")
  public Set<String> listModelAdminGroup(
      @RequestParam(required = false, name = "model") String modelName
  ) {
    return modelStoreService.getModelAsModelStored(modelName).getModelMeta().getAllowedAdminGroups();
  }

  @PostMapping(value = "/add/users")
  public boolean addModelUser(
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "user") String user
  ) {
    log.debug("Received request to add user to model");
    return modelStoreService.addUsersAllowedForModel(modelName, user);
  }

  @PostMapping(value = "/add/groups")
  public boolean addModelGroup(
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "group") String group
  ) {
    log.debug("Received request to add group to model");
    return modelStoreService.addGroupsAllowedForModel(modelName, group);
  }

  @PostMapping(value = "/add/admin_users")
  public boolean addModelAdminUser(
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "user") String user
  ) {
    log.debug("Received request to add admin user to model");
    return modelStoreService.addUsersAdminForModel(modelName, user);
  }

  @PostMapping(value = "/add/admin_groups")
  public boolean addModelAdminGroup(
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "group") String group
  ) {
    log.debug("Received request to add admin group to model");
    return modelStoreService.addGroupsAdminForModel(modelName, group);
  }

  @DeleteMapping(value = "/delete/users")
  public boolean deleteModelUser(
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "user") String user
  ) {
    log.debug("Received request to delete user to model");
    return modelStoreService.removeUsersAllowedForModel(modelName, user);
  }

  @DeleteMapping(value = "/delete/groups")
  public boolean deleteModelGroup(
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "group") String group
  ) {
    log.debug("Received request to delete group to model");
    return modelStoreService.removeGroupsAllowedForModel(modelName, group);
  }

  @DeleteMapping(value = "/delete/admin_users")
  public boolean deleteModelAdminUser(
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "user") String user
  ) {
    log.debug("Received request to delete admin user to model");
    return modelStoreService.removeUsersAdminForModel(modelName, user);
  }

  @DeleteMapping(value = "/delete/admin_groups")
  public boolean deleteModelAdminGroup(
      @RequestParam(required = false, name = "model") String modelName,
      @RequestParam(required = false, name = "group") String group
  ) {
    log.debug("Received request to delete admin group to model");
    return modelStoreService.removeGroupsAdminForModel(modelName, group);
  }

}
