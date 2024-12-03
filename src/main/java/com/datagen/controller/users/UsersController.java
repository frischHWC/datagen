package com.datagen.controller.users;

import com.datagen.service.users.UsersService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Set;

@Slf4j
@RestController
@RequestMapping("/api/v1/users")
public class UsersController {

  @Autowired
  private UsersService usersService;

  @GetMapping(value = "/list", produces = {
      MediaType.APPLICATION_JSON_VALUE})
  public List<UsersService.UserDatagen> getUsers() {
    return usersService.getUsers().values().stream().map(u -> {u.setPasswordEncoded(""); return u;}).toList();
  }

  @PostMapping(value = "/add")
  public boolean addUser(
      @RequestParam String username,
      @RequestParam String password,
      @RequestParam Set<String> groups
  ) {
    return usersService.addUser(username, groups, password);
  }

  @PostMapping(value = "/add_groups")
  public boolean addGroups(
      @RequestParam String username,
      @RequestParam Set<String> groups
  ) {
    return usersService.setGroupsToUser(username, groups);
  }

  @PostMapping(value = "/update_password")
  public boolean updatePassword(
      @RequestParam String username,
      @RequestParam String password
  ) {
    return usersService.updatePasswordOfUser(username, password);
  }

  @DeleteMapping(value = "/delete")
  public boolean delete(
      @RequestParam String username
  ) {
    return usersService.deleteUser(username);
  }


}
