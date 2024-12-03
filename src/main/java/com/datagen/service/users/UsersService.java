package com.datagen.service.users;

import com.datagen.config.ApplicationConfigs;
import com.datagen.config.PropertiesLoader;
import com.datagen.connector.storage.utils.FileUtils;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class UsersService {

  // User file is like this: username;group1,group2;passwordEncoded

  private final PropertiesLoader propertiesLoader;
  private final Map<ApplicationConfigs, String> props;
  @Getter
  private final InMemoryUserDetailsManager inMemoryUserDetailsManager;
  @Getter
  private final PasswordEncoder passwordEncoder;
  // To retain all users for making read/write to a file
  @Getter
  private final Map<String, UserDatagen> users;

  private final Set<String> adminUsers;
  private final Set<String> adminGroups;

  @Getter
  @Setter
  @AllArgsConstructor
  public class UserDatagen {
    String username;
    Set<String> groups;
    String passwordEncoded;
    public UserDatagen(String username){
      this.username = username;
      this.groups = new HashSet<>();
    }
  }


  public UsersService(PropertiesLoader propertiesLoader){
    this.propertiesLoader = propertiesLoader;
    this.props = propertiesLoader.getPropertiesCopy();
    this.passwordEncoder = new BCryptPasswordEncoder();
    this.users = new HashMap<>();
    this.adminUsers = this.props.get(ApplicationConfigs.DATAGEN_AUTH_INTERNAL_USER_ADMINS)==null?Collections.emptySet():
        Arrays.stream(this.props.get(ApplicationConfigs.DATAGEN_AUTH_INTERNAL_USER_ADMINS).split(",")).collect(Collectors.toSet());
    this.adminGroups = this.props.get(ApplicationConfigs.DATAGEN_AUTH_INTERNAL_GROUP_ADMINS)==null?Collections.emptySet():
        Arrays.stream(this.props.get(ApplicationConfigs.DATAGEN_AUTH_INTERNAL_GROUP_ADMINS).split(",")).collect(Collectors.toSet());

    // Init Admin user first
    UserDetails admin = User.builder()
        .username(props.get(ApplicationConfigs.DATAGEN_ADMIN_USER))
        .password(passwordEncoder.encode(props.get(ApplicationConfigs.DATAGEN_ADMIN_PASSWORD)))
        .roles("DATAGEN_ADMIN")
        .build();

    this.inMemoryUserDetailsManager = new InMemoryUserDetailsManager(admin);

    if(this.props.get(ApplicationConfigs.DATAGEN_AUTH_TYPE).equalsIgnoreCase("internal")) {
      // In this case, we need to initiate the internal user service made for such case
      var userFilePath = this.props.get(ApplicationConfigs.DATAGEN_USERS_FILE_PATH);
      if(!FileUtils.checkLocalFileExists(userFilePath)) {
        FileUtils.createLocalFile(userFilePath);
      } else {
        var usersFromFile = FileUtils.getfileContent(userFilePath);
        for(String userLine: usersFromFile){
          var ud = createUserFromString(userLine);
          this.users.put(ud.getUsername(), ud);
          this.inMemoryUserDetailsManager.createUser(createUserDetailsFromUserDatagen(ud));
        }
      }
    }
  }

  /**
   * Create a user from a line (stored in a file)
   * @param userLine
   * @return
   */
  public UserDatagen createUserFromString(String userLine) {
    var userSplitted = userLine.split(";");
    var user = new UserDatagen(userSplitted[0]);
    var groups = Arrays.stream(userSplitted[1].split(",")).collect(
        Collectors.toSet());
    user.setGroups(groups);
    user.setPasswordEncoded(userSplitted[2]);
    return user;
  }

  public String createStringFromUserDatagen(UserDatagen userDatagen){
    var sb = new StringBuilder();
    sb.append(userDatagen.getUsername());
    sb.append(";");
    for(String group: userDatagen.getGroups()) {
      sb.append(group);
      sb.append(",");
    }
    sb.deleteCharAt(sb.lastIndexOf(","));
    sb.append(";");
    sb.append(userDatagen.getPasswordEncoded());
    return sb.toString();
  }

  /**
   * For internal users, basic definition of user details does not contain concept of "group", so here it is using GrantedAuthority to put groups in it
   * @param userDatagen
   * @return
   */
  public UserDetails createUserDetailsFromUserDatagen(UserDatagen userDatagen) {
    var ub = User.builder()
        .username(userDatagen.getUsername())
        .password(userDatagen.getPasswordEncoded());

    var grantedAuthorityList = new ArrayList<GrantedAuthority>();

    var roleToAdd = "ROLE_DATAGEN_USER";

    if(this.adminUsers.contains(userDatagen.getUsername())) {
      roleToAdd = "ROLE_DATAGEN_ADMIN";
    }

    for(String group: userDatagen.getGroups()){
      if(this.adminGroups.contains(group)) {
        roleToAdd = "ROLE_DATAGEN_ADMIN";
      }
      if(!group.equalsIgnoreCase("ROLE_DATAGEN_ADMIN") && !group.isBlank()) {
        grantedAuthorityList.add(new SimpleGrantedAuthority(group));
      }
    }
    grantedAuthorityList.add(new SimpleGrantedAuthority(roleToAdd));

    ub.authorities(grantedAuthorityList);

    return ub.build();
  }

  public boolean addUser(String username, Set<String> groups, String passwordNotEnc) {
    var ud = new UserDatagen(username, groups, this.passwordEncoder.encode(passwordNotEnc));
    users.put(username, ud);
    if(this.props.get(ApplicationConfigs.DATAGEN_AUTH_TYPE).equalsIgnoreCase("internal")) {
      this.inMemoryUserDetailsManager.createUser(
          createUserDetailsFromUserDatagen(ud));
      return flushUsers();
    }
    return true;
  }

  public boolean deleteUser(String username) {
    users.remove(username);
    if(this.props.get(ApplicationConfigs.DATAGEN_AUTH_TYPE).equalsIgnoreCase("internal")) {
      this.inMemoryUserDetailsManager.deleteUser(username);
      return flushUsers();
    }
    return true;
  }

  public UserDatagen getUser(String username) {
    return users.get(username);
  }

  public List<UserDatagen> listUsers() {
    return new ArrayList<>(users.values());
  }

  public boolean isUserExisting(String username) {
    return users.containsKey(username);
  }

  public boolean setGroupsToUser(String username, Set<String> groups) {
    var ud = users.get(username);
    ud.setGroups(groups);
    this.inMemoryUserDetailsManager.updateUser(createUserDetailsFromUserDatagen(ud));
    return flushUsers();
  }

  public boolean updatePasswordOfUser(String username, String passwordNotEnc) {
    var ud = users.get(username);
    var passwordEnc = this.passwordEncoder.encode(passwordNotEnc);
    this.inMemoryUserDetailsManager.updatePassword(createUserDetailsFromUserDatagen(ud), passwordEnc);
    ud.setPasswordEncoded(passwordEnc);
    return flushUsers();
  }



  /**
   * Flush users in-memory to the user file (should be called each time there are changes on users)
   * @return
   */
  public synchronized boolean flushUsers() {
    var userList = this.users.values().stream().map(this::createStringFromUserDatagen).toList();
    FileUtils.moveLocalFile(this.props.get(ApplicationConfigs.DATAGEN_USERS_FILE_PATH), this.props.get(ApplicationConfigs.DATAGEN_USERS_FILE_PATH)+"_backup");
    var fos = FileUtils.createLocalFileAsOutputStream(this.props.get(ApplicationConfigs.DATAGEN_USERS_FILE_PATH));
    try {
      for(String u: userList) {
          fos.write(u.getBytes());
          fos.write(System.lineSeparator().getBytes());
      }
      fos.close();
      FileUtils.deleteLocalFile(this.props.get(ApplicationConfigs.DATAGEN_USERS_FILE_PATH)+"_backup");
      return true;
    } catch (IOException e) {
      log.warn("Cannot write users to file with error: ", e);
    }
    return false;
  }

  public boolean isUserFromInternal() {
    return this.props.get(ApplicationConfigs.DATAGEN_AUTH_TYPE).equalsIgnoreCase("internal");
  }
}
