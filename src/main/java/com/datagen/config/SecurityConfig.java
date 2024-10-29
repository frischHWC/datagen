/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datagen.config;


import com.datagen.service.users.UsersService;
import com.datagen.views.login.LoginView;
import com.vaadin.flow.spring.security.VaadinWebSecurity;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.ldap.core.AttributesMapper;
import org.springframework.ldap.core.ContextSource;
import org.springframework.ldap.core.DirContextOperations;
import org.springframework.ldap.filter.EqualsFilter;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.builders.WebSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.ldap.DefaultSpringSecurityContextSource;
import org.springframework.security.ldap.userdetails.DefaultLdapAuthoritiesPopulator;
import org.springframework.security.ldap.userdetails.NestedLdapAuthoritiesPopulator;

import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchControls;
import java.util.*;

/**
 * This security class forces https and basic authentication with only one possible user able to do requests
 */
@Slf4j
@Configuration
@EnableWebSecurity
public class SecurityConfig extends VaadinWebSecurity {

  private final PropertiesLoader propertiesLoader;
  private final Map<ApplicationConfigs, String> props;
  private final UsersService usersService;

  @Autowired
  public SecurityConfig(PropertiesLoader propertiesLoader, UsersService usersService) {
    this.usersService = usersService;
    this.propertiesLoader = propertiesLoader;
    this.props = propertiesLoader.getPropertiesCopy();
  }

  @Override
  protected void configure(HttpSecurity http) throws Exception {
    // Disable JWT auth and use basic auth instead + CSRF disabled for api endpoints
    // Disable auth for metrics & status endpoints
    http
        .authorizeHttpRequests(auth ->
        auth.requestMatchers("/api/v1/metrics/**", "/api/v1/health/status").permitAll())
        .authorizeHttpRequests(authorize -> authorize.requestMatchers("/api/v1/**").hasAnyRole("DATAGEN_ADMIN"))
        .httpBasic(Customizer.withDefaults())
        .csrf((csrf) -> csrf.ignoringRequestMatchers("/api/v1/**"));
    ;
    // Let Vaadin secure rest of application
    super.configure(http);

    setLoginView(http, LoginView.class);
  }

  @Override
  public void configure(WebSecurity web) throws Exception {
    super.configure(web);
  }


  /**
   * Configure the authentication provider (either ldap, ldap-embedded or file)
   * @param auth
   * @throws Exception
   */
  @Autowired
  public void configure(AuthenticationManagerBuilder auth) throws Exception {
    var authtype = props.get(ApplicationConfigs.DATAGEN_AUTH_TYPE);

    if(authtype.equalsIgnoreCase("ldap")) {
      log.info("Using LDAP authentication");
      configureLDAP(auth);

    } else if(authtype.equalsIgnoreCase("ldap-embedded")) {
      log.info("Using Embedded LDAP authentication");
      // log in with: francois/Cloudera1234 for ADMIN role or with auguste/Cloudera1234 for USER role
      configureLdapEmbedded(auth, this.usersService.getPasswordEncoder());
    } else if (authtype.equalsIgnoreCase("internal")) {
      log.info("Using user/password authentication");
      // Login as admin user defined in properties file
      configureInternalUsers(auth, this.usersService.getPasswordEncoder());
    }

  }

  /**
   * Configure authentication using internal users, backed by a file and a default admin user defined in properties file
   * @param auth
   * @param passwordEncoder
   */
  private void configureInternalUsers(AuthenticationManagerBuilder auth, PasswordEncoder passwordEncoder) {
    try {
      auth.userDetailsService(this.usersService.getInMemoryUserDetailsManager())
          .passwordEncoder(passwordEncoder);
    } catch (Exception e) {
      log.error("Could not configure authentication to internal users (backed by a file) due to: ", e);
    }
  }


  /**
   * Configure auth to use a LDAP embedded server
   * @param auth
   * @param passwordEncoder
   */
  private void configureLdapEmbedded(AuthenticationManagerBuilder auth, PasswordEncoder passwordEncoder) {
    var contextSource = new DefaultSpringSecurityContextSource(
        List.of("ldap://localhost:8389/"), "dc=springframework,dc=org");
    contextSource.setUserDn("uid=admin,ou=people,dc=springframework,dc=org");
    contextSource.setPassword("password");
    contextSource.afterPropertiesSet();

    DefaultLdapAuthoritiesPopulator authorities =
        new DefaultLdapAuthoritiesPopulator(contextSource, "ou=groups");
    authorities.setGroupSearchFilter("member={0}");
    authorities.setDefaultRole("ADMIN");

    try {
      auth
          .ldapAuthentication()
          .userDnPatterns("uid={0},ou=people")
          .ldapAuthoritiesPopulator(authorities)
          .contextSource(contextSource)
          .passwordCompare()
          .passwordEncoder(passwordEncoder)
          .passwordAttribute("userPassword")
      ;
    } catch (Exception e) {
      log.error("Could not configure authentication to an embedded LDAP server due to: ", e);
    }
  }

  /**
   * Configure authentication toward an external LDAP server configured in properties file
   * @param auth
   */
  private void configureLDAP(AuthenticationManagerBuilder auth) {
    // Auth of bind user
    var contextSource = new DefaultSpringSecurityContextSource(
        List.of(props.get(ApplicationConfigs.DATAGEN_AUTH_LDAP_URL)),
        props.get(ApplicationConfigs.DATAGEN_AUTH_LDAP_BASEDN));
    contextSource.setUserDn(props.get(ApplicationConfigs.DATAGEN_AUTH_LDAP_BIND_USER));
    contextSource.setPassword(props.get(ApplicationConfigs.DATAGEN_AUTH_LDAP_BIND_PASSWORD));
    contextSource.afterPropertiesSet();

    // Using custom authorizer defined below, it is the functions making mapping of users and its groups to roles (USER or ADMIN)
    List<String> groupUsersList = props.get(ApplicationConfigs.DATAGEN_AUTH_LDAP_GROUP_USERS)==null?Collections.emptyList():
        Arrays.stream(props.get(ApplicationConfigs.DATAGEN_AUTH_LDAP_GROUP_USERS).split(",")).toList();
    List<String> userUsersList = props.get(ApplicationConfigs.DATAGEN_AUTH_LDAP_USER_USERS)==null?Collections.emptyList():
        Arrays.stream(props.get(ApplicationConfigs.DATAGEN_AUTH_LDAP_USER_USERS).split(",")).toList();
    List<String> groupAdminsList = props.get(ApplicationConfigs.DATAGEN_AUTH_LDAP_GROUP_ADMINS)==null?Collections.emptyList():
        Arrays.stream(props.get(ApplicationConfigs.DATAGEN_AUTH_LDAP_GROUP_ADMINS).split(",")).toList();
    List<String> userAdminsList = props.get(ApplicationConfigs.DATAGEN_AUTH_LDAP_USER_ADMINS)==null?Collections.emptyList():
        Arrays.stream(props.get(ApplicationConfigs.DATAGEN_AUTH_LDAP_USER_ADMINS).split(",")).toList();

    var authorities =
        new CustomNestedLdapAuthoritiesPopulator(contextSource,
            Integer.valueOf(props.get(ApplicationConfigs.DATAGEN_AUTH_LDAP_GROUP_REVERSE_SEARCH_TIMEOUT)),
            props.get(ApplicationConfigs.DATAGEN_AUTH_LDAP_BASEDN),
            props.get(ApplicationConfigs.DATAGEN_AUTH_LDAP_GROUP_BASE),
            props.get(ApplicationConfigs.DATAGEN_AUTH_LDAP_GROUP_ATTRIBUTE),
            props.get(ApplicationConfigs.DATAGEN_AUTH_LDAP_GROUP_FILTER),
            props.get(ApplicationConfigs.DATAGEN_AUTH_LDAP_GROUP_ROLE_ATTRIBUTE),
            Boolean.valueOf(props.get(ApplicationConfigs.DATAGEN_AUTH_LDAP_GROUP_SEARCH_SUBTREE)),
            props.get(ApplicationConfigs.DATAGEN_AUTH_LDAP_GROUP_ROLE_ATTRIBUTE),
            Boolean.valueOf(props.get(ApplicationConfigs.DATAGEN_AUTH_LDAP_GROUP_CONVERT_UPPERCASE)),
            Integer.valueOf(props.get(ApplicationConfigs.DATAGEN_AUTH_LDAP_GROUP_SEARCH_MAXDEPTH)),
            props.get(ApplicationConfigs.DATAGEN_AUTH_LDAP_USER_ATTRIBUTE),
            props.get(ApplicationConfigs.DATAGEN_AUTH_LDAP_GROUP_USER_ATTRIBUTE),
            Boolean.valueOf(props.get(ApplicationConfigs.DATAGEN_AUTH_LDAP_GROUP_REVERSE_SEARCH)),
            groupAdminsList, userAdminsList, groupUsersList, userUsersList
            );

    try {
      auth
          .ldapAuthentication()
          .userSearchBase(
              props.get(ApplicationConfigs.DATAGEN_AUTH_LDAP_USER_BASE))
          .userSearchFilter(
              props.get(ApplicationConfigs.DATAGEN_AUTH_LDAP_USER_FILTER))
          .ldapAuthoritiesPopulator(authorities)
          .contextSource(contextSource);
    } catch (Exception e) {
      log.error("Could not configure authentication to an external LDAP server due to: ", e);
    }
  }

  /**
   * Custom Nested LDAP Authorities Populator
   * Goal of this class is given a user and its group, and based on users/groups that should be considered as user or admin,
   * it provides the required role (i.e. ROLE_DATAGEN_USER & ROLE_DATAGEN_ADMIN)
   * It also returns thr groups for a user in grantedAuthority
   */
  private static class CustomNestedLdapAuthoritiesPopulator extends NestedLdapAuthoritiesPopulator {

    private String baseDn;
    private String groupAttribute;
    private String userAttribute;
    private String groupUserAttribute;
    private String groupNameAttribute;
    private Boolean reverseSearch;
    private List<String> groupAdmins;
    private List<String> userAdmins;
    private List<String> groupUsers;
    private List<String> userUsers;

    public CustomNestedLdapAuthoritiesPopulator(
        ContextSource contextSource,
        Integer timeout,
        String baseDn,
        String groupSearchBase,
        String groupAttribute,
        String groupFilter,
        String groupNameAttribute,
        Boolean searchSubTree,
        String groupRoleAttribute,
        Boolean convertToUpperCase,
        Integer maxSearchDepth,
        String userAttribute,
        String groupUserAttribute,
        Boolean reverseSearch,
        List<String> groupAdmins,
        List<String> userAdmins,
        List<String> groupUsers,
        List<String> userUsers) {

      super(contextSource, groupSearchBase);
      this.baseDn = baseDn;
      this.groupAttribute = groupAttribute;
      this.userAttribute = userAttribute;
      this.groupUserAttribute = groupUserAttribute;
      this.groupNameAttribute = groupNameAttribute;
      this.reverseSearch = reverseSearch;
      this.setGroupSearchFilter(groupFilter);
      this.setSearchSubtree(searchSubTree);
      this.setGroupRoleAttribute(groupRoleAttribute);
      this.setConvertToUpperCase(convertToUpperCase);
      this.setMaxSearchDepth(maxSearchDepth);
      this.setRolePrefix("");

      this.groupAdmins = groupAdmins;
      this.userAdmins = userAdmins;
      this.groupUsers = groupUsers;
      this.userUsers = userUsers;
      // If no users are specified, everyone should be granted access to datagen UI
      if(groupUsers.isEmpty() && userUsers.isEmpty()) {
        this.setDefaultRole("ROLE_DATAGEN_USER");
      }
      this.getLdapTemplate().setDefaultTimeLimit(timeout);
    }

    @Override
    protected Set<GrantedAuthority> getAdditionalRoles(DirContextOperations userData, String username) {
      Set<GrantedAuthority> authorities = new HashSet<>();

      var groups = userData.getStringAttributes(groupAttribute);
      var userName = userData.getStringAttributes(userAttribute)[0];
      var userDn = userData.getDn().toString();


      // Check groups
      if (groups != null) {
        for (String group : groups) {
          // Extract group name
          var exactGroupName = group;
          try {
            exactGroupName = group.split(",")[0].split("=")[1];
          } catch (Exception e) {
            log.debug("Cannot extract group exact name from group: {}", group);
          }
          log.debug("Checking group: {}", exactGroupName);
          if(groupAdmins.contains(exactGroupName)){
            log.debug("Set user: {} as ROLE_DATAGEN_ADMIN because it has group: {}", userName, group);
            authorities.add(new SimpleGrantedAuthority("ROLE_DATAGEN_ADMIN"));
          }
          if(groupUsers.contains(exactGroupName)){
            log.debug("Set user: {} as ROLE_DATAGEN_USER because it has group: {}", userName, group);
            authorities.add(new SimpleGrantedAuthority("ROLE_DATAGEN_USER"));
          }
          authorities.add(new SimpleGrantedAuthority(exactGroupName));
        }
      }
      // Check user
      log.debug("Checking user: {}", userName);
      if(userAdmins.contains(userName)){
        log.debug("Set user: {} as ROLE_DATAGEN_ADMIN because of its name", userName);
        authorities.add(new SimpleGrantedAuthority("ROLE_DATAGEN_ADMIN"));
      }
      if(userUsers.contains(userName)){
        log.debug("Set user: {} as ROLE_DATAGEN_USER because of its name", userName);
        authorities.add(new SimpleGrantedAuthority("ROLE_DATAGEN_USER"));
      }

      // Reverse search group
      if(reverseSearch) {
        SearchControls searchControls = new SearchControls();
        searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
        EqualsFilter filter = new EqualsFilter(this.groupUserAttribute, userDn+","+this.baseDn);

        log.debug("Launching reverse search on group base: {} with filter: {}", this.getGroupSearchBase(), filter.encode());

        List<String> reverseGroups = this.getLdapTemplate()
            .search(this.getGroupSearchBase(), filter.encode(), searchControls,
                new AttributeMapper(authorities, this.groupNameAttribute));
        for(String group: reverseGroups) {
          log.debug("Checking group from reversed search: {}", group);
          if(groupAdmins.contains(group)){
            log.debug("Set user: {} as ROLE_DATAGEN_ADMIN because it has group: {}", userName, group);
            authorities.add(new SimpleGrantedAuthority("ROLE_DATAGEN_ADMIN"));
          }
          if(groupUsers.contains(group)){
            log.debug("Set user: {} as ROLE_DATAGEN_USER because it has group: {}", userName, group);
            authorities.add(new SimpleGrantedAuthority("ROLE_DATAGEN_USER"));
          }
          authorities.add(new SimpleGrantedAuthority(group));
        }
      }

      // Add user to internal user service just for display


      return authorities;
    }

    /**
     * Internal class to get 'cn' attribute (or defined group name attribute in properties file) from a search made on groups
     */
    private class AttributeMapper implements AttributesMapper<String> {
      private Set<GrantedAuthority> authorities;
      private String groupNameAttribute;
      public AttributeMapper(Set<GrantedAuthority> authorities, String groupNameAttribute) {
        super();
        this.authorities = authorities;
        this.groupNameAttribute = groupNameAttribute;
      }
      @Override
      public String mapFromAttributes(Attributes attributes) {
        try {
          return attributes.get(groupNameAttribute).get().toString();
        } catch (NamingException e) {
          log.warn("Could not find: {} for group", groupNameAttribute);
        }
        return "";
      }

    }

  }



}
