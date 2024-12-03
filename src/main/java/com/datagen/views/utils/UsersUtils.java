package com.datagen.views.utils;

import com.vaadin.flow.spring.security.AuthenticationContext;
import org.springframework.security.core.GrantedAuthority;

import java.util.Set;
import java.util.stream.Collectors;

public class UsersUtils {

  public static String getUser(AuthenticationContext authContext) {
    return authContext.getPrincipalName().orElseGet(() -> "anonymous");
  }

  public static Set<String> getUserGroups(AuthenticationContext authContext) {
    return authContext.getGrantedAuthorities().stream()
        .map(GrantedAuthority::getAuthority)
        .filter(r -> !r.startsWith("ROLE_"))
        .collect(Collectors.toSet());
  }

  public static Set<String> getUserRoles(AuthenticationContext authContext) {
    return authContext.getGrantedAuthorities().stream()
        .map(GrantedAuthority::getAuthority)
        .filter(r -> r.startsWith("ROLE_"))
        .collect(Collectors.toSet());
  }

  public static boolean isUserDatagenAdmin(AuthenticationContext authenticationContext) {
    return getUserRoles(authenticationContext).contains("ROLE_DATAGEN_ADMIN");
  }

  public static boolean isUserDatagenUser(AuthenticationContext authenticationContext) {
    return getUserRoles(authenticationContext).contains("ROLE_DATAGEN_USER");
  }
}
