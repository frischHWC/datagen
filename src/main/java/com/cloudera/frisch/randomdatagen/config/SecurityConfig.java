package com.cloudera.frisch.randomdatagen.config;


import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;

import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;

import static org.springframework.security.config.Customizer.withDefaults;

/**
 * This security class forces https and basic authentication with only one possible user able to do requests
 */
@Slf4j
@Configuration
@EnableWebSecurity
public class SecurityConfig {


  private final PropertiesLoader propertiesLoader;

  @Autowired
  public SecurityConfig(PropertiesLoader propertiesLoader) {
    this.propertiesLoader = propertiesLoader;
  }

  @Bean
  public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
    return http
          .requiresChannel(channel -> channel.anyRequest().requiresSecure())
          .authorizeRequests(authorize -> authorize.anyRequest().authenticated())
          .httpBasic(withDefaults())
          .csrf().disable()
          .build();
  }

  @Bean
  public UserDetailsService users() {
    // The builder will ensure the passwords are encoded before saving in memory
    User.UserBuilder users = User.builder();
    UserDetails admin = users
        .username(propertiesLoader.getPropertiesCopy().get(ApplicationConfigs.ADMIN_USER))
        .password(passwordEncoder().encode(propertiesLoader.getPropertiesCopy().get(ApplicationConfigs.ADMIN_PASSWORD)))
        .roles("ADMIN")
        .build();
    return new InMemoryUserDetailsManager(admin);
  }

  @Bean
  public PasswordEncoder passwordEncoder() {
    return new BCryptPasswordEncoder();
  }

}
