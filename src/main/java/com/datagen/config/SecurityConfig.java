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


import com.datagen.views.login.LoginView;
import com.vaadin.flow.spring.security.VaadinWebSecurity;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.builders.WebSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;

/**
 * This security class forces https and basic authentication with only one possible user able to do requests
 */
@Slf4j
@Configuration
@EnableWebSecurity
public class SecurityConfig extends VaadinWebSecurity {


  private final PropertiesLoader propertiesLoader;

  @Autowired
  public SecurityConfig(PropertiesLoader propertiesLoader) {
    this.propertiesLoader = propertiesLoader;
  }

  @Override
  protected void configure(HttpSecurity http) throws Exception {
    // Disable JWT auth and use basic auth instead + CSRF disabled for api endpoints
    // Disable auth for metrics & status endpoints
    http
        .authorizeHttpRequests(auth ->
        auth.requestMatchers("/api/v1/metrics/**", "/api/v1/health/status").permitAll())
        .authorizeHttpRequests(authorize -> authorize.requestMatchers("/api/v1/**").authenticated())
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


  /**@Bean
  public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
    return http
        .authorizeRequests(
            authorize -> authorize.requestMatchers("/api/v1/metrics/**").permitAll())
        .authorizeRequests(
            authorize -> authorize.requestMatchers("/api/v1/health/status").permitAll())
        .authorizeRequests(authorize -> authorize.requestMatchers("/api/v1/**").authenticated())
        .httpBasic(withDefaults())
        .csrf(AbstractHttpConfigurer::disable)
        .build();
  }**/

  @Bean
  public UserDetailsService users() {
    // The builder will ensure the passwords are encoded before saving in memory
    User.UserBuilder users = User.builder();
    UserDetails admin = users
        .username(propertiesLoader.getPropertiesCopy()
            .get(ApplicationConfigs.ADMIN_USER))
        .password(passwordEncoder().encode(propertiesLoader.getPropertiesCopy()
            .get(ApplicationConfigs.ADMIN_PASSWORD)))
        .roles("ADMIN")
        .build();
    return new InMemoryUserDetailsManager(admin);
  }

  @Bean
  public PasswordEncoder passwordEncoder() {
    return new BCryptPasswordEncoder();
  }

  // TODO: Add LDAP connection and users from it
  /*@Autowired
  public void configure(AuthenticationManagerBuilder auth) throws Exception {
    auth
        .ldapAuthentication()
        .userDnPatterns("uid={0},ou=people")
        .groupSearchBase("ou=groups")
        .contextSource()
        .url("ldap://localhost:8389/dc=springframework,dc=org")
        .and()
        .configure(new LdapBindAuthenticationManagerFactory(
            new DefaultSpringSecurityContextSource("ldap://localhost:53389/dc=springframework,dc=org"))
            .createAuthenticationManager());
  } */

}
