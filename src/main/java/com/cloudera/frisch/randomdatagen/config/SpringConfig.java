package com.cloudera.frisch.randomdatagen.config;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;


@Getter
@Configuration
public class SpringConfig {

  @Value("${spring.profiles.active:dev}")
  public String activeProfile;

}
