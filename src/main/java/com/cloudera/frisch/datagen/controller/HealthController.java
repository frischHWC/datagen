package com.cloudera.frisch.datagen.controller;


import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;


@Slf4j
@RestController
@RequestMapping("/health")
public class HealthController {

  @GetMapping(value = "/status")
  @ResponseBody
  public String getStatus() {
    return "{ \"status\": \"OK\" }";
  }

}
