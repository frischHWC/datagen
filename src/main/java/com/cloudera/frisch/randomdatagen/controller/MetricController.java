package com.cloudera.frisch.randomdatagen.controller;


import com.cloudera.frisch.randomdatagen.service.MetricsService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;


@Slf4j
@RestController
@RequestMapping("/metrics")
public class MetricController {

  @Autowired
  private MetricsService metricsService;

  @GetMapping(value = "/all")
  @ResponseBody
  public String getAllMetrics() {
    return metricsService.getMetricsAsAJson();
  }

}
