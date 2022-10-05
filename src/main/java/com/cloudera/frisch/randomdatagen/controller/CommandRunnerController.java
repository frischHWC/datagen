package com.cloudera.frisch.randomdatagen.controller;


import com.cloudera.frisch.randomdatagen.service.Command;
import com.cloudera.frisch.randomdatagen.service.CommandRunnerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/command")
public class CommandRunnerController {

  @Autowired
  private CommandRunnerService commandRunnerService;

  @PostMapping(value = "/getCommandStatus")
  public String getStatusOfACommand(
      @RequestParam(name = "commandUuid") UUID commandUUID
  ) {
    return commandRunnerService.getCommandStatusAsString(commandUUID);
  }

  @PostMapping(value = "/get")
  public String getACommand(
      @RequestParam(name = "commandUuid") UUID commandUUID
  ) {
    return commandRunnerService.getCommandAsString(commandUUID);
  }

  @PostMapping(value = "/getByStatus")
  public List<String> getCommandsByStatus(
      @RequestParam(name = "status") Command.CommandStatus commandStatus
  ) {
    return commandRunnerService.getCommandsByStatus(commandStatus);
  }

  @PostMapping(value = "/getAll")
  public List<String> getAllCommands() {
    return commandRunnerService.getAllCommands();
  }

  @PostMapping(value = "/getAllScheduled")
  public List<String> getAllScheduledCommands() {
    return commandRunnerService.getAllScheduledCommands();
  }

  @PostMapping(value = "/removeScheduled")
  public void removeScheduledCommands(
      @RequestParam(name = "commandUuid") UUID commandUUID
  ) {
    commandRunnerService.removeScheduledCommands(commandUUID);
  }
}
