package com.cloudera.frisch.randomdatagen.controller;


import com.cloudera.frisch.randomdatagen.service.Command;
import com.cloudera.frisch.randomdatagen.service.CommandRunnerService;
import com.cloudera.frisch.randomdatagen.service.CommandSoft;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
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

  @PostMapping(value = "/getCommandStatus", produces = {MediaType.APPLICATION_JSON_VALUE})
  public CommandSoft getStatusOfACommand(
      @RequestParam(name = "commandUuid") UUID commandUUID
  ) {
    return commandRunnerService.getCommandStatusShort(commandUUID);
  }

  @PostMapping(value = "/get")
  public String getACommand(
      @RequestParam(name = "commandUuid") UUID commandUUID
  ) {
    return commandRunnerService.getCommandAsString(commandUUID);
  }

  @PostMapping(value = "/getByStatus", produces = {MediaType.APPLICATION_JSON_VALUE})
  public List<CommandSoft> getCommandsByStatus(
      @RequestParam(name = "status") Command.CommandStatus commandStatus
  ) {
    return commandRunnerService.getCommandsByStatus(commandStatus);
  }

  @PostMapping(value = "/getAll", produces = {MediaType.APPLICATION_JSON_VALUE})
  public List<CommandSoft> getAllCommands() {
    return commandRunnerService.getAllCommands();
  }

  @PostMapping(value = "/getAllScheduled", produces = {MediaType.APPLICATION_JSON_VALUE})
  public List<CommandSoft> getAllScheduledCommands() {
    return commandRunnerService.getAllScheduledCommands();
  }

  @PostMapping(value = "/removeScheduled")
  public void removeScheduledCommands(
      @RequestParam(name = "commandUuid") UUID commandUUID
  ) {
    commandRunnerService.removeScheduledCommands(commandUUID);
  }
}
