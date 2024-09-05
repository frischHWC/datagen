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
package com.datagen.controller.command;


import com.datagen.service.command.Command;
import com.datagen.service.command.CommandRunnerService;
import com.datagen.service.command.CommandSoft;
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
@RequestMapping("/api/v1/command")
public class CommandRunnerController {

  @Autowired
  private CommandRunnerService commandRunnerService;

  @PostMapping(value = "/getCommandStatus", produces = {
      MediaType.APPLICATION_JSON_VALUE})
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

  @PostMapping(value = "/getByStatus", produces = {
      MediaType.APPLICATION_JSON_VALUE})
  public List<CommandSoft> getCommandsByStatus(
      @RequestParam(name = "status") Command.CommandStatus commandStatus
  ) {
    return commandRunnerService.getCommandsSoftByStatus(commandStatus);
  }

  @PostMapping(value = "/getAll", produces = {MediaType.APPLICATION_JSON_VALUE})
  public List<CommandSoft> getAllCommands() {
    return commandRunnerService.getAllCommandsSoft();
  }

  @PostMapping(value = "/getAllScheduled", produces = {
      MediaType.APPLICATION_JSON_VALUE})
  public List<CommandSoft> getAllScheduledCommands() {
    return commandRunnerService.getAllScheduledCommandsSoft();
  }

  @PostMapping(value = "/removeScheduled")
  public void removeScheduledCommands(
      @RequestParam(name = "commandUuid") UUID commandUUID
  ) {
    commandRunnerService.removeScheduledCommands(commandUUID);
  }
}
