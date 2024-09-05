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
package com.datagen.service.command;

import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nullable;
import java.util.UUID;

@Getter
@Setter
public class CommandSoft {

  private static final long serialVersionUID = 1L;

  private UUID commandUuid = null;
  private Command.CommandStatus status = null;
  private String commandComment = "";
  private Long durationSeconds = 0L;
  private double progress = 0d;

  CommandSoft(@Nullable Command command) {
    if (command != null) {
      this.commandUuid = command.getCommandUuid();
      this.status = command.getStatus();
      this.commandComment = command.getCommandError();
      this.durationSeconds = command.getDurationMilliSeconds();
      this.progress = command.getProgress();
    } else {
      this.commandComment = "Command not Found";
    }
  }


}
