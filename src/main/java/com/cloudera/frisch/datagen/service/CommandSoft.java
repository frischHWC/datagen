package com.cloudera.frisch.datagen.service;

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
    if(command!=null) {
      this.commandUuid = command.getCommandUuid();
      this.status = command.getStatus();
      this.commandComment = command.getCommandComment();
      this.durationSeconds = command.getDurationSeconds();
      this.progress = command.getProgress();
    } else {
      this.commandComment = "Command not Found";
    }
  }


}
