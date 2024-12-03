package com.datagen.utils;

import lombok.Getter;

public class CommandException extends Throwable {
  private static final long serialVersionUID = -338751699229948L;
  @Getter
  private ExceptionType type;
  // TODO: Throws this exception each time a command fails, so easier to log and restitute to the user

  public CommandException() {
    super();
  }

  public CommandException(String message) {
    super(message);
  }

  public CommandException(String message, ExceptionType exceptionType) {
    super(message);
    this.type = exceptionType;
  }

  public CommandException(String message, Throwable cause) {
    super(message, cause);
  }

  public CommandException(Throwable cause) {
    super(cause);
  }

  protected CommandException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  public enum ExceptionType {
    ACCESS_RIGHTS,
    FILE_NOT_FOUND,
    DIRECTORY_NOT_FOUND,
    DATABASE_NO_FOUND,
    TABLE_NOT_FOUND,
    CONNECTION,
    WRONG_CREDENTIALS
  }
}
