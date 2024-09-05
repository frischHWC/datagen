package com.datagen.service.credentials;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;
import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
public class Credentials {
  // This class should be written in a <uuid>.meta file in JSON format, while credentials itself are written in <uuid>.credentials file
  String name;
  UUID id; // This is giving the name of the credentials file which is <uuid>.credentials
  CredentialsType type;
  String accountAssociated; // used for kerberos username or id of the access key or account name associated with a token or password of keystore
  Boolean encrypted;
  String owner;
  List<String> usersAuthorized;
  List<String> groupsAuthorized;

  public Credentials(String name, CredentialsType credentialsType,
                     String accountAssociated, Boolean encrypted,
                     String owner, List<String> usersAuthorized, List<String> groupsAuthorized) {
    this.name = name;
    this.type = credentialsType;
    this.accountAssociated =accountAssociated;
    this.encrypted = encrypted;
    this.owner = owner;
    this.usersAuthorized = usersAuthorized;
    this.groupsAuthorized = groupsAuthorized;
    this.id = UUID.randomUUID();
  }
}
