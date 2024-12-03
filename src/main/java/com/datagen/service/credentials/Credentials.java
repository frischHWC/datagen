package com.datagen.service.credentials;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Collections;
import java.util.Set;
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
  Set<String> usersAuthorized;
  Set<String> groupsAuthorized;

  public Credentials(String name, CredentialsType credentialsType,
                     String accountAssociated, Boolean encrypted,
                     String owner, Set<String> usersAuthorized, Set<String> groupsAuthorized) {
    this.name = name;
    this.type = credentialsType;
    this.accountAssociated = accountAssociated;
    this.encrypted = encrypted;
    if( owner!=null && !owner.isEmpty()) {
      this.owner = owner;
    } else {
      this.owner = "anonymous";
    }
    if(usersAuthorized==null) {
      this.usersAuthorized = Collections.emptySet();
    } else {
      this.usersAuthorized = usersAuthorized;
    }
    if(groupsAuthorized==null) {
      this.groupsAuthorized = Collections.emptySet();
    } else {
      this.groupsAuthorized = groupsAuthorized;
    }

    this.id = UUID.randomUUID();
  }
}
