package com.datagen.controller.credentials;

import com.datagen.service.credentials.Credentials;
import com.datagen.service.credentials.CredentialsService;
import com.datagen.service.credentials.CredentialsType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("/api/v1/credentials")
public class CredentialsController {

  @Autowired
  private CredentialsService credentialsService;

  @GetMapping(value = "/listCredentials", produces = {
      MediaType.APPLICATION_JSON_VALUE})
  public List<Credentials> getCredentials() {
    return credentialsService.listCredentialsMeta();
  }

  @GetMapping(value = "/listCredentialsForUser", produces = {
      MediaType.APPLICATION_JSON_VALUE})
  public List<Credentials> getCredentialsOwnedByAUser(
      @RequestParam(name = "user") String username
  ) {
    return credentialsService.listCredentialsMeta(username);
  }

  @PostMapping(value = "/addCredentials", produces = {
      MediaType.APPLICATION_JSON_VALUE})
  public Credentials addCredentials(
      @RequestParam(name = "name") String name,
      @RequestParam(name = "type") CredentialsType type,
      @RequestParam(name = "account") String accountAssociated,
      @RequestParam(name = "owner") String owner,
      @RequestParam(name = "content") String credentialsContent,
      @RequestParam(name = "repllace") Boolean replace
  ) {
    return credentialsService.addCredentials(name,type,accountAssociated,owner, credentialsContent,replace);
  }

  @DeleteMapping(value = "/removeCredentials", produces = {
      MediaType.APPLICATION_JSON_VALUE})
  public void removeCredentals(
      @RequestParam(name = "credName") String credName
  ) {
    credentialsService.removeCredentials(credName);
  }

}
