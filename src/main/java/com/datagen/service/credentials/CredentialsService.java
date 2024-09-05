package com.datagen.service.credentials;

import com.datagen.config.ApplicationConfigs;
import com.datagen.config.PropertiesLoader;
import com.datagen.connector.storage.utils.FileUtils;
import com.datagen.model.Model;
import com.datagen.model.OptionsConverter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

@Slf4j
@Service
public class CredentialsService {
  // This service aims at storing and listing/deleting and retrieving  different type of credentials
  // encrypted or not, with limited access to some users/groups

  private final Map<ApplicationConfigs, String> properties;
  private Map<String, Credentials> credentialsStored;
  private final String credentialsDir;
  private final ObjectMapper objectMapper;

  @Autowired
  public CredentialsService(PropertiesLoader propertiesLoader) {
    this.objectMapper = new ObjectMapper();
    this.properties = propertiesLoader.getPropertiesCopy();
    this.credentialsDir = this.properties.get(ApplicationConfigs.DATAGEN_CREDENTIALS_PATH);
    this.credentialsStored = new HashMap<>();

    FileUtils.createLocalDirectoryWithStrongRights(this.credentialsDir);

    // Read credentials stored at start
    var files = FileUtils.listLocalFiles(this.credentialsDir);
    var filesToDelete = new ArrayList<String>();
    if(files!=null) {
      Arrays.stream(files).forEach(f -> {
        if(f.getName().endsWith(".meta")) {
          try {
            var credentialsRead = this.objectMapper.readValue(f, Credentials.class);
            if(!FileUtils.checkLocalFileExists(this.credentialsDir+"/"+f.getName().split("[.]")[0]+".credentials")) {
              log.warn("Local credentials file does not exists for credentials: {}, hence deleting it", credentialsRead.getName());
              filesToDelete.add(f.getAbsolutePath());
            } else {
              credentialsStored.put(credentialsRead.getName(), credentialsRead);
            }
          } catch (Exception e) {
            log.warn("Could not deserialize credentials meta file: {} due to error: ", f.getAbsolutePath(), e);
          }
        } else if (f.getName().endsWith(".credentials")) {
          if(!FileUtils.checkLocalFileExists(this.credentialsDir+"/"+f.getName().split("[.]")[0]+".meta")) {
            log.warn(
                "Local meta file does not exists for credentials: {}, hence deleting it",
                f.getName());
            filesToDelete.add(f.getAbsolutePath());
          }
        } else {
          log.warn("File: {} is not a credentials, hence will be ignored", f.getAbsolutePath());
        }
      });

      filesToDelete.forEach(FileUtils::deleteLocalFile);
    }
  }


  public String toJson(Credentials credentials) {
    try {
      return objectMapper.writeValueAsString(credentials);
    } catch (JsonProcessingException e) {
      log.warn("Cannot return credentials: {} as JSON", credentials.getName());
      return "{ Exception: " + e + " } ";
    }
  }

  public String toJson(String credentials) {
    return toJson(this.credentialsStored.get(credentials));
  }

  public List<Credentials> listCredentialsMeta() {
    return this.credentialsStored.values().stream().toList();
  }

  public Map<String, Credentials> listCredentialsMetaAsMap() {
    return this.credentialsStored;
  }

  public Map<String, Credentials> listCredentialsMetaAsMap(String filterOwner) {
    return this.credentialsStored;
  }

  public List<Credentials> listCredentialsMeta(String filterOwner) {
    return this.credentialsStored.values().stream()
        .filter(c -> c.getOwner().equalsIgnoreCase(filterOwner))
        .toList();
  }

  public List<Credentials> listCredentialsMetaAuthorized(String filterUser) {
    return this.credentialsStored.values().stream()
        .filter(c -> c.getOwner().equalsIgnoreCase(filterUser) || c.getUsersAuthorized().contains(filterUser))
        .toList();
  }

  public void addCredentials(Credentials credentials, Boolean replace, String filePathToCredentials) {
    if(credentials.getName()!=null) {
      if(credentialsStored.get(credentials.getName())!=null && !replace) {
        return;
      } else {
        try {
          FileUtils
              .createLocalFileAsOutputStream(metaCredFilePath(credentials))
              .write(objectMapper.writeValueAsBytes(credentials));
          FileUtils.moveLocalFile(filePathToCredentials, credFilePath(credentials));
          credentialsStored.put(credentials.getName(), credentials);
        } catch (Exception e) {
          log.warn("Could not write files for credentials: {}", credentials.getName());
        }
      }
    }
  }

  public void addCredentialsWithValue(Credentials credentials, Boolean replace, String credentialsValue) {
    if(credentialsValue!=null) {
      addCredentialsWithValue(credentials, replace, credentialsValue.getBytes());
    }
  }

  public void addCredentialsWithValue(Credentials credentials, Boolean replace, byte[] credentialsValue) {
    if(credentialsValue!=null) {
      try {
        FileUtils.createLocalFileAsOutputStream(credFilePath(credentials)+".tmp")
            .write(credentialsValue);
        addCredentials(credentials, replace, credFilePath(credentials)+".tmp");
      } catch (Exception e) {
        log.warn("Cannot create temporary credentials value");
      }
    }
  }

  public Credentials addCredentials(@NonNull String name,
                                CredentialsType type,
                                String accountAssociated,
                                String owner,
                                Boolean toEncrypt,
                                List<String> usersAuthorized,
                                List<String> groupsAuthorized,
                                String credentialsFilePath,
                                Boolean replace) {
    var cred = new Credentials(name, type, accountAssociated, toEncrypt, owner, usersAuthorized, groupsAuthorized);

    addCredentials(cred, replace, credentialsFilePath);

    return cred;
  }

  public Credentials addCredentials(@NonNull String name,
                                    CredentialsType type,
                                    String accountAssociated,
                                    String owner,
                                    String credentialsFilePath,
                                    Boolean replace) {
  return addCredentials(name,type, accountAssociated, owner, false, null, null, credentialsFilePath, replace);
  }

  public String getCredentialsFileName(String credName) {
    return getCredentialsFileName(credentialsStored.get(credName));
  }

  public String getCredentialsFileName(Credentials credentials) {
    return credFilePath(credentials);
  }

  public String getCredentialsContent(String credName) {
    return getCredentialsContent(credentialsStored.get(credName));
  }

  public String getCredentialsContent(Credentials cred) {
    var credFilePath = credFilePath(cred);
    return FileUtils.getfileContentInString(credFilePath);
  }

  public void removeCredentials(String credName) {
    var cred = credentialsStored.get(credName);
    if(cred !=null) {
      FileUtils.deleteLocalFile(metaCredFilePath(cred));
      FileUtils.deleteLocalFile(credFilePath(cred));
      credentialsStored.remove(credName);
    } else {
      log.warn("Credentials: {} has not been found", credName);
    }
  }

  public void removeCredentials(Credentials cred) {
    removeCredentials(cred.getName());
  }


  /**
   * Give the Absolute Path for Meta File for a given credentials
   * @return
   */
  private String metaCredFilePath(Credentials credentials) {
    return credentialsDir + "/" + credentials.getId() + ".meta";
  }

  /**
   * Give the Absolute Path for Cred File for a given credentials
   * @return
   */
  public String credFilePath(Credentials credentials) {
    return credentialsDir + "/" + credentials.getId() + ".credentials";
  }

  public void enrichModelWithCredentials(Model model, String credentials) {
    enrichModelWithCredentials(model, credentialsStored.get(credentials));
  }

  // Given a credentials name passed and a model, this function aims at retrieving the credentials
  // and inject into the model all properties that are affected by this type of credentials
  public void enrichModelWithCredentials(Model model, Credentials credentials) {
    if(credentials==null) {
      log.warn("Could not retrieve credentials, so cannot enrich model with this credentials");
    } else {
      Map<OptionsConverter.TableNames, String> tableNames = model.getTableNames();
      switch(credentials.getType()) {
      case KEYTAB -> {
        tableNames.put(OptionsConverter.TableNames.HDFS_USE_KERBEROS, "true");
        tableNames.put(OptionsConverter.TableNames.HDFS_USER, credentials.getAccountAssociated());
        tableNames.put(OptionsConverter.TableNames.HDFS_KEYTAB, getCredentialsFileName(credentials));
        tableNames.put(OptionsConverter.TableNames.OZONE_USE_KERBEROS, "true");
        tableNames.put(OptionsConverter.TableNames.OZONE_USER, credentials.getAccountAssociated());
        tableNames.put(OptionsConverter.TableNames.OZONE_KEYTAB, getCredentialsFileName(credentials));
        tableNames.put(OptionsConverter.TableNames.HIVE_USE_KERBEROS, "true");
        tableNames.put(OptionsConverter.TableNames.HIVE_USER, credentials.getAccountAssociated());
        tableNames.put(OptionsConverter.TableNames.HIVE_KEYTAB, getCredentialsFileName(credentials));
        tableNames.put(OptionsConverter.TableNames.HBASE_USE_KERBEROS, "true");
        tableNames.put(OptionsConverter.TableNames.HBASE_USER, credentials.getAccountAssociated());
        tableNames.put(OptionsConverter.TableNames.HBASE_KEYTAB, getCredentialsFileName(credentials));
        tableNames.put(OptionsConverter.TableNames.KAFKA_USE_KERBEROS, "true");
        tableNames.put(OptionsConverter.TableNames.KAFKA_USER, credentials.getAccountAssociated());
        tableNames.put(OptionsConverter.TableNames.KAFKA_KEYTAB, getCredentialsFileName(credentials));
        tableNames.put(OptionsConverter.TableNames.KUDU_USE_KERBEROS, "true");
        tableNames.put(OptionsConverter.TableNames.KUDU_USER, credentials.getAccountAssociated());
        tableNames.put(OptionsConverter.TableNames.KUDU_KEYTAB, getCredentialsFileName(credentials));
        tableNames.put(OptionsConverter.TableNames.SOLR_USE_KERBEROS, "true");
        tableNames.put(OptionsConverter.TableNames.SOLR_USER, credentials.getAccountAssociated());
        tableNames.put(OptionsConverter.TableNames.SOLR_KEYTAB, getCredentialsFileName(credentials));
      }
      case KEYSTORE -> {
        tableNames.put(OptionsConverter.TableNames.KAFKA_KEYSTORE_KEY_PASSWORD, credentials.getAccountAssociated());
        tableNames.put(OptionsConverter.TableNames.KAFKA_KEYSTORE_PASSWORD, credentials.getAccountAssociated());
        tableNames.put(OptionsConverter.TableNames.KAFKA_KEYSTORE_LOCATION, getCredentialsFileName(credentials));
        tableNames.put(OptionsConverter.TableNames.SOLR_KEYSTORE_PASSWORD, credentials.getAccountAssociated());
        tableNames.put(OptionsConverter.TableNames.SOLR_KEYSTORE_LOCATION, getCredentialsFileName(credentials));
      }
      case TRUSTSTORE -> {
        tableNames.put(OptionsConverter.TableNames.KAFKA_TRUSTSTORE_PASSWORD, credentials.getAccountAssociated());
        tableNames.put(OptionsConverter.TableNames.KAFKA_KEYSTORE_LOCATION, getCredentialsFileName(credentials));
        tableNames.put(OptionsConverter.TableNames.SOLR_TRUSTSTORE_PASSWORD, credentials.getAccountAssociated());
        tableNames.put(OptionsConverter.TableNames.SOLR_TRUSTSTORE_LOCATION, getCredentialsFileName(credentials));
      }
      case ADLS_SAS_TOKEN -> {
        tableNames.put(OptionsConverter.TableNames.ADLS_ACCOUNT_NAME, credentials.getAccountAssociated());
        tableNames.put(OptionsConverter.TableNames.ADLS_SAS_TOKEN, getCredentialsContent(credentials));
      }
      case S3_ACCESS_KEY -> {
        tableNames.put(OptionsConverter.TableNames.S3_ACCESS_KEY_ID, credentials.getAccountAssociated());
        tableNames.put(OptionsConverter.TableNames.S3_ACCESS_KEY_SECRET, getCredentialsContent(credentials));
      }
      case GCP_KEY_FILE -> {
        tableNames.put(OptionsConverter.TableNames.GCS_PROJECT_ID, credentials.getAccountAssociated());
        tableNames.put(OptionsConverter.TableNames.GCS_ACCOUNT_KEY_PATH, getCredentialsFileName(credentials));
      }
      case GCP_ACCESS_TOKEN -> {
        tableNames.put(OptionsConverter.TableNames.GCS_PROJECT_ID, credentials.getAccountAssociated());
        tableNames.put(OptionsConverter.TableNames.GCS_ACCOUNT_ACCESS_TOKEN, getCredentialsContent(credentials));
      }
      }
    }

  }



}
