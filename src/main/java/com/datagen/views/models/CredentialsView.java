package com.datagen.views.models;

import com.datagen.service.credentials.Credentials;
import com.datagen.service.credentials.CredentialsService;
import com.datagen.service.credentials.CredentialsType;
import com.datagen.views.MainLayout;
import com.datagen.views.utils.UsersUtils;
import com.vaadin.flow.component.Component;
import com.vaadin.flow.component.Composite;
import com.vaadin.flow.component.HasText;
import com.vaadin.flow.component.button.Button;
import com.vaadin.flow.component.button.ButtonVariant;
import com.vaadin.flow.component.combobox.ComboBox;
import com.vaadin.flow.component.confirmdialog.ConfirmDialog;
import com.vaadin.flow.component.dialog.Dialog;
import com.vaadin.flow.component.formlayout.FormLayout;
import com.vaadin.flow.component.grid.Grid;
import com.vaadin.flow.component.grid.GridSortOrder;
import com.vaadin.flow.component.html.Span;
import com.vaadin.flow.component.notification.Notification;
import com.vaadin.flow.component.notification.NotificationVariant;
import com.vaadin.flow.component.orderedlayout.VerticalLayout;
import com.vaadin.flow.component.textfield.PasswordField;
import com.vaadin.flow.component.textfield.TextField;
import com.vaadin.flow.component.upload.Upload;
import com.vaadin.flow.component.upload.receivers.MultiFileMemoryBuffer;
import com.vaadin.flow.data.provider.SortDirection;
import com.vaadin.flow.data.renderer.ComponentRenderer;
import com.vaadin.flow.dom.Style;
import com.vaadin.flow.router.PageTitle;
import com.vaadin.flow.router.Route;
import com.vaadin.flow.spring.security.AuthenticationContext;
import jakarta.annotation.security.RolesAllowed;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.vaadin.lineawesome.LineAwesomeIcon;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Route(value = "model/credentials", layout = MainLayout.class)
@RolesAllowed({"ROLE_DATAGEN_USER", "ROLE_DATAGEN_ADMIN"})
public class CredentialsView extends Composite<VerticalLayout> {

    private CredentialsService credentialsService;

  private final transient AuthenticationContext authContext;

  /**
   * Fake binders below for just getting required info for adding a credentials
   */
    @Getter
    @Setter
    private byte[] credValueAdded;
    @Getter
    @Setter
    private String credNameAdded;
    @Getter
    @Setter
    private String credAccountAdded;
    @Getter
    @Setter
    private CredentialsType credTypeAdded;


    @Autowired
    public CredentialsView(AuthenticationContext authContext, CredentialsService credentialsService) {
        this.credentialsService = credentialsService;
        this.authContext = authContext;

        VerticalLayout layoutColumn = new VerticalLayout();
        layoutColumn.setWidth("100%");
        layoutColumn.getStyle().set("flex-grow", "1");

        var grid = new Grid<Credentials>();
        grid.addColumn(Credentials::getName)
            .setHeader("Name")
            .setSortable(true)
            .setFrozen(true)
            .setAutoWidth(true)
            .setWidth("10rem")
            .setFlexGrow(0);
        grid.addColumn(Credentials::getOwner)
            .setHeader("Owner")
            .setSortable(true)
            .setAutoWidth(true)
            .setWidth("10rem")
            .setFlexGrow(0);
      grid.addColumn(Credentials::getType)
          .setHeader("Type")
          .setSortable(true)
          .setAutoWidth(true)
          .setWidth("10rem")
          .setFlexGrow(0);
      grid.addColumn(Credentials::getAccountAssociated)
          .setHeader("Account")
          .setSortable(true)
          .setAutoWidth(true)
          .setWidth("10rem")
          .setFlexGrow(0);

      // Button to print credentials
      grid.addColumn(infoButton())
          .setHeader("Info")
          .setWidth("5rem")
          .setFlexGrow(0);

        // Delete button
        grid.addColumn(deleteButton(grid))
            .setHeader("Delete")
            .setWidth("5rem")
            .setFlexGrow(0);;

      // RightsButton
      grid.addColumn(rightsButton())
          .setHeader("Rights")
          .setWidth("5rem")
          .setFlexGrow(0);

      grid.getStyle().setBorderRadius("15px");
      grid.getStyle().setOverflow(Style.Overflow.HIDDEN);

      // Add grid to main layout and load data
      setGridItems(grid);
      grid.sort(List.of(new GridSortOrder<>(grid.getColumns().get(0), SortDirection.ASCENDING)));
      getContent().add(grid);

      getContent().add(uploadButton(grid));

    }

    private void setGridItems(Grid<Credentials> grid) {
      if(UsersUtils.isUserDatagenAdmin(authContext)){
        grid.setItems(this.credentialsService.listCredentialsMeta());
      } else {
        var user = UsersUtils.getUser(authContext);
        var groups = UsersUtils.getUserGroups(authContext);
        grid.setItems(this.credentialsService.listCredentialsMetaAllowedForUser(user, groups));
      }
    }

  /**
   * Create a button that pops up info in JSON format for a credentials
   * @return
   */
  private ComponentRenderer<Button, Credentials> infoButton() {
      return new ComponentRenderer<>(Button::new, (button, credentials) -> {
        button.addThemeVariants(ButtonVariant.LUMO_ICON,
            ButtonVariant.LUMO_TERTIARY);
        button.addClickListener(e -> {
          Dialog dialogInfo = new Dialog();
          dialogInfo.setHeaderTitle("Model: " + credentials.getName());

          var credAsJson = new Span(this.credentialsService.toJson(credentials));

          credAsJson.setWhiteSpace(HasText.WhiteSpace.PRE);
          dialogInfo.add(credAsJson);
          var closeButton = new Button("Close", eventClose -> dialogInfo.close());
          dialogInfo.getFooter().add(closeButton);
          dialogInfo.open();
        });
        button.setIcon(LineAwesomeIcon.INFO_CIRCLE_SOLID.create());
      });
    }

  /**
   * Creates a delete button that pops up a confirmation box and deletes the credentials (includes also a notification when done)
   * @param grid to refresh data once deleted
   * @return
   */
  private ComponentRenderer<Button, Credentials> deleteButton(Grid grid) {
      // Dialog
      ConfirmDialog dialog = new ConfirmDialog();
      dialog.setText("Are you sure you want to permanently delete this credential?");
      dialog.setCancelable(true);
      dialog.setConfirmText("Delete");
      dialog.setConfirmButtonTheme("error primary");

      return new ComponentRenderer<>(Button::new, (button, credentials) -> {
        button.addThemeVariants(ButtonVariant.LUMO_ICON,
            ButtonVariant.LUMO_ERROR);
        button.addClickListener(e -> {
          dialog.setHeader("Delete : " + credentials.getName());
          dialog.addConfirmListener(event -> {
            this.credentialsService.removeCredentials(credentials.getName());
            grid.getDataProvider().refreshAll();
            setGridItems(grid);
            Notification notification = Notification.show("Deleted Credential:" + credentials.getName());
            notification.addThemeVariants(NotificationVariant.LUMO_PRIMARY);
          });
          dialog.open();
        });
        if(!this.credentialsService.isUserOwnerOfCred(UsersUtils.getUser(authContext), credentials.getName()) &&
            !UsersUtils.isUserDatagenAdmin(authContext)){
          button.setEnabled(false);
          button.setIcon(LineAwesomeIcon.TRASH_ALT.create());
          button.addThemeVariants(ButtonVariant.LUMO_TERTIARY);
        } else {
          button.setIcon(LineAwesomeIcon.TRASH_ALT.create());
          button.addThemeVariants(ButtonVariant.LUMO_TERTIARY);
        }
      });
    }

  /**
   * Creates an upload button with drag and drop, once uploaded credentials is added
   * @param grid to refresh data once uploaded
   * @return
   */
  private Button uploadButton(Grid grid) {

    Button addCredentials = new Button("Add a Credential");
    addCredentials.addThemeVariants(ButtonVariant.LUMO_PRIMARY);

    Dialog dialogUpload= new Dialog();
    dialogUpload.setHeaderTitle("Add a Credentials");

    // Create a form to create a credentials and according to the type selected, an upload is possible
    FormLayout formLayout = new FormLayout();
    formLayout.setResponsiveSteps(
        // Use 2 columns by default
        new FormLayout.ResponsiveStep("0", 2)
    );

    // Name
    TextField name = new TextField("Name");
    name.setClearButtonVisible(true);
    name.setAutoselect(true);
    name.setRequired(true);
    name.setRequiredIndicatorVisible(true);
    name.addValueChangeListener(value -> this.setCredNameAdded(value.getValue()));
    formLayout.add(name);

    // Type
    ComboBox<CredentialsType> comboBox =
        new ComboBox<>("Type");
    comboBox.setRequired(true);
    comboBox.setRequiredIndicatorVisible(true);
    comboBox.setItems(
        EnumSet.allOf(CredentialsType.class).stream().toList());
    formLayout.add(comboBox);

    // Save and Close
    var saveCredentials = new Button("Save Credentials");
    saveCredentials.addThemeVariants(ButtonVariant.LUMO_PRIMARY);
    saveCredentials.setEnabled(false);
    saveCredentials.addClickListener(saveEvent -> {

      if(this.credentialsService.listCredentialsMetaAsMap().containsKey(this.getCredNameAdded())) {
        Notification notification = Notification.show("Credentials with same name already exists, change the name or remove previous credentials with this name",
            5000, Notification.Position.MIDDLE);
        notification.addThemeVariants(NotificationVariant.LUMO_ERROR);
      } else {
        var credentialsToSave =
            new Credentials(this.getCredNameAdded(), this.getCredTypeAdded(), this.getCredAccountAdded(), false, UsersUtils.getUser(authContext), null,
                null);
        this.credentialsService.addCredentialsWithValue(credentialsToSave, true,
            this.getCredValueAdded());
        var pathToCredentials = this.credentialsService.credFilePath(credentialsToSave);
        Notification notification = Notification.show("Credentials well saved, you can select it when launching a generation or use it directly in your model file by referencing this path: " + pathToCredentials, 5000,
            Notification.Position.BOTTOM_CENTER);
        notification.addThemeVariants(NotificationVariant.LUMO_SUCCESS);
        dialogUpload.close();
      }

      grid.getDataProvider().refreshAll();
      setGridItems(grid);
    });
    var closeCredentials = new Button("Close");
    closeCredentials.addClickListener(closeEvent -> dialogUpload.close());
    formLayout.add(saveCredentials, closeCredentials);

    // Depending on field type, add required fields for upload or password
    var listOfPossible = new LinkedList<Component>();
    comboBox.addValueChangeListener(e -> {
      this.setCredTypeAdded(e.getValue());
      formLayout.remove(listOfPossible);
      formLayout.remove(saveCredentials, closeCredentials);
      listOfPossible.clear();
      this.setCredValueAdded(null);
      this.setCredAccountAdded("");

      switch (e.getValue()) {
      case KEYTAB -> {
        uploadFileType(listOfPossible, "Upload Keytab", "Principal", "Principal name associated with keytab (like: francois@FRISCH.COM)");
      }
      case TRUSTSTORE -> {
        uploadFileType(listOfPossible, "Upload Truststore", "Truststore", "Password associated with the truststore (leave blank if no password set)");
      }
      case KEYSTORE -> {
        uploadFileType(listOfPossible, "Upload Keystore", "Keystore", "Password associated with the keystore (leave blank if no password set)");
      }
      case GCP_KEY_FILE -> {
        uploadFileType(listOfPossible, "Upload GCP Application Credentials File", "Project ID", "Project ID where inside the Application Credentials File has been generated");
      }
      case GCP_ACCESS_TOKEN -> {
        uploadPasswordType(listOfPossible, "GCP Access Token", "The value of the GCP Access token generated (and still valid) for an account that has rights to access GCP resources", "Project ID", "Project ID where inside the Access Token has been generated");
      }
      case S3_ACCESS_KEY -> {
        uploadPasswordType(listOfPossible, "S3 Access Key Secret", "S3 Access Key Secret", "S3 Access Key Id", "S3 Access Key Id");
      }
      case ADLS_SAS_TOKEN -> {
        uploadPasswordType(listOfPossible, "ADLS SAS Token", "SAS token generated for the storage account and valid", "Storage Account Name", "Name of the storage account on which this SAS token has been generated");
      }
      case PASSWORD-> {
        uploadPasswordType(listOfPossible, "Password", "Password associated to the account", "Account", "Account associated with the password");
      }
      }

      saveCredentials.setEnabled(true);
      formLayout.add(listOfPossible);
      formLayout.add(saveCredentials, closeCredentials);
    });

      dialogUpload.add(formLayout);
      addCredentials.addClickListener(event -> dialogUpload.open());

      return addCredentials;
    }

    private void uploadPasswordType(LinkedList<Component> listOfPossible,
                                    String passwordFieldName, String passwordFieldHelperText,
                                    String accountFieldName, String accountFieldHelperText ) {
      var passwordfield = new PasswordField(passwordFieldName);
      passwordfield.setRequired(true);
      passwordfield.setRevealButtonVisible(true);
      if(passwordFieldHelperText!=null && !passwordFieldHelperText.isEmpty()) {
        passwordfield.setHelperText(passwordFieldHelperText);
      }
      passwordfield.addValueChangeListener(vl -> this.setCredValueAdded(vl.getValue().getBytes()));
      var accountField = new TextField(accountFieldName);
      accountField.setRequired(false);
      if(accountFieldHelperText!=null && !accountFieldHelperText.isEmpty()) {
        accountField.setHelperText(accountFieldHelperText);
      }
      accountField.addValueChangeListener(value -> this.setCredAccountAdded(value.getValue()));
      listOfPossible.add(passwordfield);
      listOfPossible.add(accountField);
    }

    private void uploadFileType(LinkedList<Component> listOfPossible,
        String uploadFileName,
        String accountFieldName, String accountFieldHelperText) {
      // Add a drag and drop to import files of credentials, and just keep it in memory until saving of credentials is made
      var buffer = new MultiFileMemoryBuffer();
      var upload = new Upload(buffer);
      Button uploadButton = new Button(uploadFileName);
      uploadButton.addThemeVariants(ButtonVariant.LUMO_PRIMARY);
      upload.setUploadButton(uploadButton);

      upload.addSucceededListener(event -> {
        var fileName = event.getFileName();
        try(var inputStream = buffer.getInputStream(fileName)) {
          this.setCredValueAdded(inputStream.readAllBytes());
        } catch (IOException ioe){
          log.warn("Cannot read input data");
        } catch (Exception alle) {
          log.error("Failed to process uploaded file: {} with error: ", event.getFileName(), alle);
        }
      });

      var accountField = new TextField(accountFieldName);
      accountField.setRequired(false);
      if(accountFieldHelperText!=null && !accountFieldHelperText.isEmpty()) {
        accountField.setHelperText(accountFieldHelperText);
      }
      accountField.addValueChangeListener(value -> this.setCredAccountAdded(value.getValue()));
      listOfPossible.add(accountField);
      listOfPossible.add(upload);
    }

  /**
   * Create a rights Button to check rights but also change them
   * @return
   */
  private ComponentRenderer<Button, Credentials> rightsButton() {
    return new ComponentRenderer<>(Button::new, (button, credentials) -> {
      button.addThemeVariants(ButtonVariant.LUMO_ICON,
          ButtonVariant.LUMO_TERTIARY);

      button.addClickListener(e -> {
        Dialog dialogRights = new Dialog();
        dialogRights.setHeaderTitle("Credential: " + credentials.getName());

        var metaVL = new VerticalLayout();
        var spanOwner = new Span("Owner: " + credentials.getOwner());
        metaVL.add(spanOwner);

        var userUsers = new TextField("Users:");
        userUsers.setClearButtonVisible(true);
        userUsers.setTooltipText("A ',' separated list of users for use privileges");
        userUsers.setValue(String.join(",", credentials.getUsersAuthorized()));

        var userGroups = new TextField("Groups:");
        userGroups.setClearButtonVisible(true);
        userGroups.setTooltipText("A ',' separated list of groups for use privileges");
        userGroups.setValue(String.join(",", credentials.getGroupsAuthorized()));

        metaVL.add(userUsers, userGroups);
        dialogRights.add(metaVL);

        // Only admin users of a credentials can modify its rights (or datagen admin)
        if(UsersUtils.isUserDatagenAdmin(authContext) || this.credentialsService.isUserOwnerOfCred(UsersUtils.getUser(authContext), credentials.getName())) {
          var saveButton = new Button("Save", event -> {
            var success = this.credentialsService.changeRightsCred(credentials.getName(),
                Arrays.stream(userUsers.getValue().split(",")).collect(Collectors.toSet()),
                Arrays.stream(userGroups.getValue().split(",")).collect(Collectors.toSet()));
            if(success) {
              Notification.show(
                      "Successfully changed rights for credential: " + credentials.getName())
                  .addThemeVariants(NotificationVariant.LUMO_SUCCESS);
            } else {
              Notification.show(
                      "Unable to change rights for credential: " + credentials.getName() + " check server logs for more details")
                  .addThemeVariants(NotificationVariant.LUMO_ERROR);
            }
            dialogRights.close();
          });
          saveButton.addThemeVariants(ButtonVariant.LUMO_PRIMARY);
          dialogRights.getFooter().add(saveButton);
        } else {
          userUsers.setEnabled(false);
          userGroups.setEnabled(false);
          metaVL.add(new Span("You are not the owner, so cannot modify its rights."));
        }

        var closeButton = new Button("Close", eventClose -> dialogRights.close());

        dialogRights.getFooter().add(closeButton);
        dialogRights.open();
      });
      button.setIcon(LineAwesomeIcon.ACCESSIBLE_ICON.create());
    });
  }

}
