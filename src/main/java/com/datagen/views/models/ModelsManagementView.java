package com.datagen.views.models;

import com.datagen.model.Row;
import com.datagen.service.model.ModelStoreService;
import com.datagen.views.MainLayout;
import com.datagen.views.utils.UsersUtils;
import com.vaadin.flow.component.Composite;
import com.vaadin.flow.component.HasText;
import com.vaadin.flow.component.button.Button;
import com.vaadin.flow.component.button.ButtonVariant;
import com.vaadin.flow.component.confirmdialog.ConfirmDialog;
import com.vaadin.flow.component.dialog.Dialog;
import com.vaadin.flow.component.grid.Grid;
import com.vaadin.flow.component.grid.GridSortOrder;
import com.vaadin.flow.component.grid.GridVariant;
import com.vaadin.flow.component.html.Span;
import com.vaadin.flow.component.notification.Notification;
import com.vaadin.flow.component.notification.NotificationVariant;
import com.vaadin.flow.component.orderedlayout.VerticalLayout;
import com.vaadin.flow.component.textfield.TextField;
import com.vaadin.flow.component.upload.Upload;
import com.vaadin.flow.component.upload.receivers.MultiFileMemoryBuffer;
import com.vaadin.flow.data.provider.SortDirection;
import com.vaadin.flow.data.renderer.ComponentRenderer;
import com.vaadin.flow.dom.Style;
import com.vaadin.flow.router.PageTitle;
import com.vaadin.flow.router.Route;
import com.vaadin.flow.server.StreamResource;
import com.vaadin.flow.spring.security.AuthenticationContext;
import jakarta.annotation.security.RolesAllowed;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.vaadin.lineawesome.LineAwesomeIcon;
import org.vaadin.olli.FileDownloadWrapper;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Route(value = "model/management", layout = MainLayout.class)
@RolesAllowed({"ROLE_DATAGEN_USER", "ROLE_DATAGEN_ADMIN"})
public class ModelsManagementView extends Composite<VerticalLayout> {

    private final transient AuthenticationContext authContext;

    private ModelStoreService modelStoreService;

    @Autowired
    public ModelsManagementView(AuthenticationContext authContext, ModelStoreService modelStoreService) {
        this.authContext = authContext;
        this.modelStoreService = modelStoreService;

        VerticalLayout layoutColumn = new VerticalLayout();
        layoutColumn.setWidth("100%");
        layoutColumn.getStyle().set("flex-grow", "1");

        var grid = new Grid<ModelStoreService.ModelStored>();
        grid.addThemeVariants(GridVariant.LUMO_ROW_STRIPES);
        grid.addColumn(ModelStoreService.ModelStored::getName)
            .setHeader("Name")
            .setSortable(true)
            .setFrozen(true)
            .setAutoWidth(true)
            .setWidth("10rem")
            .setFlexGrow(0);
        grid.addColumn(ModelStoreService.ModelStored::getOwner)
            .setHeader("Owner")
            .setSortable(true)
            .setWidth("10rem")
            .setFlexGrow(0);

      // Button to print model and download it
      grid.addColumn(infoButtonWithDownload())
          .setHeader("Info")
          .setWidth("5rem")
          .setFlexGrow(0);

        // Delete button
        grid.addColumn(deleteButton(grid))
            .setHeader("Delete")
            .setWidth("5rem")
            .setFlexGrow(0);

      // TestButton
      grid.addColumn(testButton(layoutColumn))
          .setHeader("Test")
          .setWidth("5rem")
          .setFlexGrow(0);

      // RightsButton
      grid.addColumn(rightsButton())
          .setHeader("Rights")
          .setWidth("5rem")
          .setFlexGrow(0);

        // Columns list of each model
        grid.addColumn(m -> m.getModel().getFields().keySet().stream().collect(Collectors.joining(",")))
            .setHeader("Columns List")
            .setAutoWidth(true);

      grid.getStyle().setBorderRadius("15px");
      grid.getStyle().setOverflow(Style.Overflow.HIDDEN);

      // Add grid to main layout and load data
      setItemsForGrid(grid);
      grid.sort(
          List.of(new GridSortOrder<>(grid.getColumns().get(0), SortDirection.ASCENDING)));
      getContent().add(grid);


      getContent().add(uploadButton(grid));

    }

  /**
   * Set the items for the grid based on user rights
   * This function must be called to refresh or set items each time
   */
  private void setItemsForGrid(Grid<ModelStoreService.ModelStored> grid) {
    var user = UsersUtils.getUser(authContext);
    var groups = UsersUtils.getUserGroups(authContext);

    if(UsersUtils.isUserDatagenAdmin(authContext)) {
      grid.setItems(this.modelStoreService.listModelsAsModelStored());
    } else {
      grid.setItems(this.modelStoreService.listModelsAsModelStoredAuthorized(user, groups));
    }
  }

  /**
   * Create a button that pops up info on a model (by describing it) and add a button to download it
   * @return
   */
  private ComponentRenderer<Button, ModelStoreService.ModelStored> infoButtonWithDownload() {
      return new ComponentRenderer<>(Button::new, (button, modelStored) -> {
        button.addThemeVariants(ButtonVariant.LUMO_ICON,
            ButtonVariant.LUMO_TERTIARY);
        button.addClickListener(e -> {
          Dialog dialogInfo = new Dialog();
          dialogInfo.setHeaderTitle("Model: " + modelStored.getName());

          var modelAsJson = new Span(modelStoreService.getModelAsJson(modelStored.getName()));
          modelAsJson.setWhiteSpace(HasText.WhiteSpace.PRE);
          dialogInfo.add(modelAsJson);

          var closeButton = new Button("Close", eventClose -> dialogInfo.close());
          var downloadButton = new Button(LineAwesomeIcon.DOWNLOAD_SOLID.create());
          downloadButton.setTooltipText("Download Model");
          downloadButton.addThemeVariants(ButtonVariant.LUMO_PRIMARY);
          downloadButton.addClickListener(eventD -> {
            Notification notification = Notification.show("Downloaded Model:" + modelStored.getName());
            notification.addThemeVariants(NotificationVariant.LUMO_PRIMARY);
          });

          // Setup download resource for download button
          StreamResource resource = new StreamResource(modelStored.getName() + ".json",
              () ->  {
                try { return new FileInputStream(modelStoreService.getModelAsFile(modelStored.getName()));
                } catch (Exception exception) {
                  return new ByteArrayInputStream("Model not Found".getBytes());
                }
              }
          );
          FileDownloadWrapper downloadButtonWrapper = new FileDownloadWrapper(resource);
          downloadButtonWrapper.wrapComponent(downloadButton);

          dialogInfo.getFooter().add(closeButton);
          dialogInfo.getFooter().add(downloadButtonWrapper);
          dialogInfo.open();
        });
        button.setIcon(LineAwesomeIcon.INFO_CIRCLE_SOLID.create());

        // If a user is not admin or cannot see a model, it should not be able to see it or download it
        if(!UsersUtils.isUserDatagenAdmin(authContext) && !this.modelStoreService.isUserAllowedToSeeModel(UsersUtils.getUser(authContext),
            UsersUtils.getUserGroups(authContext), modelStored.getName())) {
          button.setEnabled(false);
        }
      });

    }

  /**
   * Creates a delete button that pops up a confirmation box and deletes the model (includes also a notification when done)
   * @param grid to refresh data once deleted
   * @return
   */
  private ComponentRenderer<Button, ModelStoreService.ModelStored> deleteButton(Grid grid) {
      // Dialog
      ConfirmDialog dialog = new ConfirmDialog();
      dialog.setText("Are you sure you want to permanently delete this model?");
      dialog.setCancelable(true);
      dialog.setConfirmText("Delete");
      dialog.setConfirmButtonTheme("error primary");

      return new ComponentRenderer<>(Button::new, (button, modelStored) -> {
        button.addThemeVariants(ButtonVariant.LUMO_ICON,
            ButtonVariant.LUMO_ERROR);
        button.addClickListener(e -> {
          dialog.setHeader("Delete : " + modelStored.getName());
          dialog.addConfirmListener(event -> {
            modelStoreService.deleteModel(modelStored.getName());
            grid.getDataProvider().refreshAll();
            setItemsForGrid(grid);
            Notification notification = Notification.show("Deleted Model:" + modelStored.getName());
            notification.addThemeVariants(NotificationVariant.LUMO_PRIMARY);
          });
          dialog.open();
        });

        if(
            !modelStoreService.isUserAllowedToAdminModel(UsersUtils.getUser(authContext), UsersUtils.getUserGroups(authContext), modelStored.getName())
                && !UsersUtils.isUserDatagenAdmin(authContext)
        ) {
          button.setEnabled(false);
          button.setTooltipText("Your user is not admin and has no right to delete this model. Check Model Permission.");
          button.setIcon(LineAwesomeIcon.TRASH_ALT.create());
          button.addThemeVariants(ButtonVariant.LUMO_TERTIARY);
        } else {
          button.setIcon(LineAwesomeIcon.TRASH_ALT.create());
          button.addThemeVariants(ButtonVariant.LUMO_TERTIARY);
        }
      });
    }

  /**
   * Creates a test button that pops up a dialog with a generated row
   * @return
   */
  private ComponentRenderer<Button, ModelStoreService.ModelStored> testButton(VerticalLayout layoutColumn) {
    return new ComponentRenderer<>(Button::new, (testModel, modelStored) -> {
      testModel.setIcon(LineAwesomeIcon.PLAY_CIRCLE_SOLID.create());
      testModel.addThemeVariants(ButtonVariant.LUMO_ICON,
          ButtonVariant.LUMO_TERTIARY);

      // Test a model
      testModel.addClickListener(e -> {
        // Generate a row
        var rows = modelStored.getModel()
            .generateRandomRows(1, 1);
        if (rows != null && !rows.isEmpty()) {
          var row = (Row) rows.get(0);
          var textArea = new Span(row.toPrettyJSONAllFields());
          textArea.setWhiteSpace(HasText.WhiteSpace.PRE);
          // Set it as JSON format to dialog box
          Dialog dialog = new Dialog();
          dialog.setHeaderTitle("New Generated Row");
          dialog.add(textArea);
          Button cancelButton = new Button("Close", ev -> dialog.close());
          dialog.getFooter().add(cancelButton);
          dialog.open();
        } else {
          Notification notification = Notification.show(
              "Unable to generate a row for this model: " + modelStored.getName() +
                  ". Check logs for more information", 5000,
              Notification.Position.TOP_CENTER);
          notification.addThemeVariants(NotificationVariant.LUMO_ERROR);
        }

        // If a user is not admin or cannot see a model, it should not be able to test it
        if(!UsersUtils.isUserDatagenAdmin(authContext) && !this.modelStoreService.isUserAllowedToSeeModel(UsersUtils.getUser(authContext),
            UsersUtils.getUserGroups(authContext), modelStored.getName())) {
          testModel.setEnabled(false);
        }

      });
    });
  }

  /**
   * Creates an upload button with drag and drop, once uploaded model is added
   * @param grid to refresh data once uploaded
   * @return
   */
  private Upload uploadButton(Grid grid) {
      // Add a drag and drop to import models as files
      var buffer = new MultiFileMemoryBuffer();
      var upload = new Upload(buffer);
      upload.setAcceptedFileTypes("application/json", ".json");
      Button uploadButton = new Button("Upload Model");
      uploadButton.addThemeVariants(ButtonVariant.LUMO_PRIMARY);
      upload.setUploadButton(uploadButton);

      upload.addFileRejectedListener(event -> {
        Notification notification = Notification.show(event.getErrorMessage(), 5000,
            Notification.Position.MIDDLE);
        notification.addThemeVariants(NotificationVariant.LUMO_ERROR);
      });

      upload.addSucceededListener(event -> {
        var fileName = event.getFileName();
        try(var inputStream = buffer.getInputStream(fileName)) {
          var ownerName = this.authContext.getPrincipalName().orElseGet(() -> "anonymous");
          modelStoreService.addModel(inputStream, true, ownerName);
        } catch (IOException e){
          log.warn("Cannot read input data");
        } catch (Exception e) {
          log.error("Failed to process uploaded file: {} with error: ", event.getFileName(), e);
        }
        grid.getDataProvider().refreshAll();
        setItemsForGrid(grid);
      });

      return upload;
    }

  /**
   * Create a rights Button to check rights but also change them
    * @return
   */
  private ComponentRenderer<Button, ModelStoreService.ModelStored> rightsButton() {
    return new ComponentRenderer<>(Button::new, (button, modelStored) -> {
      button.addThemeVariants(ButtonVariant.LUMO_ICON,
          ButtonVariant.LUMO_TERTIARY);

      button.addClickListener(e -> {
        Dialog dialogInfo = new Dialog();
        dialogInfo.setHeaderTitle("Model: " + modelStored.getName());

        var modelMeta = modelStored.getModelMeta();
        var metaVL = new VerticalLayout();
        var spanOwner = new Span("Owner: " + modelMeta.getOwner());
        metaVL.add(spanOwner);

        var userUsers = new TextField("Users:");
        userUsers.setClearButtonVisible(true);
        userUsers.setTooltipText("A ',' separated list of users for use privileges");
        userUsers.setValue(String.join(",", modelMeta.getAllowedUsers()));

        var userGroups = new TextField("Groups:");
        userGroups.setClearButtonVisible(true);
        userGroups.setTooltipText("A ',' separated list of groups for use privileges");
        userGroups.setValue(String.join(",", modelMeta.getAllowedGroups()));

        var adminUsers = new TextField("Admin Users:");
        adminUsers.setClearButtonVisible(true);
        adminUsers.setTooltipText("A ',' separated list of users for admin privileges");
        adminUsers.setValue(String.join(",", modelMeta.getAllowedAdminUsers()));

        var adminGroups = new TextField("Admin Groups:");
        adminGroups.setClearButtonVisible(true);
        adminGroups.setTooltipText("A ',' separated list of groups for admin privileges");
        adminGroups.setValue(String.join(",", modelMeta.getAllowedAdminGroups()));

        metaVL.add(userUsers, userGroups, adminUsers, adminGroups);
        dialogInfo.add(metaVL);

        // Only admin users of a model can modify its rights
        if(this.modelStoreService.isUserAllowedToAdminModel(UsersUtils.getUser(authContext),
            UsersUtils.getUserGroups(authContext), modelStored.getName()) || UsersUtils.isUserDatagenAdmin(authContext)) {
          var saveButton = new Button("Save", event -> {
            var success = this.modelStoreService.changeModelMeta(modelStored.getName(), Arrays.stream(userUsers.getValue().split(",")).collect(Collectors.toSet()),
                Arrays.stream(userGroups.getValue().split(",")).collect(Collectors.toSet()),
                Arrays.stream(adminUsers.getValue().split(",")).collect(Collectors.toSet()),
                Arrays.stream(adminGroups.getValue().split(",")).collect(Collectors.toSet()));
            if(success) {
              Notification.show(
                      "Successfully changed rights for model: " + modelStored.getName())
                  .addThemeVariants(NotificationVariant.LUMO_SUCCESS);
              dialogInfo.close();
            } else {
              Notification.show(
                      "Unable to change rights for model: " + modelStored.getName() + " check server logs for more details")
                  .addThemeVariants(NotificationVariant.LUMO_ERROR);
            }
          });
          saveButton.addThemeVariants(ButtonVariant.LUMO_PRIMARY);

          dialogInfo.getFooter().add(saveButton);
        } else {
          userUsers.setEnabled(false);
          userGroups.setEnabled(false);
          adminUsers.setEnabled(false);
          adminGroups.setEnabled(false);
          metaVL.add(new Span("You are not the owner, so cannot modify its rights."));
        }

        var closeButton = new Button("Close", eventClose -> dialogInfo.close());

        dialogInfo.getFooter().add(closeButton);
        dialogInfo.open();
      });
      button.setIcon(LineAwesomeIcon.ACCESSIBLE_ICON.create());
    });
  }


}
