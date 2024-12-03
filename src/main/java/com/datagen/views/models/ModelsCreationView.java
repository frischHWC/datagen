package com.datagen.views.models;

import com.datagen.config.ApplicationConfigs;
import com.datagen.config.PropertiesLoader;
import com.datagen.model.Model;
import com.datagen.model.OptionsConverter;
import com.datagen.model.Row;
import com.datagen.model.type.*;
import com.datagen.service.model.ModelStoreService;
import com.datagen.views.MainLayout;
import com.datagen.views.utils.UsersUtils;
import com.vaadin.flow.component.*;
import com.vaadin.flow.component.button.Button;
import com.vaadin.flow.component.button.ButtonVariant;
import com.vaadin.flow.component.combobox.ComboBox;
import com.vaadin.flow.component.details.Details;
import com.vaadin.flow.component.dialog.Dialog;
import com.vaadin.flow.component.formlayout.FormLayout;
import com.vaadin.flow.component.html.Span;
import com.vaadin.flow.component.icon.Icon;
import com.vaadin.flow.component.icon.VaadinIcon;
import com.vaadin.flow.component.notification.Notification;
import com.vaadin.flow.component.notification.NotificationVariant;
import com.vaadin.flow.component.orderedlayout.FlexComponent;
import com.vaadin.flow.component.orderedlayout.HorizontalLayout;
import com.vaadin.flow.component.orderedlayout.VerticalLayout;
import com.vaadin.flow.component.textfield.TextField;
import com.vaadin.flow.component.upload.Upload;
import com.vaadin.flow.component.upload.receivers.MultiFileMemoryBuffer;
import com.vaadin.flow.data.binder.Binder;
import com.vaadin.flow.data.binder.ValidationException;
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
import java.util.*;

import static com.datagen.views.models.ModelsUtils.*;


@Slf4j
@Route(value = "model/creation", layout = MainLayout.class)
@RolesAllowed({"ROLE_DATAGEN_USER", "ROLE_DATAGEN_ADMIN"})
public class ModelsCreationView extends Composite<VerticalLayout> {

  private final transient AuthenticationContext authContext;

  @Autowired
  private ModelStoreService modelStoreService;


  private final LinkedList<Binder<FieldRepresentation>> binders;
  private final Binder<Map<OptionsConverter.TableNames, String>> tableNamesPropsBinder;
  private final Binder<Map<OptionsConverter.Options, Object>> optionsPropsBinder;
  private final Map<ApplicationConfigs, String> propertiesFromConfigFile;
  private final Details connectorDetails;
  private String modelId = "";
  private String modelLoaded="";

  @Autowired
  public ModelsCreationView(AuthenticationContext authContext, PropertiesLoader propertiesLoader) {
    this.authContext = authContext;
    binders = new LinkedList<>();
    propertiesFromConfigFile = propertiesLoader.getPropertiesCopy();
    tableNamesPropsBinder = new Binder<>();
    optionsPropsBinder = new Binder<>();

    VerticalLayout layoutColumn = new VerticalLayout();
    layoutColumn.setWidth("100%");
    layoutColumn.getStyle().set("flex-grow", "1");

    // Name for the model
    var headerLayout = new HorizontalLayout();
    var modelName = new TextField("Model Name:");
    modelName.setHelperText("Provide a meaningful and unique name to your model");
    modelName.setRequired(true);
    modelName.setClearButtonVisible(true);
    modelName.setMaxWidth("50%");
    modelName.setWidth("50%");
    modelName.setMinWidth("50%");

    // Model buttons
    var modelHl = new HorizontalLayout();
    var testModelButton = testModelButton(layoutColumn);
    var downloadModel = downloadModelButton();
    var saveModel = saveModelButton(layoutColumn, modelName, downloadModel, testModelButton, modelHl);
    modelHl.add(
        saveModel,
        downloadModel,
        testModelButton
    );

    // Optional configs for Connectors
    this.connectorDetails = connectorsConfigs();

    // Add a + column button
    var plusButton = new Button("Add a Column", new Icon(VaadinIcon.PLUS));
    plusButton.addThemeVariants(ButtonVariant.LUMO_ICON);
    plusButton.setAriaLabel("Add a Column");
    // Add a column button
    plusButton.addClickListener(e -> {
      layoutColumn.remove(plusButton);
      layoutColumn.remove(connectorDetails);
      layoutColumn.remove(modelHl);
      layoutColumn.addAndExpand(createFormForAField(layoutColumn, null));
      layoutColumn.add(plusButton);
      layoutColumn.add(connectorDetails);
      layoutColumn.add(modelHl);
    });

    // Add header
    var importModel = importModelButton(layoutColumn, modelName, plusButton, modelHl);
    headerLayout.setAlignItems(FlexComponent.Alignment.BASELINE);
    headerLayout.addAndExpand(modelName);
    headerLayout.add(importModel);

    layoutColumn.add(headerLayout);
    layoutColumn.add(plusButton);
    layoutColumn.add(connectorDetails);
    layoutColumn.add(modelHl);

    getContent().add(layoutColumn);

  }

  /**
   * Creates a basic download model that will wrapped up later by save model button
   * @return
   */
  private Button downloadModelButton() {
    Button downloadModel = new Button("Download Model");
    downloadModel.addThemeVariants(ButtonVariant.LUMO_PRIMARY);
    downloadModel.setEnabled(false);

    return downloadModel;
  }

  /**
   * Create a test model button that generates a row with model passed
   * @param layoutColumn
   * @return
   */
  private Button testModelButton(VerticalLayout layoutColumn) {
    Button testModel = new Button("Test Model");
    testModel.addThemeVariants(ButtonVariant.LUMO_PRIMARY);
    testModel.setEnabled(false);

    // Test a model
    testModel.addClickListener(e -> {
      // Generate a row
      var rows = modelStoreService.getModel(modelId)
          .generateRandomRows(1, 1);
      if(rows!=null && !rows.isEmpty()) {
        var row = (Row) rows.get(0);
        var textArea = new Span(row.toPrettyJSONAllFields());
        textArea.setWhiteSpace(HasText.WhiteSpace.PRE);
        // Set it as JSON format to dialog box
        Dialog dialog = new Dialog();
        dialog.setHeaderTitle("New Generated Row");
        dialog.add(textArea);
        Button cancelButton = new Button("Close", ev -> dialog.close());
        dialog.getFooter().add(cancelButton);
        Button retryButton = new Button("Re-Try", ev -> { dialog.close(); testModel.click();});
        dialog.getFooter().add(retryButton);
        layoutColumn.add(dialog);
        dialog.open();
      }  else {
        Notification notification = Notification.show("Unable to generate a row for this model: " + modelId + ". Check logs for more information", 5000,
            Notification.Position.TOP_CENTER);
        notification.addThemeVariants(NotificationVariant.LUMO_ERROR);
      }

    });

    return testModel;
  }

  /**
   * Creates a save Model button, that enables test and download button and setup the downloaded file for download button
   * @return
   */
  private Button saveModelButton(VerticalLayout layoutColumn, TextField modelName, Button downloadModel, Button testModel, HorizontalLayout modelHl) {
    Button saveModel = new Button("Save Model");
    saveModel.addThemeVariants(ButtonVariant.LUMO_PRIMARY);

    // Save model by getting all fields, creating a model object and writing this model object on disk
    saveModel.addClickListener(e -> {
      if(modelStoreService.checkModelExists(modelName.getValue())) {
        // A non-admin user cannot overwrite a model if it does not belong to him
        if(
            !modelStoreService.isUserAllowedToAdminModel(UsersUtils.getUser(authContext), UsersUtils.getUserGroups(authContext), modelName.getValue()) &&
                !UsersUtils.isUserDatagenAdmin(authContext)
        ) {
          Notification notificationErrorModelSave = Notification.show(" You cannot save model: \'" + modelName.getValue() +
                  "\' because you are not admin. " +
                  "Change name or ask to change permissions on it.", 5000,
              Notification.Position.TOP_CENTER);
          notificationErrorModelSave.addThemeVariants(NotificationVariant.LUMO_ERROR);
          notificationErrorModelSave.open();
        } else {
          var dialogModelAlreadyExists =
              new Dialog("Model already exists, do you want to overwrite it ?");
          Button cancelButton = new Button("Close", ev -> {
            dialogModelAlreadyExists.close();
          });
          Button saveButton = new Button("Overwrite");
          saveButton.addThemeVariants(ButtonVariant.LUMO_ERROR);
          saveButton.addClickListener(saveEvent -> {
            saveModel(createModelFromBinders(modelName.getValue()),
                layoutColumn, modelHl, saveModel, testModel, downloadModel);
            dialogModelAlreadyExists.close();
          });

          dialogModelAlreadyExists.getFooter().add(cancelButton, saveButton);
          dialogModelAlreadyExists.open();
        }
      } else {
        saveModel(createModelFromBinders(modelName.getValue()), layoutColumn, modelHl, saveModel, testModel, downloadModel);
      }

    });

    return saveModel;
  }


  /**
   * Create a model from existing binders
   * @param name
   * @return
   */
  private Model createModelFromBinders(String name) {
    var listOfFields = new LinkedList<FieldRepresentation>();
    this.binders.forEach(b -> {
      var f = new FieldRepresentation();
      try {
        b.writeBean(f);
      } catch (ValidationException exception) {
        log.warn("Fields cannot be validated, user must re-take actions ");
      }
      listOfFields.add(f);
    });
    var tableNameProps = new HashMap<OptionsConverter.TableNames, String>();
    var optionProps = new HashMap<OptionsConverter.Options, Object>();
    try {
      this.optionsPropsBinder.writeBean(optionProps);
      this.tableNamesPropsBinder.writeBean(tableNameProps);
    } catch (ValidationException exception) {
      log.warn("Optional Configurations cannot be validated, user must re-take actions ");
    }

    // Then bind to a model object
    var fields = new LinkedHashMap<String, Field>();
    listOfFields.forEach(f -> {
      log.debug("Received field: {}", f.toString());
      if(f.getName()==null ||f.getName().isEmpty() || f.getType()==null) {
        log.warn("Cannot add field: {} because its name or type ({}) is empty or null", f.getName(), f.getType());
      } else {
        fields.put(f.getName(),
            Field.instantiateField(propertiesFromConfigFile, f));
      }
    });

    var model = new Model<>(name, fields, null, null, null,
        propertiesFromConfigFile);
    model.setOptions(optionProps);
    model.setTableNames(tableNameProps);

    return model;
  }

  /**
   * Save a mode and prepare buttons for testing and download
   * @param model
   * @param layoutColumn
   * @param modelHl
   * @param saveModel
   * @param testModel
   * @param downloadModel
   */
  private void saveModel(Model model, VerticalLayout layoutColumn, HorizontalLayout modelHl, Button saveModel, Button testModel, Button downloadModel) {
    // Save the model
    var modelStored = modelStoreService.addModel(model, false, UsersUtils.getUser(authContext));
    modelId = modelStored.getName();
    log.debug("Created model with id: {} from UI: {}", modelId, model);

    // Allow button for test and download
    layoutColumn.remove(modelHl);
    modelHl.removeAll();

    testModel.setEnabled(true);
    downloadModel.setEnabled(true);

    var downloadButtonWrapper = wrapDownloadButton(downloadModel, modelId);
    modelHl.add(saveModel, downloadButtonWrapper, testModel);
    layoutColumn.add(modelHl);

    // Notify user of model well saved
    Notification notification = Notification.show("Model Saved with name: " + modelId, 5000,
        Notification.Position.TOP_CENTER);
    notification.addThemeVariants(NotificationVariant.LUMO_SUCCESS);

  }

  /**
   * Creates an import model button that allows to either import from exitsing models or import one from a file
   * @param layoutColumn
   * @param modelName
   * @return
   */
  private Button importModelButton(VerticalLayout layoutColumn, TextField modelName, Button plusButton, HorizontalLayout modelHl) {
    var loadButton = new Button("Load Model", LineAwesomeIcon.UPLOAD_SOLID.create());
    loadButton.addThemeVariants(ButtonVariant.LUMO_ICON);
    loadButton.setAriaLabel("Load a Model");
    loadButton.setMaxWidth("20%");

    // Load a model button
    loadButton.addClickListener(e -> {
      var dialogLoad = new Dialog();
      dialogLoad.setHeaderTitle("Which model do you want to load ?");
      var listOfmodels = new ComboBox<String>("Registered Models");
      if(UsersUtils.isUserDatagenAdmin(authContext)) {
        listOfmodels.setItems(modelStoreService.listModels());
      } else {
        listOfmodels.setItems(
            modelStoreService.listModelsAsModelStoredAuthorized(
                UsersUtils.getUser(authContext)).stream().map(
                ModelStoreService.ModelStored::getName).toList());
      }
      listOfmodels.addValueChangeListener(modelChosen -> modelLoaded=modelChosen.getValue());
      dialogLoad.add(listOfmodels);
      Button cancelButton = new Button("Close", ev -> dialogLoad.close());
      Button loadFinalButton = new Button("Load");
      loadFinalButton.addThemeVariants(ButtonVariant.LUMO_PRIMARY);

      loadFinalButton.addClickListener(loadEvent -> {
        log.debug("Model named: {} has been loaded, once user click on load, it should create columns below", modelLoaded);
        if(!modelLoaded.isEmpty() && !modelLoaded.isBlank()) {
          var modelLoadedAsModel = modelStoreService.getModel(modelLoaded);
          if(modelLoadedAsModel!=null) {
            modelName.setValue(modelLoadedAsModel.getName());

            // Remove plus button
            layoutColumn.remove(plusButton);
            layoutColumn.remove(connectorDetails);
            layoutColumn.remove(modelHl);

            modelLoadedAsModel.getFields().forEach((f, det) -> {
              FieldRepresentation currentField = new FieldRepresentation((Field) det);
              log.debug("Adding a column named: {} with following details: {}", f, currentField.toString());
              Details currentFieldAsForm = createFormForAField(layoutColumn, currentField);
              currentFieldAsForm.setOpened(false);
              layoutColumn.add(currentFieldAsForm);
            });
            //Set back everything
            layoutColumn.add(plusButton);
            layoutColumn.add(connectorDetails);
            layoutColumn.add(modelHl);

            Notification notification =
                Notification.show("Model: " + modelId + " has been loaded", 5000,
                    Notification.Position.TOP_CENTER);
            notification.addThemeVariants(NotificationVariant.LUMO_SUCCESS);
          }
          dialogLoad.close();
        }
      });
      dialogLoad.getFooter().add(cancelButton, loadFinalButton);
      dialogLoad.open();
    });

    return loadButton;
  }


  /**
   * Add an upload model button that is setting the model loaded when clicked
   * @return
   */
  private Upload uploadModelButton() {// Add a drag and drop to import models as files
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
        modelLoaded = modelStoreService.addModel(inputStream, true, ownerName).getName();
      } catch (IOException e){
        log.warn("Cannot read input data");
      } catch (Exception e) {
        log.error("Failed to process uploaded file: {} with error: ", event.getFileName(), e);
      }
    });
    return upload;
  }

  private FileDownloadWrapper wrapDownloadButton(Button downloadModel, String modelId) {
    // Setup download resource for download button
    StreamResource resource = new StreamResource(
        modelId + ".json",
        () ->  {
          try { return new FileInputStream(modelStoreService.getModelAsFile(modelId));
          } catch (Exception exception) {
            return new ByteArrayInputStream("Model not Found".getBytes());
          }
        }
    );
    FileDownloadWrapper downloadButtonWrapper = new FileDownloadWrapper(resource);
    downloadButtonWrapper.wrapComponent(downloadModel);
    return downloadButtonWrapper;
  }

  /**
   * Return a dynamic form for a field
   * If a field representation is not null, that means that values should be pre-filled with it
   *
   * @return
   */
  private Details createFormForAField(VerticalLayout parentLayout, FieldRepresentation fieldRepresentation) {
    // Wrap the form in a Details
    HorizontalLayout summary = new HorizontalLayout();
    summary.setSpacing(true);
    summary.add(fieldRepresentation!=null?new Text(fieldRepresentation.getName()):new Text("myColumn"));

    // Binder
    Binder<FieldRepresentation> binder =
        new Binder<>(FieldRepresentation.class);

    // Form
    FormLayout formLayout = new FormLayout();
    formLayout.setResponsiveSteps(new FormLayout.ResponsiveStep("0", 3));
    formLayout.setMinWidth("100%");
    formLayout.setWidth("100%");
    formLayout.setMaxWidth("100%");

    // Main Horizontal Layout
    var mainHl = new HorizontalLayout();
    mainHl.setPadding(true);

    // Name of the field
    TextField fieldName = new TextField("Name");
    fieldName.setClearButtonVisible(true);
    fieldName.setValue(fieldRepresentation!=null?fieldRepresentation.getName():"myColumn");
    fieldName.setAutoselect(true);
    fieldName.setRequired(true);
    fieldName.setRequiredIndicatorVisible(true);
    fieldName.setMinWidth("50%");
    fieldName.setWidth("50%");
    fieldName.setMaxWidth("50%");
    binder.forField(fieldName)
        .withValidator(v -> v!=null && !v.isEmpty() && !v.isBlank(), "Name must not be empty")
        .bind(FieldRepresentation::getName,
        FieldRepresentation::setName);
    fieldName.addValueChangeListener(cl -> {
      summary.removeAll();
      summary.add(new Text(cl.getValue()));
      formLayout.setId(cl.getValue());
    });

    // Type of the field
    ComboBox<FieldRepresentation.FieldType> comboBox =
        new ComboBox<>("Type");
    comboBox.setRequired(true);
    comboBox.setRequiredIndicatorVisible(true);
    comboBox.setItems(
        EnumSet.allOf(FieldRepresentation.FieldType.class).stream()
            .sorted(Comparator.comparing(Enum::name))
            .toList());
    binder.bind(comboBox, FieldRepresentation::getType,
        FieldRepresentation::setType);
    comboBox.setMinWidth("40%");
    comboBox.setWidth("40%");
    comboBox.setMaxWidth("40%");

    // Remove button
    Button removeButton = new Button(VaadinIcon.TRASH.create());
    removeButton.addThemeVariants(ButtonVariant.LUMO_ERROR, ButtonVariant.LUMO_PRIMARY);
    removeButton.setMinWidth("5%");
    removeButton.setMaxWidth("5%");
    removeButton.setWidth("5%");

    //Align items
    mainHl.setAlignItems(FlexComponent.Alignment.BASELINE);
    mainHl.addAndExpand(fieldName, comboBox, removeButton);

    // Add main layout
    formLayout.add(mainHl, 3);

    // Add Ghost button
    var ghostButton = ModelsUtils.createGhost(binder);
    formLayout.add(createInfoForAParameter(ghostButton,
    """
    To not output this column at the end but just compute it.
    Useful to create columns that depends on results of others.
    """), 3);
    if(fieldRepresentation!=null && fieldRepresentation.getGhost()!=null) {
      ghostButton.setValue(fieldRepresentation.getGhost());
    }

    // Depending on the type of the field, there should be different printed values
    var listOfOptionsForField = new LinkedList<Component>();
    var listOfField = new ArrayList<HasValue<?, ?>>();
    binders.add(binder);

    comboBox.addValueChangeListener(e -> {
      formLayout.remove(listOfOptionsForField);
      listOfField.forEach(binder::removeBinding);
      listOfField.clear();
      listOfOptionsForField.clear();

      switch (e.getValue()) {
      case STRING, STRING_AZ -> {
        var lengthParam = ModelsUtils.createLengthInt("(Optional) Length:", binder, 0, Integer.MAX_VALUE);
        if(fieldRepresentation!=null && fieldRepresentation.getLength()!=null) {
          lengthParam.setValue(fieldRepresentation.getLength());
        }
        listOfField.add(lengthParam);
        listOfOptionsForField.add(createInfoForAParameter(lengthParam, " Optional size of the string. Default to 20 characters"));

        var possibleValues = ModelsUtils.createList("(Optional) Possible values:", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getPossibleValuesWeighted()!=null) {
          possibleValues.setValue(fieldRepresentation.getPossibleValuesWeighted());
        }
        listOfField.add(possibleValues);
        listOfOptionsForField.add(ModelsUtils.createInfoForAParameter(possibleValues, possibleValues.getDescription()));

        var injectionValue = ModelsUtils.createInjecter("(Optional) Injection:", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getInjection()!=null) {
          injectionValue.setValue(fieldRepresentation.getInjection());
        }
        listOfField.add(injectionValue);
        listOfOptionsForField.add(ModelsUtils.createInfoForAParameter(injectionValue,
            """
                Instead of generating random value, value is generated using other column values,
                  by making an injection of theses values inside this column.
                Other columns are referenced using a ${}.
                Example: 
                  ${name}@${company}.com 
                  will generate: 
                   'francois@cloudera.com' or 'michael@company.com' 
                   depending on values of columns 'name' & 'company' previous ly defined.
                """
        ));

        var formulaValue = ModelsUtils.createFormula("(Optional) Compute:", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getFormula()!=null) {
          formulaValue.setValue(fieldRepresentation.getFormula());
        }
        listOfField.add(formulaValue);
        listOfOptionsForField.add(ModelsUtils.createInfoForAParameter(formulaValue,
            """
                Instead of generating random value, value is generated using other column values, 
                  by making a computing using other column values.
                It is using a JS evaluator, hence letting to use any kind of operators to compute a formula, 
                  or to even use coding function like if, for etc...
                Other columns are referenced using a ${}.
                Examples:
                '(${hour} +6)/24'
                or:
                'if( ${test} > 15) { true } else { false }'
                """
        ));
      }

      case INTEGER -> {
        var minParam = ModelsUtils.createMinInt("(Optional) Minimum:", binder, Integer.MIN_VALUE, Integer.MAX_VALUE);
        if(fieldRepresentation!=null && fieldRepresentation.getMin()!=null) {
          minParam.setValue(fieldRepresentation.getMin().intValue());
        }
        listOfField.add(minParam);
        listOfOptionsForField.add(createInfoForAParameter(minParam,
            " Optional minimum value. Default to " + Integer.MIN_VALUE));

        var maxParam = ModelsUtils.createMaxInt("(Optional) Maximum:", binder, Integer.MIN_VALUE, Integer.MAX_VALUE);
        if(fieldRepresentation!=null && fieldRepresentation.getMax()!=null) {
          maxParam.setValue(fieldRepresentation.getMax().intValue());
        }
        listOfField.add(maxParam);
        listOfOptionsForField.add(createInfoForAParameter(maxParam,
            " Optional maximum value. Default to " + Integer.MAX_VALUE));

        var possibleValues = ModelsUtils.createList("(Optional) Possible values:", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getPossibleValuesWeighted()!=null) {
          possibleValues.setValue(fieldRepresentation.getPossibleValuesWeighted());
        }
        listOfField.add(possibleValues);
        listOfOptionsForField.add(ModelsUtils.createInfoForAParameter(possibleValues, possibleValues.getDescription()));

        var formulaValue = ModelsUtils.createFormula("(Optional) Compute:", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getFormula()!=null) {
          formulaValue.setValue(fieldRepresentation.getFormula());
        }
        listOfField.add(formulaValue);
        listOfOptionsForField.add(ModelsUtils.createInfoForAParameter(formulaValue,
            """
                Instead of generating random value, value is generated using other column values, 
                  by making a computing using other column values.
                It is using a JS evaluator, hence letting to use any kind of operators to compute a formula, 
                  or to even use coding function like if, for etc...
                Other columns are referenced using a ${}.
                Examples:
                '(${hour} +6)/24'
                or:
                'if( ${test} > 15) { true } else { false }'
                """
        ));
      }

      case INCREMENT_INTEGER -> {
        var minParam = ModelsUtils.createMinInt("(Optional) Minimum:", binder, Integer.MIN_VALUE, Integer.MAX_VALUE);
        if(fieldRepresentation!=null && fieldRepresentation.getMin()!=null) {
          minParam.setValue(fieldRepresentation.getMin().intValue());
        }
        listOfField.add(minParam);
        listOfOptionsForField.add(createInfoForAParameter(minParam,
            " Optional minimum value. Default to " + 0L));
      }

      case INCREMENT_LONG -> {
        var minParam = ModelsUtils.createMinLong("(Optional) Minimum:", binder, Long.MIN_VALUE, Long.MAX_VALUE);
        if(fieldRepresentation!=null && fieldRepresentation.getMin()!=null) {
          minParam.setValue(fieldRepresentation.getMin().doubleValue());
        }
        listOfField.add(minParam);
        listOfOptionsForField.add(createInfoForAParameter(minParam,
                " Optional minimum value. Default to " + 0L));
      }

      case COUNTRY -> {
        var possibleValues = ModelsUtils.createList("(Optional) Possible values:", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getPossibleValuesWeighted()!=null) {
          possibleValues.setValue(fieldRepresentation.getPossibleValuesWeighted());
        }
        listOfField.add(possibleValues);
        listOfOptionsForField.add(ModelsUtils.createInfoForAParameter(possibleValues, possibleValues.getDescription()));
      }

      case BOOLEAN, TIMESTAMP -> {
        var possibleValues = ModelsUtils.createList("(Optional) Possible values:", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getPossibleValuesWeighted()!=null) {
          possibleValues.setValue(fieldRepresentation.getPossibleValuesWeighted());
        }
        listOfField.add(possibleValues);
        listOfOptionsForField.add(ModelsUtils.createInfoForAParameter(possibleValues, possibleValues.getDescription()));

        var formulaValue = ModelsUtils.createFormula("(Optional) Compute:", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getFormula()!=null) {
          formulaValue.setValue(fieldRepresentation.getFormula());
        }
        listOfField.add(formulaValue);
        listOfOptionsForField.add(ModelsUtils.createInfoForAParameter(formulaValue,
            """
                Instead of generating random value, value is generated using other column values, 
                  by making a computing using other column values.
                It is using a JS evaluator, hence letting to use any kind of operators to compute a formula, 
                  or to even use coding function like if, for etc...
                Other columns are referenced using a ${}.
                Examples:
                '(${hour} +6)/24'
                or:
                'if( ${test} > 15) { true } else { false }'
                """
        ));
      }

      case FLOAT -> {
        var minParam = ModelsUtils.createMinFloat("(Optional) Minimum:", binder, Float.MIN_VALUE, Float.MAX_VALUE);
        if(fieldRepresentation!=null && fieldRepresentation.getMin()!=null) {
          minParam.setValue(fieldRepresentation.getMin().doubleValue());
        }
        listOfField.add(minParam);
        listOfOptionsForField.add(createInfoForAParameter(minParam,
            " Optional minimum value. Default to " + Float.MIN_VALUE));

        var maxParam = ModelsUtils.createMaxFloat("(Optional) Maximum:", binder, Float.MIN_VALUE, Float.MAX_VALUE);
        if(fieldRepresentation!=null && fieldRepresentation.getMax()!=null) {
          maxParam.setValue(fieldRepresentation.getMax().doubleValue());
        }
        listOfField.add(maxParam);
        listOfOptionsForField.add(createInfoForAParameter(maxParam,
            " Optional maximum value. Default to " + Float.MAX_VALUE));

        var possibleValues = ModelsUtils.createList("(Optional) Possible values:", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getPossibleValuesWeighted()!=null) {
          possibleValues.setValue(fieldRepresentation.getPossibleValuesWeighted());
        }
        listOfField.add(possibleValues);
        listOfOptionsForField.add(ModelsUtils.createInfoForAParameter(possibleValues, possibleValues.getDescription()));

        var formulaValue = ModelsUtils.createFormula("(Optional) Compute:", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getFormula()!=null) {
          formulaValue.setValue(fieldRepresentation.getFormula());
        }
        listOfField.add(formulaValue);
        listOfOptionsForField.add(ModelsUtils.createInfoForAParameter(formulaValue,
            """
                Instead of generating random value, value is generated using other column values, 
                  by making a computing using other column values.
                It is using a JS evaluator, hence letting to use any kind of operators to compute a formula, 
                  or to even use coding function like if, for etc...
                Other columns are referenced using a ${}.
                Examples:
                '(${hour} +6)/24'
                or:
                'if( ${test} > 15) { true } else { false }'
                """
        ));
      }

      case LONG -> {
        var minParam = ModelsUtils.createMinLong("(Optional) Minimum:", binder, Long.MIN_VALUE, Long.MAX_VALUE);
        if(fieldRepresentation!=null && fieldRepresentation.getMin()!=null) {
          minParam.setValue(fieldRepresentation.getMin().doubleValue());
        }
        listOfField.add(minParam);
        listOfOptionsForField.add(createInfoForAParameter(minParam,
            " Optional minimum value. Default to " + Long.MIN_VALUE));

        var maxParam = ModelsUtils.createMaxLong("(Optional) Maximum:", binder, Long.MIN_VALUE, Long.MAX_VALUE);
        if(fieldRepresentation!=null && fieldRepresentation.getMax()!=null) {
          maxParam.setValue(fieldRepresentation.getMax().doubleValue());
        }
        listOfField.add(maxParam);
        listOfOptionsForField.add(createInfoForAParameter(maxParam,
            " Optional maximum value. Default to " + Long.MAX_VALUE));

        var possibleValues = ModelsUtils.createList("(Optional) Possible values:", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getPossibleValuesWeighted()!=null) {
          possibleValues.setValue(fieldRepresentation.getPossibleValuesWeighted());
        }
        listOfField.add(possibleValues);
        listOfOptionsForField.add(ModelsUtils.createInfoForAParameter(possibleValues, possibleValues.getDescription()));

        var formulaValue = ModelsUtils.createFormula("(Optional) Compute:", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getFormula()!=null) {
          formulaValue.setValue(fieldRepresentation.getFormula());
        }
        listOfField.add(formulaValue);
        listOfOptionsForField.add(ModelsUtils.createInfoForAParameter(formulaValue,
            """
                Instead of generating random value, value is generated using other column values, 
                  by making a computing using other column values.
                It is using a JS evaluator, hence letting to use any kind of operators to compute a formula, 
                  or to even use coding function like if, for etc...
                Other columns are referenced using a ${}.
                Examples:
                '(${hour} +6)/24'
                or:
                'if( ${test} > 15) { true } else { false }'
                """
        ));
      }

      case BYTES, HASH_MD5 -> {
        var lengthParam = ModelsUtils.createLengthInt("(Optional) Length:", binder, 0, Integer.MAX_VALUE);
        if(fieldRepresentation!=null && fieldRepresentation.getLength()!=null) {
          lengthParam.setValue(fieldRepresentation.getLength());
        }
        listOfField.add(lengthParam);
        listOfOptionsForField.add(createInfoForAParameter(lengthParam, " Optional size of the bytes array. Default to 20 bytes"));

        var possibleValues = ModelsUtils.createList("(Optional) Possible values:", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getPossibleValuesWeighted()!=null) {
          possibleValues.setValue(fieldRepresentation.getPossibleValuesWeighted());
        }
        listOfField.add(possibleValues);
        listOfOptionsForField.add(ModelsUtils.createInfoForAParameter(possibleValues, possibleValues.getDescription()));

        var formulaValue = ModelsUtils.createFormula("(Optional) Compute:", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getFormula()!=null) {
          formulaValue.setValue(fieldRepresentation.getFormula());
        }
        listOfField.add(formulaValue);
        listOfOptionsForField.add(ModelsUtils.createInfoForAParameter(formulaValue,
            """
                Instead of generating random value, value is generated using other column values, 
                  by making a computing using other column values.
                It is using a JS evaluator, hence letting to use any kind of operators to compute a formula, 
                  or to even use coding function like if, for etc...
                Other columns are referenced using a ${}.
                Examples:
                '(${hour} +6)/24'
                or:
                'if( ${test} > 15) { true } else { false }'
                """
        ));
      }

      case BIRTHDATE -> {
        var possibleValues = ModelsUtils.createList("(Optional) Possible values:", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getPossibleValuesWeighted()!=null) {
          possibleValues.setValue(fieldRepresentation.getPossibleValuesWeighted());
        }
        listOfField.add(possibleValues);
        listOfOptionsForField.add(ModelsUtils.createInfoForAParameter(possibleValues, possibleValues.getDescription()));

        var minParam = ModelsUtils.createMinBirthdate("(Optional) Minimum:", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getMinDate()!=null) {
          minParam.setValue(fieldRepresentation.getMinDate());
        }
        listOfField.add(minParam);
        listOfOptionsForField.add(createInfoForAParameter(minParam,
            " Optional minimum value in format: DD/MM/YYYY. Default to 01/01/1920"));

        var maxParam = ModelsUtils.createMaxBirthdate("(Optional) Maximum:", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getMaxDate()!=null) {
          maxParam.setValue(fieldRepresentation.getMaxDate());
        }
        listOfField.add(maxParam);
        listOfOptionsForField.add(createInfoForAParameter(maxParam,
            " Optional maximum value in format: DD/MM/YYYY. Default to 01/01/2024"));
      }

      case NAME -> {
        var filters = ModelsUtils.createFilters("(Optional) Filters on Country:", binder,
            NameField.listOfAvailableCountries);
        if(fieldRepresentation!=null && fieldRepresentation.getFilters()!=null) {
          filters.setValue(fieldRepresentation.getFilters());
        }
        listOfField.add(filters);
        listOfOptionsForField.add(ModelsUtils.createInfoForAParameter(filters,
            "Apply one or more filters to get only names present in one or more specified country."
            ));
      }

      case CITY -> {
        var filters = ModelsUtils.createFilters("(Optional) Filters on Country:", binder,
            CityField.listOfAvailableCountries);
        if(fieldRepresentation!=null && fieldRepresentation.getFilters()!=null) {
          filters.setValue(fieldRepresentation.getFilters());
        }
        listOfField.add(filters);
        listOfOptionsForField.add(ModelsUtils.createInfoForAParameter(filters,
            "Apply one or more filters to get only cities present in one or more specified country."
        ));
      }

      case EMAIL -> {
        var possibleValues = ModelsUtils.createList("(Optional) Possible values:", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getPossibleValuesWeighted()!=null) {
          possibleValues.setValue(fieldRepresentation.getPossibleValuesWeighted());
        }
        listOfField.add(possibleValues);
        listOfOptionsForField.add(ModelsUtils.createInfoForAParameter(possibleValues, possibleValues.getDescription()));

        var filters = ModelsUtils.createFilters("(Optional) Filters on Country:", binder,
            NameField.listOfAvailableCountries);
        if(fieldRepresentation!=null && fieldRepresentation.getFilters()!=null) {
          filters.setValue(fieldRepresentation.getFilters());
        }
        listOfField.add(filters);
        listOfOptionsForField.add(ModelsUtils.createInfoForAParameter(filters,
            "Apply one or more filters to get only names present in one or more specified country to construct the email."
        ));
      }

      case LINK -> {
        var linkValue = ModelsUtils.createLinkField("Link :", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getLink()!=null) {
          linkValue.setValue(fieldRepresentation.getLink());
        }
        listOfField.add(linkValue);
        listOfOptionsForField.add(ModelsUtils.createInfoForAParameter(linkValue,
            """
      This field provides a way to make a link from another column whose type is either CSV or City or Name.
      The link must be consists of 4 parts: 
        $<name of the other column to reference>.<name_of_the_column_to_use>
      Example: 
        $city_col.lat will give the latitude of a city picked by another column named 'city_col'.
      For City, links are: lat, long, country. 
      For Name, links are: sex, male, female, unisex.
      For CSV, links are other columns present in the CSV file.
      """
        ));}

      case CSV -> {
        var csvFile = ModelsUtils.createFileField("CSV File path", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getFile()!=null) {
          csvFile.setValue(fieldRepresentation.getFile());
        }
        listOfField.add(csvFile);
        listOfOptionsForField.add(ModelsUtils.createInfoForAParameter(csvFile,
            "Absolute path of the CSV file to read to get data to generate"
        ));

        var mainField = ModelsUtils.createMainField("Column name", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getMainField()!=null) {
          mainField.setValue(fieldRepresentation.getMainField());
        }
        listOfField.add(mainField);
        listOfOptionsForField.add(ModelsUtils.createInfoForAParameter(mainField,
            "Name of the column to pick based on the CSV header"
        ));

        var separator = ModelsUtils.createSeparatorField("(Optional) Separator", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getSeparator()!=null) {
          separator.setValue(fieldRepresentation.getSeparator());
        }
        listOfField.add(separator);
        listOfOptionsForField.add(ModelsUtils.createInfoForAParameter(separator,
            "Separator character of the input CSV "
        ));

        var filters = ModelsUtils.createListOfFilters("(Optional) Filters to apply:", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getFilters()!=null) {
          filters.setValue(fieldRepresentation.getFilters());
        }
        listOfField.add(filters);
        listOfOptionsForField.add(ModelsUtils.createInfoForAParameter(filters, filters.getDescription()));
      }

      case PHONE -> {
        var filters = ModelsUtils.createFilters("(Optional) Filters on Country:", binder,
            PhoneField.listOfAvailableCountries);
        if(fieldRepresentation!=null && fieldRepresentation.getFilters()!=null) {
          filters.setValue(fieldRepresentation.getFilters());
        }
        listOfField.add(filters);
        listOfOptionsForField.add(ModelsUtils.createInfoForAParameter(filters,
            "Apply one or more filters to get only phone numbers like present in one or more specified country."
        ));
      }

      case DATE -> {
        var useNow = ModelsUtils.createUseNow("(Optional) Use Date Generation:", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getUseNow()!=null) {
          useNow.setValue(fieldRepresentation.getUseNow());
        }
        listOfField.add(useNow);
        listOfOptionsForField.add(createInfoForAParameter(useNow,
            " To use or not the date when data is generated"));

        var minParam = ModelsUtils.createMinDate("(Optional) Minimum:", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getMinDateTime()!=null) {
          minParam.setValue(fieldRepresentation.getMinDateTime());
        }
        listOfField.add(minParam);
        listOfOptionsForField.add(createInfoForAParameter(minParam,
            " Optional minimum value. Default to 1970-01-01 00:00:00"));

        var maxParam = ModelsUtils.createMaxDate("(Optional) Maximum:", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getMaxDateTime()!=null) {
          maxParam.setValue(fieldRepresentation.getMaxDateTime());
        }
        listOfField.add(maxParam);
        listOfOptionsForField.add(createInfoForAParameter(maxParam,
            " Optional maximum value in format. Default to 2030-12-31 23:59:59"));

        var possibleValues = ModelsUtils.createList("(Optional) Possible values:", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getPossibleValuesWeighted()!=null) {
          possibleValues.setValue(fieldRepresentation.getPossibleValuesWeighted());
        }
        listOfField.add(possibleValues);
        listOfOptionsForField.add(ModelsUtils.createInfoForAParameter(possibleValues, possibleValues.getDescription()));
      }

      case DATE_AS_STRING -> {
        var useNow = ModelsUtils.createUseNow("(Optional) Use Date Generation:", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getUseNow()!=null) {
          useNow.setValue(fieldRepresentation.getUseNow());
        }
        listOfField.add(useNow);
        listOfOptionsForField.add(createInfoForAParameter(useNow,
            " To use or not the date when data is generated"));

        var pattern = ModelsUtils.createPatterDate("(Optional) Date Pattern:", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getPattern()!=null) {
          pattern.setValue(fieldRepresentation.getPattern());
        }
        listOfField.add(pattern);
        listOfOptionsForField.add(createInfoForAParameter(pattern,
            """
                Optional pattern on how date should be generated.
                Patterns are respecting standard Java ones described here:
                  https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html
                (Note that the output is cast to as String using this pattern).
                (Default to pattern: YYYY-MM-DD HH:MM:SS if not set)
                """));

        var minParam = ModelsUtils.createMinDate("(Optional) Minimum:", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getMinDateTime()!=null) {
          minParam.setValue(fieldRepresentation.getMinDateTime());
        }
        listOfField.add(minParam);
        listOfOptionsForField.add(createInfoForAParameter(minParam,
            " Optional minimum value in format of the pattern set above. Default to 1970-01-01 00:00:00"));

        var maxParam = ModelsUtils.createMaxDate("(Optional) Maximum:", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getMaxDateTime()!=null) {
          maxParam.setValue(fieldRepresentation.getMaxDateTime());
        }
        listOfField.add(maxParam);
        listOfOptionsForField.add(createInfoForAParameter(maxParam,
            " Optional maximum value in format of the pattern set above. Default to 2030-12-31 23:59:59"));

        var possibleValues = ModelsUtils.createList("(Optional) Possible values:", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getPossibleValuesWeighted()!=null) {
          possibleValues.setValue(fieldRepresentation.getPossibleValuesWeighted());
        }
        listOfField.add(possibleValues);
        listOfOptionsForField.add(ModelsUtils.createInfoForAParameter(possibleValues, possibleValues.getDescription()));
      }

      case STRING_REGEX -> {
        var regex = ModelsUtils.createRegex(" Regex:", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getRegex()!=null) {
          regex.setValue(fieldRepresentation.getRegex());
        }
        listOfField.add(regex);
        listOfOptionsForField.add(createInfoForAParameter(regex,
            """
                Give the regex that define what data should be generated.
                All characters are accepted and will be printed out.
                To insert a regex, it must be between [], and followed with a number between {} 
                  to determine the repetition of this expression.
                Inside the [], all values are accepted (including special characters), and must be separated by a ','.
                To make a range, 3 types of range are available: 
                  A-Z for upper, a-z for lower and 0-9 for numbers. (Range can be shortened to A-D for example)
                """));
      }

      case LOCAL_LLM -> {
        var modelFile = ModelsUtils.createFileField("Model File path", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getFile()!=null) {
          modelFile.setValue(fieldRepresentation.getFile());
        }
        listOfField.add(modelFile);
        listOfOptionsForField.add(ModelsUtils.createInfoForAParameter(modelFile,
            """
        Absolute path of the Model file in gguf format present on Datagen's machine.
          It will then load the model for data generation.
        Or URL to a file in a repository (such as Hugging Face).
         It will be downloaded from and used.
        """
        ));

        var request = ModelsUtils.createRequest("Request: ", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getRequest()!=null) {
          request.setValue(fieldRepresentation.getRequest());
        }
        listOfField.add(request);
        listOfOptionsForField.add(createInfoForAParameter(request,
            """
                What request to send to the LLM to generate wanted data.
                The request can include references to other columns values by using ${}.
                Example: 
                  'generate a one line birthday wish to ${name} who is ${age} years old today'.
                """));

        var context = ModelsUtils.createContext("(Optional) Context: ", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getContext()!=null) {
          context.setValue(fieldRepresentation.getContext());
        }
        listOfField.add(context);
        listOfOptionsForField.add(createInfoForAParameter(context,
            """
                To provide additional context to the LLM.
                Default to nothing.
                """));

        var temperature = ModelsUtils.createTemperature("(Optional) Temperature: ", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getTemperature()!=null) {
          temperature.setValue(fieldRepresentation.getTemperature().doubleValue());
        }
        listOfField.add(temperature);
        listOfOptionsForField.add(createInfoForAParameter(temperature,
            """
                To set the temperature for the LLM.
                Default to the one set in configuration, and if not set, to 1.0.
                """));

        var frequencyPenalty = ModelsUtils.createFrequencyPenalty("(Optional) Frequency Penalty: ", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getFrequencyPenalty()!=null) {
          frequencyPenalty.setValue(fieldRepresentation.getFrequencyPenalty().doubleValue());
        }
        listOfField.add(frequencyPenalty);
        listOfOptionsForField.add(createInfoForAParameter(frequencyPenalty,
            """
                To set the frequency penalty for the LLM.
                Default to the one set in configuration, and if not set, to 1.0.
                """));

        var presencePenalty = ModelsUtils.createPresencePenalty("(Optional) Presence Penalty: ", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getPresencePenalty()!=null) {
          presencePenalty.setValue(fieldRepresentation.getPresencePenalty().doubleValue());
        }
        listOfField.add(presencePenalty);
        listOfOptionsForField.add(createInfoForAParameter(presencePenalty,
            """
                To set the presence penalty for the LLM.
                Default to the one set in configuration, and if not set, to 1.0.
                """));

        var topP = ModelsUtils.createTopP("(Optional) topP: ", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getTopP()!=null) {
          topP.setValue(fieldRepresentation.getTopP().doubleValue());
        }
        listOfField.add(topP);
        listOfOptionsForField.add(createInfoForAParameter(topP,
            """
                To set the topP for the LLM.
                Default to the one set in configuration, and if not set, to 1.0.
                """));
      }


      case OLLAMA -> {
        var request = ModelsUtils.createRequest("Request: ", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getRequest()!=null) {
          request.setValue(fieldRepresentation.getRequest());
        }
        listOfField.add(request);
        listOfOptionsForField.add(createInfoForAParameter(request,
            """
                What request to send to the LLM to generate wanted data.
                The request can include references to other columns values by using ${}.
                Example: 
                  'generate a one line birthday wish to ${name} who is ${age} years old today'.
                """));

        var url = ModelsUtils.createURL("(Optional) Url: ", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getUrl()!=null) {
          url.setValue(fieldRepresentation.getUrl());
        }
        listOfField.add(url);
        listOfOptionsForField.add(createInfoForAParameter(url,
            """ 
        URL of the llama to use in format: http://hostname:port.
        Example: http://localhost:11434
        Default to the one set in configuration files.
        """));

        var modelType = ModelsUtils.createModelType("(Optional) Model: ", binder, "llama3");
        if(fieldRepresentation!=null && fieldRepresentation.getModelType()!=null) {
          modelType.setValue(fieldRepresentation.getModelType());
        }
        listOfField.add(modelType);
        listOfOptionsForField.add(createInfoForAParameter(modelType,
            """
                Name of the model to ask LLM to use 
                  (tested are llama3, mistral, phi3, moondream).
                Default to the one set in configuration, and if not set to 'llama3'.
                """));

        var context = ModelsUtils.createContext("(Optional) Context: ", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getContext()!=null) {
          context.setValue(fieldRepresentation.getContext());
        }
        listOfField.add(context);
        listOfOptionsForField.add(createInfoForAParameter(context,
            """
                To provide additional context to the LLM.
                Default to nothing.
                """));

        var temperature = ModelsUtils.createTemperature("(Optional) Temperature: ", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getTemperature()!=null) {
          temperature.setValue(fieldRepresentation.getTemperature().doubleValue());
        }
        listOfField.add(temperature);
        listOfOptionsForField.add(createInfoForAParameter(temperature,
            """
                To set the temperature for the LLM.
                Default to the one set in configuration, and if not set, to 1.0.
                """));

        var frequencyPenalty = ModelsUtils.createFrequencyPenalty("(Optional) Frequency Penalty: ", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getFrequencyPenalty()!=null) {
          frequencyPenalty.setValue(fieldRepresentation.getFrequencyPenalty().doubleValue());
        }
        listOfField.add(frequencyPenalty);
        listOfOptionsForField.add(createInfoForAParameter(frequencyPenalty,
            """
                To set the frequency penalty for the LLM.
                Default to the one set in configuration, and if not set, to 1.0.
                """));

        var presencePenalty = ModelsUtils.createPresencePenalty("(Optional) Presence Penalty: ", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getPresencePenalty()!=null) {
          presencePenalty.setValue(fieldRepresentation.getPresencePenalty().doubleValue());
        }
        listOfField.add(presencePenalty);
        listOfOptionsForField.add(createInfoForAParameter(presencePenalty,
            """
                To set the presence penalty for the LLM.
                Default to the one set in configuration, and if not set, to 1.0.
                """));

        var topP = ModelsUtils.createTopP("(Optional) topP: ", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getTopP()!=null) {
          topP.setValue(fieldRepresentation.getTopP().doubleValue());
        }
        listOfField.add(topP);
        listOfOptionsForField.add(createInfoForAParameter(topP,
            """
                To set the topP for the LLM.
                Default to the one set in configuration, and if not set, to 1.0.
                """));
      }

      case BEDROCK -> {
        var request = ModelsUtils.createRequest("Request: ", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getRequest()!=null) {
          request.setValue(fieldRepresentation.getRequest());
        }
        listOfField.add(request);
        listOfOptionsForField.add(createInfoForAParameter(request,
            """
                What request to send to the LLM to generate wanted data.
                The request can include references to other columns values by using ${}.
                Example: 
                  'generate a one line birthday wish to ${name} who is ${age} years old today'.
                """));

        var username = ModelsUtils.createUsername("(Optional) AWS Access Key ID: ", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getUser()!=null) {
          username.setValue(fieldRepresentation.getUser());
        }
        listOfField.add(username);
        listOfOptionsForField.add(createInfoForAParameter(username,
            """
                To authenticate against Bedrock.
                Default to the one set in configuration.
                """));

        var password = ModelsUtils.createPassword("(Optional) AWS Access Key Secret: ", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getPassword()!=null) {
          password.setValue(fieldRepresentation.getPassword());
        }
        listOfField.add(password);
        listOfOptionsForField.add(createInfoForAParameter(password,
            """
                To authenticate against Bedrock.
                Default to the one set in configuration.
                """));

        var modelType = ModelsUtils.createModelType("(Optional) Model: ", binder, "amazon.titan-text-lite-v1");
        if(fieldRepresentation!=null && fieldRepresentation.getModelType()!=null) {
          modelType.setValue(fieldRepresentation.getModelType());
        }
        listOfField.add(modelType);
        listOfOptionsForField.add(createInfoForAParameter(modelType,
            """
                Name of the model to ask LLM to use 
                  (tested are amazon.titan-text-lite-v1, meta.llama3-8b-instruct-v1:0).
                Default to the one set in configuration, and if not set to amazon.titan-text-lite-v1.
                List of all current model IDs can be found in AWS documentation:
                  https://docs.aws.amazon.com/bedrock/latest/userguide/model-ids.html
                """));

        var context = ModelsUtils.createContext("(Optional) Context: ", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getContext()!=null) {
          context.setValue(fieldRepresentation.getContext());
        }
        listOfField.add(context);
        listOfOptionsForField.add(createInfoForAParameter(context,
            """
                To provide additional context to the LLM.
                Default to nothing.
                """));

        var temperature = ModelsUtils.createTemperature("(Optional) Temperature: ", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getTemperature()!=null) {
          temperature.setValue(fieldRepresentation.getTemperature().doubleValue());
        }
        listOfField.add(temperature);
        listOfOptionsForField.add(createInfoForAParameter(temperature,
            """
                To set the temperature for the LLM.
                Default to the one set in configuration, and if not set, to 0.5.
                """));

        var maxTokens = ModelsUtils.createMaxTokens("(Optional) max Tokens: ", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getMaxTokens()!=null) {
          maxTokens.setValue(fieldRepresentation.getMaxTokens().doubleValue());
        }
        listOfField.add(maxTokens);
        listOfOptionsForField.add(createInfoForAParameter(maxTokens,
            """
                To set the max Tokens for the LLM.
                Default to the one set in configuration, and if not set, to 256.
                """));
      }

      case OPEN_AI -> {
        var request = ModelsUtils.createRequest("Request: ", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getRequest()!=null) {
          request.setValue(fieldRepresentation.getRequest());
        }
        listOfField.add(request);
        listOfOptionsForField.add(createInfoForAParameter(request,
            """
                What request to send to the LLM to generate wanted data.
                The request can include references to other columns values by using ${}.
                Example: 'generate a one line birthday wish to ${name} who is ${age} years old today'.
                """));

        var password = ModelsUtils.createPassword("(Optional) OpenAI Token: ", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getPassword()!=null) {
          password.setValue(fieldRepresentation.getPassword());
        }
        listOfField.add(password);
        listOfOptionsForField.add(createInfoForAParameter(password,
            """
                To authenticate to OpenAI API.
                Default to the one set in configuration.
                """));

        var modelType = ModelsUtils.createModelType("(Optional) Model: ", binder, "gpt-4o");
        if(fieldRepresentation!=null && fieldRepresentation.getModelType()!=null) {
          modelType.setValue(fieldRepresentation.getModelType());
        }
        listOfField.add(modelType);
        listOfOptionsForField.add(createInfoForAParameter(modelType,
            """
                Name of the model to ask LLM to use (tested are gpt-4-32k, gpt-4o).
                Default to the one set in configuration, and if not set to gpt-4-32k.
                """));

        var context = ModelsUtils.createContext("(Optional) Context: ", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getContext()!=null) {
          context.setValue(fieldRepresentation.getContext());
        }
        listOfField.add(context);
        listOfOptionsForField.add(createInfoForAParameter(context,
            """
                To provide additional context to the LLM.
                Default to nothing.
                """));

        var temperature = ModelsUtils.createTemperature("(Optional) Temperature: ", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getTemperature()!=null) {
          temperature.setValue(fieldRepresentation.getTemperature().doubleValue());
        }
        listOfField.add(temperature);
        listOfOptionsForField.add(createInfoForAParameter(temperature,
            """
                To set the temperature for the LLM.
                Default to the one set in configuration, and if not set, to 0.5.
                """));

        var frequencyPenalty = ModelsUtils.createFrequencyPenalty("(Optional) Frequency Penalty: ", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getFrequencyPenalty()!=null) {
          frequencyPenalty.setValue(fieldRepresentation.getFrequencyPenalty().doubleValue());
        }
        listOfField.add(frequencyPenalty);
        listOfOptionsForField.add(createInfoForAParameter(frequencyPenalty,
            """
                To set the frequency penalty for the LLM.
                Default to the one set in configuration, and if not set, to 1.0.
                """));

        var presencePenalty = ModelsUtils.createPresencePenalty("(Optional) Presence Penalty: ", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getPresencePenalty()!=null) {
          presencePenalty.setValue(fieldRepresentation.getPresencePenalty().doubleValue());
        }
        listOfField.add(presencePenalty);
        listOfOptionsForField.add(createInfoForAParameter(presencePenalty,
            """
                To set the presence penalty for the LLM.
                Default to the one set in configuration, and if not set, to 1.0.
                """));

        var topP = ModelsUtils.createTopP("(Optional) topP: ", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getTopP()!=null) {
          topP.setValue(fieldRepresentation.getTopP().doubleValue());
        }
        listOfField.add(topP);
        listOfOptionsForField.add(createInfoForAParameter(topP,
            """
                To set the topP for the LLM.
                Default to the one set in configuration, and if not set, to 1.0.
                """));

        var maxTokens = ModelsUtils.createMaxTokens("(Optional) max Tokens: ", binder);
        if(fieldRepresentation!=null && fieldRepresentation.getMaxTokens()!=null) {
          maxTokens.setValue(fieldRepresentation.getMaxTokens().doubleValue());
        }
        listOfField.add(maxTokens);
        listOfOptionsForField.add(createInfoForAParameter(maxTokens,
            """
                To set the max Tokens for the LLM.
                Default to the one set in configuration, and if not set, to 256.
                """));
      }
      }

      listOfOptionsForField.forEach(f -> formLayout.add(f, 3));
    });

    // If it is an imported column, set its value
    if(fieldRepresentation!=null) {
      comboBox.setValue(fieldRepresentation.getType());
    }

    // Create final details component containing all information of the column
    Details details = new Details(summary, formLayout);
    details.setOpened(true);
    details.setSizeFull();
    details.getStyle().setBorder("2px dashed #e6ecf4");
    details.getStyle().setBorderRadius("1%");
    removeButton.addClickListener(e -> {
      binders.remove(binder);
      parentLayout.remove(details);
    });



    return details;
  }

  /**
   * Create all optional configurations for Connectors
   * @return
   */
  private Details connectorsConfigs() {
    var mainDetails = new Details("(Optional) Connectors Configuration: ");
    mainDetails.setWidthFull();
    var genDetails = new Details("Generic");
    genDetails.add(
        List.of(
            // Generic Options
            createInfoForAParameter(
                createGenericBooleanOptionProps("Delete Previous", true,
                    OptionsConverter.Options.DELETE_PREVIOUS, optionsPropsBinder),
                "To delete previous data generated"),
            createInfoForAParameter(
                createGenericBooleanOptionProps("One File Per Iteration", true,
                    OptionsConverter.Options.ONE_FILE_PER_ITERATION, optionsPropsBinder),
                "To have one file foreach batch or one file for entire generation")
        )
    );

    var localDetails = new Details("Local File");
    localDetails.add(
        List.of(
            createInfoForAParameter(
                createGenericStringTableNamesProps("Local File Path", null,
                    OptionsConverter.TableNames.LOCAL_FILE_PATH, tableNamesPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericStringTableNamesProps("Local File Name", null,
                    OptionsConverter.TableNames.LOCAL_FILE_NAME, tableNamesPropsBinder),
                "")
        )
    );

    var csvDetails = new Details("CSV");
    csvDetails.add(
        // CSV
        createInfoForAParameter(
            createGenericBooleanOptionProps("CSV Header", true,
                OptionsConverter.Options.CSV_HEADER, optionsPropsBinder),
            "To set a CSV Header or not in CSV files")
    );

    var parquetDetails = new Details("Parquet");
    parquetDetails.add(List.of(
        // Parquet
        createInfoForAParameter(
            createGenericIntegerOptionProps("Parquet Page Size", null,
                OptionsConverter.Options.PARQUET_PAGE_SIZE, optionsPropsBinder),
            "Size of Parquet Pages for Parquet Files"),
        createInfoForAParameter(
            createGenericIntegerOptionProps("Parquet Row Group Size", null,
                OptionsConverter.Options.PARQUET_ROW_GROUP_SIZE, optionsPropsBinder),
            "Size of Parquet Row Group for Parquet Files"),
        createInfoForAParameter(
            createGenericIntegerOptionProps("Parquet Dictionary Page Size", null,
                OptionsConverter.Options.PARQUET_DICTIONARY_PAGE_SIZE, optionsPropsBinder),
            "Size of Parquet Dictionary for Parquet Files"),
        createInfoForAParameter(
            createGenericBooleanOptionProps("Parquet Dictionary Encoding", true,
                OptionsConverter.Options.PARQUET_DICTIONARY_ENCODING, optionsPropsBinder),
            "To encode or not Parquet dictionary for Parquet Files")
    ));

    var hdfsDetails = new Details("HDFS");
    hdfsDetails.add(
        List.of(
            // HDFS
            createInfoForAParameter(
                createGenericStringTableNamesProps("HDFS File Name", null,
                    OptionsConverter.TableNames.HDFS_FILE_NAME, tableNamesPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericStringTableNamesProps("HDFS File Path", null,
                    OptionsConverter.TableNames.HDFS_FILE_PATH, tableNamesPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericIntegerOptionProps("HDFS Replication Factor", null,
                    OptionsConverter.Options.HDFS_REPLICATION_FACTOR, optionsPropsBinder),
                "")
        )
    );

    var ozoneDetails = new Details("Ozone");
    ozoneDetails.add(
        List.of(
            // OZONE
            createInfoForAParameter(
                createGenericStringTableNamesProps("Ozone Volume", null,
                    OptionsConverter.TableNames.OZONE_VOLUME, tableNamesPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericStringTableNamesProps("Ozone Bucket", null,
                    OptionsConverter.TableNames.OZONE_BUCKET, tableNamesPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericStringTableNamesProps("Ozone Key name", null,
                    OptionsConverter.TableNames.OZONE_KEY_NAME, tableNamesPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericStringTableNamesProps("Ozone Local File Path", null,
                    OptionsConverter.TableNames.OZONE_LOCAL_FILE_PATH, tableNamesPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericIntegerOptionProps("Ozone Replication Factor", null,
                    OptionsConverter.Options.OZONE_REPLICATION_FACTOR, optionsPropsBinder),
                "")
        )
    );

    var s3Details = new Details("S3");
    s3Details.add(
        List.of(
            // S3
            createInfoForAParameter(
                createGenericStringTableNamesProps("S3 Bucket", null,
                    OptionsConverter.TableNames.S3_BUCKET, tableNamesPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericStringTableNamesProps("S3 Directory", null,
                    OptionsConverter.TableNames.S3_DIRECTORY, tableNamesPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericStringTableNamesProps("S3 Key name", null,
                    OptionsConverter.TableNames.S3_KEY_NAME, tableNamesPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericStringTableNamesProps("S3 Local File Path", null,
                    OptionsConverter.TableNames.S3_LOCAL_FILE_PATH, tableNamesPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericStringTableNamesProps("S3 Region", null,
                    OptionsConverter.TableNames.S3_REGION, tableNamesPropsBinder),
                "S3 Region as described by AWS here: https://docs.aws.amazon.com/general/latest/gr/rande.html#regional-endpoints"),
            createInfoForAParameter(
                createGenericStringTableNamesProps("S3 Access Key ID", null,
                    OptionsConverter.TableNames.S3_ACCESS_KEY_ID, tableNamesPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericStringTableNamesProps("S3 Access Key Secret", null,
                    OptionsConverter.TableNames.S3_ACCESS_KEY_SECRET, tableNamesPropsBinder),
                "")
        )
    );

    var adlsDetails = new Details("ADLS");
    adlsDetails.add(
        List.of(
            // ADLS
            createInfoForAParameter(
                createGenericStringTableNamesProps("ADLS container", null,
                    OptionsConverter.TableNames.ADLS_CONTAINER, tableNamesPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericStringTableNamesProps("ADLS Directory", null,
                    OptionsConverter.TableNames.ADLS_DIRECTORY, tableNamesPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericStringTableNamesProps("ADLS File Name", null,
                    OptionsConverter.TableNames.ADLS_FILE_NAME, tableNamesPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericStringTableNamesProps("ADLS Local File path", null,
                    OptionsConverter.TableNames.ADLS_LOCAL_FILE_PATH, tableNamesPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericIntegerOptionProps("ADLS Block Size", null,
                    OptionsConverter.Options.ADLS_BLOCK_SIZE, optionsPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericIntegerOptionProps("ADLS Max Upload Size", null,
                    OptionsConverter.Options.ADLS_MAX_UPLOAD_SIZE, optionsPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericIntegerOptionProps("ADLS Max Concurrency", null,
                    OptionsConverter.Options.ADLS_MAX_CONCURRENCY, optionsPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericStringTableNamesProps("ADLS Account Name", null,
                    OptionsConverter.TableNames.ADLS_ACCOUNT_NAME, tableNamesPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericComboStringOptionProps("ADLS Account Type", null,
                    OptionsConverter.TableNames.ADLS_ACCOUNT_TYPE, tableNamesPropsBinder, "dfs", "blob"),
                "ADLS account type is either dfs or blob depending on storage type"),
            createInfoForAParameter(
                createGenericStringTableNamesProps("ADLS SAS Token", null,
                    OptionsConverter.TableNames.ADLS_SAS_TOKEN, tableNamesPropsBinder),
                "")
        )
    );

    var gcsDetails = new Details("GCS");
    gcsDetails.add(
        List.of(
            // GCS
            createInfoForAParameter(
                createGenericStringTableNamesProps("GCS Bucket", null,
                    OptionsConverter.TableNames.GCS_BUCKET, tableNamesPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericStringTableNamesProps("GCS Directory", null,
                    OptionsConverter.TableNames.GCS_DIRECTORY, tableNamesPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericStringTableNamesProps("GCS Object name", null,
                    OptionsConverter.TableNames.GCS_OBJECT_NAME, tableNamesPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericStringTableNamesProps("GCS Local File Path", null,
                    OptionsConverter.TableNames.GCS_LOCAL_FILE_PATH, tableNamesPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericStringTableNamesProps("GCS Region", null,
                    OptionsConverter.TableNames.GCS_REGION, tableNamesPropsBinder),
                "GCS Region in format described by Google: https://cloud.google.com/storage/docs/locations "),
            createInfoForAParameter(
                createGenericStringTableNamesProps("GCS Project ID", null,
                    OptionsConverter.TableNames.GCS_PROJECT_ID, tableNamesPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericStringTableNamesProps("GCS Account key Path", null,
                    OptionsConverter.TableNames.GCS_ACCOUNT_KEY_PATH, tableNamesPropsBinder),
                "")
        )
    );

    var hiveDetails = new Details("Hive");
    hiveDetails.add(
        List.of(
            createInfoForAParameter(
                createGenericStringTableNamesProps("Hive Database", null,
                    OptionsConverter.TableNames.HIVE_DATABASE, tableNamesPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericStringTableNamesProps("Hive Table Name", null,
                    OptionsConverter.TableNames.HIVE_TABLE_NAME, tableNamesPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericStringTableNamesProps("Hive Temporary Table Name", null,
                    OptionsConverter.TableNames.HIVE_TEMPORARY_TABLE_NAME, tableNamesPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericStringTableNamesProps("Hive HDFS File Path", null,
                    OptionsConverter.TableNames.HIVE_HDFS_FILE_PATH, tableNamesPropsBinder),
                ""),
            // Hive
            createInfoForAParameter(
                createGenericIntegerOptionProps("Hive Thread Number", null,
                    OptionsConverter.Options.HIVE_THREAD_NUMBER, optionsPropsBinder),
                "Number of threads to parallelize insertions for Hive if using direct SQL statements"),
            createInfoForAParameter(
                createGenericComboStringOptionProps("Hive Table Type", "EXTERNAL",
                    OptionsConverter.Options.HIVE_TABLE_TYPE, optionsPropsBinder,
                    "EXTERNAL", "MANAGED", "ICEBERG"),
                ""),
            createInfoForAParameter(
                createGenericComboStringOptionProps("Hive Table Format", "PARQUET",
                    OptionsConverter.Options.HIVE_TABLE_FORMAT, optionsPropsBinder,
                    "PARQUET", "ORC", "AVRO", "CSV", "JSON"),
                ""),
            createInfoForAParameter(
                createGenericBooleanOptionProps("Hive on HDFS", true,
                    OptionsConverter.Options.HIVE_ON_HDFS, optionsPropsBinder),
                "To use HDFS files for inserting data (instead of SQL statements)"),
            createInfoForAParameter(
                createGenericStringOptionProps("Hive Queue Name", null,
                    OptionsConverter.Options.HIVE_TEZ_QUEUE_NAME, optionsPropsBinder),
                "Queue Name set for Hive queries launched for data insertion"),
            createInfoForAParameter(
                createGenericStringOptionProps("Hive Partition Columns", null,
                    OptionsConverter.Options.HIVE_TABLE_PARTITIONS_COLS, optionsPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericStringOptionProps("Hive Bucket Columns", null,
                    OptionsConverter.Options.HIVE_TABLE_BUCKETS_COLS, optionsPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericIntegerOptionProps("Hive Number of Buckets", null,
                    OptionsConverter.Options.HIVE_TABLE_BUCKETS_NUMBER, optionsPropsBinder),
                "")
        )
    );

    var hbaseDetails = new Details("HBase");
    hbaseDetails.add(
        List.of(
            createInfoForAParameter(
                createGenericStringTableNamesProps("HBase Table Name", null,
                    OptionsConverter.TableNames.HBASE_TABLE_NAME, tableNamesPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericStringTableNamesProps("HBase Namespace", null,
                    OptionsConverter.TableNames.HBASE_NAMESPACE, tableNamesPropsBinder),
                ""),
            // HBase
            createInfoForAParameter(
                createGenericStringOptionProps("HBase Primary Key", null,
                    OptionsConverter.Options.HBASE_PRIMARY_KEY, optionsPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericStringOptionProps("HBase Column Families Mapping", null,
                    OptionsConverter.Options.HBASE_COLUMN_FAMILIES_MAPPING, optionsPropsBinder),
                "")
        )
    );

    var kafkaDetails = new Details("Kafka");
    kafkaDetails.add(
        List.of(
            createInfoForAParameter(
                createGenericStringTableNamesProps("Kafka Topic", null,
                    OptionsConverter.TableNames.KAFKA_TOPIC, tableNamesPropsBinder),
                ""),
            //Kafka
            createInfoForAParameter(
                createGenericStringOptionProps("Kafka Message Key", null,
                    OptionsConverter.Options.KAFKA_MSG_KEY, optionsPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericStringOptionProps("Kafka Acks", null,
                    OptionsConverter.Options.KAFKA_ACKS_CONFIG, optionsPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericComboStringOptionProps("Kafka Number of Retries", null,
                    OptionsConverter.Options.KAFKA_RETRIES_CONFIG, optionsPropsBinder,"ALL", "1", "2", "3", "0"),
                ""),
            createInfoForAParameter(
                createGenericStringOptionProps("Kafka JaaS File Path", null,
                    OptionsConverter.Options.KAFKA_JAAS_FILE_PATH, optionsPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericIntegerOptionProps("Kafka Replication Factor", null,
                    OptionsConverter.Options.KAFKA_REPLICATION_FACTOR, optionsPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericIntegerOptionProps("Kafka Number of Partitions", null,
                    OptionsConverter.Options.KAFKA_PARTITIONS_NUMBER, optionsPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericComboStringOptionProps("Kafka Message Type", "JSON",
                    OptionsConverter.Options.KAFKA_MESSAGE_TYPE, optionsPropsBinder, "JSON", "AVRO", "CSV"),
                "")
        )
    );

    var kuduDetails = new Details("Kudu");
    kuduDetails.add(
        List.of(
            createInfoForAParameter(
                createGenericStringTableNamesProps("Kudu Table Name", null,
                    OptionsConverter.TableNames.KUDU_TABLE_NAME, tableNamesPropsBinder),
                ""),
            // Kudu
            createInfoForAParameter(
                createGenericIntegerOptionProps("Kudu Number of Buckets", null,
                    OptionsConverter.Options.KUDU_BUCKETS, optionsPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericIntegerOptionProps("Kudu Buffer Size", null,
                    OptionsConverter.Options.KUDU_BUFFER, optionsPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericComboStringOptionProps("Kudu Flush Type", null,
                    OptionsConverter.Options.KUDU_FLUSH, optionsPropsBinder,"AUTO_FLUSH_SYNC", "AUTO_FLUSH_BACKGROUND", "MANUAL_FLUSH"),
                ""),
            createInfoForAParameter(
                createGenericIntegerOptionProps("Kudu Replicas", null,
                    OptionsConverter.Options.KUDU_REPLICAS, optionsPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericStringOptionProps("Kudu Hash Keys", null,
                    OptionsConverter.Options.KUDU_HASH_KEYS, optionsPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericStringOptionProps("Kudu Range Keys", null,
                    OptionsConverter.Options.KUDU_RANGE_KEYS, optionsPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericStringOptionProps("Kudu Primary Keys", null,
                    OptionsConverter.Options.KUDU_PRIMARY_KEYS, optionsPropsBinder),
                "")
        )
    );

    var solrDetails = new Details("SolR");
    solrDetails.add(
        List.of(
            createInfoForAParameter(
                createGenericStringTableNamesProps("SolR Collection", null,
                    OptionsConverter.TableNames.SOLR_COLLECTION, tableNamesPropsBinder),
                ""),
          //SolR
            createInfoForAParameter(
                createGenericIntegerOptionProps("SolR Shards", null,
                    OptionsConverter.Options.SOLR_SHARDS, optionsPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericIntegerOptionProps("SolR Replicas", null,
                    OptionsConverter.Options.SOLR_REPLICAS, optionsPropsBinder),
                ""),
            createInfoForAParameter(
                createGenericStringOptionProps("SOLR JaaS File Path", null,
                    OptionsConverter.Options.SOLR_JAAS_FILE_PATH, optionsPropsBinder),
                "")
        )
    );

    mainDetails.add(
        List.of(
            genDetails,
            localDetails,
            csvDetails,
            parquetDetails,
            hdfsDetails,
            ozoneDetails,
            s3Details,
            adlsDetails,
            gcsDetails,
            hiveDetails,
            hbaseDetails,
            kafkaDetails,
            kuduDetails,
            solrDetails
        )
    );
    return mainDetails;
  }


}
