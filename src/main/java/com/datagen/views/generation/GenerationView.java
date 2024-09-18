package com.datagen.views.generation;

import com.datagen.config.ConnectorParser;
import com.datagen.model.Model;
import com.datagen.model.OptionsConverter;
import com.datagen.service.command.Command;
import com.datagen.service.command.CommandRunnerService;
import com.datagen.service.credentials.Credentials;
import com.datagen.service.credentials.CredentialsService;
import com.datagen.service.model.ModelStoreService;
import com.datagen.utils.Utils;
import com.datagen.views.MainLayout;
import com.vaadin.flow.component.Component;
import com.vaadin.flow.component.Composite;
import com.vaadin.flow.component.HasText;
import com.vaadin.flow.component.button.Button;
import com.vaadin.flow.component.button.ButtonVariant;
import com.vaadin.flow.component.combobox.ComboBox;
import com.vaadin.flow.component.combobox.MultiSelectComboBox;
import com.vaadin.flow.component.dialog.Dialog;
import com.vaadin.flow.component.formlayout.FormLayout;
import com.vaadin.flow.component.html.NativeLabel;
import com.vaadin.flow.component.html.Span;
import com.vaadin.flow.component.notification.Notification;
import com.vaadin.flow.component.notification.NotificationVariant;
import com.vaadin.flow.component.orderedlayout.HorizontalLayout;
import com.vaadin.flow.component.orderedlayout.VerticalLayout;
import com.vaadin.flow.component.progressbar.ProgressBar;
import com.vaadin.flow.component.progressbar.ProgressBarVariant;
import com.vaadin.flow.data.binder.Binder;
import com.vaadin.flow.data.binder.ValidationException;
import com.vaadin.flow.router.PageTitle;
import com.vaadin.flow.router.Route;
import com.vaadin.flow.theme.lumo.LumoUtility;
import jakarta.annotation.security.PermitAll;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;
import java.util.stream.Collectors;

import static com.datagen.views.generation.GenerationUtils.*;

@Slf4j
@PageTitle("Data Generation")
@Route(value = "generation", layout = MainLayout.class)
@PermitAll
public class GenerationView extends Composite<VerticalLayout> {

    @Autowired
    private ModelStoreService modelStoreService;
    @Autowired
    private CommandRunnerService commandRunnerService;
    @Autowired
    private CredentialsService credentialsService;

    private Binder<Model> binderModel;
    private Binder<List<Credentials>> binderCredentials;
    @Getter @Setter
    private Model modelChosen;

    @Getter @Setter
    protected class InternalCommandLaunch {
        ConnectorParser.Connector connectorChosen;
        int threads;
        long numberOfBatches;
        long rowsPerBatch;
    }

    private Binder<InternalCommandLaunch> binderCommand;

    @Autowired
    public GenerationView(ModelStoreService modelStoreService, CommandRunnerService commandRunnerService, CredentialsService credentialsService) {
        this.binderModel = new Binder<>();
        this.binderCommand = new Binder<>();
        this.binderCredentials = new Binder<>();
        this.modelStoreService = modelStoreService;
        this.commandRunnerService = commandRunnerService;
        this.credentialsService = credentialsService;

        VerticalLayout layoutColumn = new VerticalLayout();
        layoutColumn.setWidth("100%");
        layoutColumn.getStyle().set("flex-grow", "1");

        var formLayout = new FormLayout();

        var launchButton = launchDataGenerationButton();
        var cbConnector = createConnectorListButton(formLayout, launchButton);
        var cbModel = createModelListButton(cbConnector);

        formLayout.add(cbModel, cbConnector);

        var threadsEntry = createThreadsField(this.binderCommand);
        var batchesEntry = createBatchesField(this.binderCommand);
        var rowsEntry = createRowsField(this.binderCommand);

        var hlLayout = new HorizontalLayout();
        hlLayout.add(batchesEntry, rowsEntry, threadsEntry);

        var credentialslayout = createCredentialsButton();

        layoutColumn.add(formLayout);
        layoutColumn.add(hlLayout);
        layoutColumn.add(credentialslayout);
        layoutColumn.add(launchButton);

        getContent().add(layoutColumn);

    }

    /**
     * Create a model list button
     * @return
     */
    private ComboBox<Model> createModelListButton(ComboBox<ConnectorParser.Connector> cbConnector) {
        var comboBox = new ComboBox<Model>("Model:");
        comboBox.setItems(modelStoreService.listModelsAsModels());
        comboBox.setItemLabelGenerator(Model::getName);
        comboBox.setRequired(true);
        comboBox.setHelperText("Select what kind of data to generate using existing models");
        comboBox.addValueChangeListener(e -> {
            this.modelChosen = e.getValue();
            cbConnector.setVisible(true);
        });
        return comboBox;
    }

    /**
     * Createa multi select button to choose credentials
     * @return
     */
    private MultiSelectComboBox<Credentials> createCredentialsButton() {
        var comboBox = new MultiSelectComboBox<Credentials>("Credentials:");
        comboBox.setItems(credentialsService.listCredentialsMeta());
        comboBox.setItemLabelGenerator(Credentials::getName);
        comboBox.setRequired(false);
        comboBox.setHelperText("Select one or multiple credentials stored to use for data generation");
        this.binderCredentials.forField(comboBox)
            .bind(HashSet::new, List::addAll);
        return comboBox;
    }

    /**
     * Creates a launch data generation button
     * @return
     */
    private Button launchDataGenerationButton() {
        var button = new Button("Generate Data");
        button.addThemeVariants(ButtonVariant.LUMO_PRIMARY);
        button.setEnabled(false);
        button.addClickListener(e -> {
            log.debug("Asked to make data generation");

            // Bind Data
            var command = new InternalCommandLaunch();
            var credsList = new ArrayList<Credentials>();
            try {
                this.binderModel.writeBean(this.modelChosen);
                this.binderCredentials.writeBean(credsList);
                this.binderCommand.writeBean(command);
            } catch (ValidationException exception) {
                log.warn("Fields cannot be validated, user must re-take actions ");
            }

            var modelStored = modelStoreService.getModelAsModelStored(this.modelChosen.getName());
            if(modelStored==null) {
                log.warn("Model cannot be retrieved, hence cannot launch generation");
                Notification notification = Notification.show(
                    "Cannot find model, refresh page and retry, otherwise check logs for more information" , 5000,
                    Notification.Position.TOP_CENTER);
                notification.addThemeVariants(NotificationVariant.LUMO_ERROR);
            } else {
                var commandId =
                    this.commandRunnerService.generateData(modelStored,
                        command.getThreads(), command.getNumberOfBatches(),
                        command.getRowsPerBatch(),
                        null, null,
                        List.of(command.getConnectorChosen().name()), null,
                        credsList);

                Notification notification = Notification.show(
                    "Launch Data Generation with ID: " + commandId, 5000,
                    Notification.Position.TOP_CENTER);
                notification.addThemeVariants(NotificationVariant.LUMO_SUCCESS);

                progressBarOfCommand(commandId, modelStored.getName(),
                    List.of(command.getConnectorChosen().name()),
                    command.getNumberOfBatches() * command.getRowsPerBatch());
            }

        });
        return button;
    }

    /**
     * Create a progress bar for a command, with an update button to get status
     * @param commandId
     * @param modelName
     * @param connectors
     * @param numberOfRows
     * @return
     */
    private ProgressBar progressBarOfCommand(UUID commandId, String modelName, List<String> connectors, Long numberOfRows) {
        var dialog = new Dialog("Command ID: " + commandId);
        var closeButton = new Button("Close", eventClose -> dialog.close());

        var span = new Span(
            "Model: " + modelName + System.lineSeparator() +
                "To: " + connectors.stream().collect(Collectors.joining(", ")) + System.lineSeparator() +
                "Number of Rows: " + Utils.formatNumber(numberOfRows) + System.lineSeparator()
            );
        span.setWhiteSpace(HasText.WhiteSpace.PRE);
        var progressBar = new ProgressBar();
        progressBar.setMin(0);
        progressBar.setMax(100);
        progressBar.setValue(0);

        var progressBarLabel = new NativeLabel("Progress: ");
        progressBarLabel.setId("pblbl");
        progressBarLabel.addClassName(LumoUtility.TextColor.SECONDARY);
        progressBar.getElement().setAttribute("aria-labelledby", "pblbl");

        var refreshButton = new Button("Refresh", event -> updateProgressBar(commandId, progressBar, progressBarLabel));

        dialog.add(span);
        dialog.add(progressBarLabel, progressBar);
        dialog.getFooter().add(closeButton);
        dialog.getFooter().add(refreshButton);
        dialog.open();

        return progressBar;
    }


    /**
     * Update a given progress bar for a command, by getting its status and progress
     * @param commandId
     * @param progressBar
     */
    private void updateProgressBar(UUID commandId, ProgressBar progressBar, NativeLabel progressMade) {
        var commandStatus  = this.commandRunnerService.getCommandStatusShort(commandId);
        var progress  = commandStatus.getProgress();
        progressMade.setText("Progress: " + Math.round(progress) + " %");
            progressBar.setValue(progress);
                if(commandStatus.getStatus()== Command.CommandStatus.FAILED){
                    progressBar.addThemeVariants(ProgressBarVariant.LUMO_ERROR);
                } else if (commandStatus.getStatus()== Command.CommandStatus.FINISHED) {
                    progressBar.addThemeVariants(ProgressBarVariant.LUMO_SUCCESS);
                }
    }


    /**
     * Create a connector list button
     * @return
     */
    private ComboBox<ConnectorParser.Connector> createConnectorListButton(FormLayout formLayout, Button launchButton) {
        var comboBox = new ComboBox<ConnectorParser.Connector>("Connector:");
        comboBox.setItems(EnumSet.allOf(ConnectorParser.Connector.class).stream().toList());
        comboBox.setRequired(true);
        comboBox.setVisible(false);
        comboBox.setHelperText("Select where to generate data among existing connectors");

        this.binderCommand.forField(comboBox)
            .bind(InternalCommandLaunch::getConnectorChosen, InternalCommandLaunch::setConnectorChosen);

        // Depending on the type of the field, there should be different printed values
        var listOfParametersForConnector = new LinkedList<Component>();

        comboBox.addValueChangeListener(e -> {
            formLayout.remove(listOfParametersForConnector);
            listOfParametersForConnector.clear();

            var model = modelStoreService.getModel(this.modelChosen.getName());
            Map<OptionsConverter.TableNames, String> tableNamesProps = model.getTableNames();
            Map<OptionsConverter.Options, Object> optionsProps = model.getOptions();

            if(e.getValue()!=null) {
                launchButton.setEnabled(true);
                listOfParametersForConnector.addAll(getComponentListForAConnector(e.getValue(), binderModel, tableNamesProps, optionsProps));
                listOfParametersForConnector.forEach(f -> formLayout.add(f, 1));
            } else {
                launchButton.setEnabled(false);
            }
        });

        return comboBox;
    }

    /**
     * For a given connector, return the list of components to configure it
     * @param c
     * @param tableNamesProps
     * @param optionsProps
     * @return
     */
    public static List<Component> getComponentListForAConnector(ConnectorParser.Connector c,
                                                         Binder<Model> binderModel,
                                                         Map<OptionsConverter.TableNames, String> tableNamesProps,
                                                         Map<OptionsConverter.Options, Object> optionsProps) {
        switch (c) {

        /**
         Local Files
         */
        case CSV -> {
            var pathText = createLocalPath(binderModel, tableNamesProps);
            var nameText = createLocalName(binderModel, tableNamesProps);
            // Optional
            var oneFilePerBatch = createOneFilePerIteration(binderModel, optionsProps);
            var deletePrevious = createDeletePrevious(binderModel, optionsProps);
            var appendHeader = createCsvHeader(binderModel, optionsProps);
            var details = createOptionalConfigs(oneFilePerBatch, deletePrevious, appendHeader);
            return List.of(pathText, nameText, details);
        }

        case JSON, ORC, AVRO -> {
            var pathText = createLocalPath(binderModel, tableNamesProps);
            var nameText = createLocalName(binderModel, tableNamesProps);
            // Optional
            var oneFilePerBatch = createOneFilePerIteration(binderModel, optionsProps);
            var deletePrevious = createDeletePrevious(binderModel, optionsProps);
            var details = createOptionalConfigs(oneFilePerBatch, deletePrevious);
            return List.of(pathText, nameText, details);
        }

        case PARQUET -> {
            var pathText = createLocalPath(binderModel, tableNamesProps);
            var nameText = createLocalName(binderModel, tableNamesProps);
            // Optional
            var oneFilePerBatch = createOneFilePerIteration(binderModel, optionsProps);
            var deletePrevious = createDeletePrevious(binderModel, optionsProps);
            var parquetGroupSize = createParquetPageSize(binderModel, optionsProps);
            var parquetRowGroupSize = createParquetRowGroupSize(binderModel, optionsProps);
            var parquetDicSize = createParquetDictionaryPageSize(binderModel, optionsProps);
            var parquetEncoding = createParquetDictionaryEncoding(binderModel, optionsProps);
            var details = createOptionalConfigs(oneFilePerBatch, deletePrevious, parquetGroupSize, parquetRowGroupSize, parquetDicSize, parquetEncoding);
            return List.of(pathText, nameText, details);
        }

        /**
         HDFS Files
         */

        case HDFS_CSV -> {
            var pathText = createHdfsPath(binderModel, tableNamesProps);
            var nameText = createHdfsName(binderModel, tableNamesProps);
            // Optional
            var oneFilePerBatch = createOneFilePerIteration(binderModel, optionsProps);
            var deletePrevious = createDeletePrevious(binderModel, optionsProps);
            var appendHeader = createCsvHeader(binderModel, optionsProps);
            var hdfsRepFactor = createHdfsReplicationFactor(binderModel, optionsProps);
            var details = createOptionalConfigs(oneFilePerBatch, deletePrevious, appendHeader, hdfsRepFactor);
            return List.of(pathText, nameText, details);
        }

        case HDFS_JSON, HDFS_AVRO, HDFS_ORC -> {
            var pathText = createHdfsPath(binderModel, tableNamesProps);
            var nameText = createHdfsName(binderModel, tableNamesProps);
            // Optional
            var oneFilePerBatch = createOneFilePerIteration(binderModel, optionsProps);
            var deletePrevious = createDeletePrevious(binderModel, optionsProps);
            var hdfsRepFactor = createHdfsReplicationFactor(binderModel, optionsProps);
            var details = createOptionalConfigs(oneFilePerBatch, deletePrevious, hdfsRepFactor);
            return List.of(pathText, nameText, details);
        }

        case HDFS_PARQUET -> {
            var pathText = createHdfsPath(binderModel, tableNamesProps);
            var nameText = createHdfsName(binderModel, tableNamesProps);
            // Optional
            var oneFilePerBatch = createOneFilePerIteration(binderModel, optionsProps);
            var deletePrevious = createDeletePrevious(binderModel, optionsProps);
            var parquetGroupSize = createParquetPageSize(binderModel, optionsProps);
            var parquetRowGroupSize = createParquetRowGroupSize(binderModel, optionsProps);
            var parquetDicSize = createParquetDictionaryPageSize(binderModel, optionsProps);
            var parquetEncoding = createParquetDictionaryEncoding(binderModel, optionsProps);
            var hdfsRepFactor = createHdfsReplicationFactor(binderModel, optionsProps);
            var details = createOptionalConfigs(oneFilePerBatch, deletePrevious, parquetGroupSize, parquetRowGroupSize, parquetDicSize, parquetEncoding, hdfsRepFactor);
            return List.of(pathText, nameText, details);
        }

        /**
         Ozone Files
         */

        case OZONE_CSV -> {
            var volume = createOzoneVolume(binderModel, tableNamesProps);
            var bucket = createOzoneBucket(binderModel, tableNamesProps);
            var key = createOzoneKey(binderModel, tableNamesProps);
            var localFile = createOzoneLocalFile(binderModel, tableNamesProps);
            // Optional
            var oneFilePerBatch = createOneFilePerIteration(binderModel, optionsProps);
            var deletePrevious = createDeletePrevious(binderModel, optionsProps);
            var appendHeader = createCsvHeader(binderModel, optionsProps);
            var ozoneRepFactor = createOzoneReplicationFactor(binderModel, optionsProps);
            var details = createOptionalConfigs(oneFilePerBatch, deletePrevious, appendHeader, ozoneRepFactor);
            return List.of(volume, bucket, key, localFile, details);
        }

        case OZONE_JSON, OZONE_AVRO, OZONE_ORC -> {
            var volume = createOzoneVolume(binderModel, tableNamesProps);
            var bucket = createOzoneBucket(binderModel, tableNamesProps);
            var key = createOzoneKey(binderModel, tableNamesProps);
            var localFile = createOzoneLocalFile(binderModel, tableNamesProps);
            // Optional
            var oneFilePerBatch = createOneFilePerIteration(binderModel, optionsProps);
            var deletePrevious = createDeletePrevious(binderModel, optionsProps);
            var ozoneRepFactor = createOzoneReplicationFactor(binderModel, optionsProps);
            var details = createOptionalConfigs(oneFilePerBatch, deletePrevious, ozoneRepFactor);
            return List.of(volume, bucket, key, localFile, details);
        }

        case OZONE_PARQUET -> {
            var volume = createOzoneVolume(binderModel, tableNamesProps);
            var bucket = createOzoneBucket(binderModel, tableNamesProps);
            var key = createOzoneKey(binderModel, tableNamesProps);
            var localFile = createOzoneLocalFile(binderModel, tableNamesProps);
            // Optional
            var oneFilePerBatch = createOneFilePerIteration(binderModel, optionsProps);
            var deletePrevious = createDeletePrevious(binderModel, optionsProps);
            var parquetGroupSize = createParquetPageSize(binderModel, optionsProps);
            var parquetRowGroupSize = createParquetRowGroupSize(binderModel, optionsProps);
            var parquetDicSize = createParquetDictionaryPageSize(binderModel, optionsProps);
            var parquetEncoding = createParquetDictionaryEncoding(binderModel, optionsProps);
            var ozoneRepFactor = createOzoneReplicationFactor(binderModel, optionsProps);
            var details = createOptionalConfigs(oneFilePerBatch, deletePrevious, parquetGroupSize, parquetRowGroupSize, parquetDicSize, parquetEncoding, ozoneRepFactor);
            return List.of(volume, bucket, key, localFile, details);
        }

        /**
         S3 Files
         */
        case S3_CSV -> {
            var bucket = createS3Bucket(binderModel, tableNamesProps);
            var directory = createS3Directory(binderModel, tableNamesProps);
            var key = createS3Key(binderModel, tableNamesProps);
            var localFile = createS3LocalFile(binderModel, tableNamesProps);
            var s3region = createS3Region(binderModel, tableNamesProps);
            // Optional
            var oneFilePerBatch = createOneFilePerIteration(binderModel, optionsProps);
            var deletePrevious = createDeletePrevious(binderModel, optionsProps);
            var appendHeader = createCsvHeader(binderModel, optionsProps);
            var details = createOptionalConfigs(oneFilePerBatch, deletePrevious, appendHeader);
            return List.of(bucket, directory, key, localFile, s3region, new Span(), details);
        }

        case S3_JSON, S3_AVRO, S3_ORC -> {
            var bucket = createS3Bucket(binderModel, tableNamesProps);
            var directory = createS3Directory(binderModel, tableNamesProps);
            var key = createS3Key(binderModel, tableNamesProps);
            var localFile = createS3LocalFile(binderModel, tableNamesProps);
            var s3region = createS3Region(binderModel, tableNamesProps);
            // Optional
            var oneFilePerBatch = createOneFilePerIteration(binderModel, optionsProps);
            var deletePrevious = createDeletePrevious(binderModel, optionsProps);
            var details = createOptionalConfigs(oneFilePerBatch, deletePrevious);
            return List.of(bucket, directory, key, localFile, s3region, new Span(), details);
        }

        case S3_PARQUET -> {
            var bucket = createS3Bucket(binderModel, tableNamesProps);
            var directory = createS3Directory(binderModel, tableNamesProps);
            var key = createS3Key(binderModel, tableNamesProps);
            var localFile = createS3LocalFile(binderModel, tableNamesProps);
            var s3region = createS3Region(binderModel, tableNamesProps);
            // Optional
            var oneFilePerBatch = createOneFilePerIteration(binderModel, optionsProps);
            var deletePrevious = createDeletePrevious(binderModel, optionsProps);
            var parquetGroupSize = createParquetPageSize(binderModel, optionsProps);
            var parquetRowGroupSize = createParquetRowGroupSize(binderModel, optionsProps);
            var parquetDicSize = createParquetDictionaryPageSize(binderModel, optionsProps);
            var parquetEncoding = createParquetDictionaryEncoding(binderModel, optionsProps);
            var details = createOptionalConfigs(
                oneFilePerBatch, deletePrevious, parquetGroupSize, parquetRowGroupSize,
                parquetDicSize, parquetEncoding);
            return List.of(bucket, directory, key, localFile, s3region, new Span(), details);
        }


        /**
         ADLS Files
         */
        case ADLS_CSV -> {
            var container = createADLSContainer(binderModel, tableNamesProps);
            var directory = createADLSDirectory(binderModel, tableNamesProps);
            var key = createADLSFileName(binderModel, tableNamesProps);
            var localFile = createADLSLocalFile(binderModel, tableNamesProps);
            // Optional
            var accountType = createADLSAccountType(binderModel, tableNamesProps);
            var oneFilePerBatch = createOneFilePerIteration(binderModel, optionsProps);
            var blockSize = createAdlsBlockSize(binderModel, optionsProps);
            var maxUploadSize = createAdlsMaxUploadSize(binderModel, optionsProps);
            var maxConcurrency = createAdlsMaxConcurrency(binderModel, optionsProps);
            var deletePrevious = createDeletePrevious(binderModel, optionsProps);
            var appendHeader = createCsvHeader(binderModel, optionsProps);
            var details = createOptionalConfigs(accountType,
                oneFilePerBatch, deletePrevious, appendHeader, blockSize, maxUploadSize, maxConcurrency);
            return List.of(container, directory, key, localFile, details);
        }

        case ADLS_JSON, ADLS_AVRO, ADLS_ORC -> {
            var container = createADLSContainer(binderModel, tableNamesProps);
            var directory = createADLSDirectory(binderModel, tableNamesProps);
            var key = createADLSFileName(binderModel, tableNamesProps);
            var localFile = createADLSLocalFile(binderModel, tableNamesProps);
            // Optional
            var accountType = createADLSAccountType(binderModel, tableNamesProps);
            var oneFilePerBatch = createOneFilePerIteration(binderModel, optionsProps);
            var blockSize = createAdlsBlockSize(binderModel, optionsProps);
            var maxUploadSize = createAdlsMaxUploadSize(binderModel, optionsProps);
            var maxConcurrency = createAdlsMaxConcurrency(binderModel, optionsProps);
            var deletePrevious = createDeletePrevious(binderModel, optionsProps);
            var details = createOptionalConfigs(accountType,
                oneFilePerBatch, deletePrevious, blockSize, maxUploadSize, maxConcurrency);
            return List.of(container, directory, key, localFile, details);
        }

        case ADLS_PARQUET -> {
            var container = createADLSContainer(binderModel, tableNamesProps);
            var directory = createADLSDirectory(binderModel, tableNamesProps);
            var key = createADLSFileName(binderModel, tableNamesProps);
            var localFile = createADLSLocalFile(binderModel, tableNamesProps);
            // Optional
            var accountType = createADLSAccountType(binderModel, tableNamesProps);
            var oneFilePerBatch = createOneFilePerIteration(binderModel, optionsProps);
            var blockSize = createAdlsBlockSize(binderModel, optionsProps);
            var maxUploadSize = createAdlsMaxUploadSize(binderModel, optionsProps);
            var maxConcurrency = createAdlsMaxConcurrency(binderModel, optionsProps);
            var deletePrevious = createDeletePrevious(binderModel, optionsProps);
            var parquetGroupSize = createParquetPageSize(binderModel, optionsProps);
            var parquetRowGroupSize = createParquetRowGroupSize(binderModel, optionsProps);
            var parquetDicSize = createParquetDictionaryPageSize(binderModel, optionsProps);
            var parquetEncoding = createParquetDictionaryEncoding(binderModel, optionsProps);
            var details = createOptionalConfigs(accountType,
                oneFilePerBatch, deletePrevious, parquetGroupSize, parquetRowGroupSize, parquetDicSize, parquetEncoding, blockSize, maxUploadSize, maxConcurrency);
            return List.of(container, directory, key, localFile, details);
        }


        /**
         GCS Files
         */
        case GCS_CSV -> {
            var bucket = createGCSBucket(binderModel, tableNamesProps);
            var directory = createGCSDirectory(binderModel, tableNamesProps);
            var key = createGCSObjectName(binderModel, tableNamesProps);
            var localFile = createGCSLocalFile(binderModel, tableNamesProps);
            var gcsRegion = createGCSRegion(binderModel, tableNamesProps);
            // Optional
            var oneFilePerBatch = createOneFilePerIteration(binderModel, optionsProps);
            var deletePrevious = createDeletePrevious(binderModel, optionsProps);
            var appendHeader = createCsvHeader(binderModel, optionsProps);
            var details = createOptionalConfigs(oneFilePerBatch, deletePrevious, appendHeader);
            return List.of(bucket, directory, key, localFile, gcsRegion, details);
        }

        case GCS_JSON, GCS_AVRO, GCS_ORC -> {
            var bucket = createGCSBucket(binderModel, tableNamesProps);
            var directory = createGCSDirectory(binderModel, tableNamesProps);
            var key = createGCSObjectName(binderModel, tableNamesProps);
            var localFile = createGCSLocalFile(binderModel, tableNamesProps);
            var gcsRegion = createGCSRegion(binderModel, tableNamesProps);
            // Optional
            var oneFilePerBatch = createOneFilePerIteration(binderModel, optionsProps);
            var deletePrevious = createDeletePrevious(binderModel, optionsProps);
            var details = createOptionalConfigs(oneFilePerBatch, deletePrevious);
            return List.of(bucket, directory, key, localFile, gcsRegion, details);
        }

        case GCS_PARQUET -> {
            var bucket = createGCSBucket(binderModel, tableNamesProps);
            var directory = createGCSDirectory(binderModel, tableNamesProps);
            var key = createGCSObjectName(binderModel, tableNamesProps);
            var localFile = createGCSLocalFile(binderModel, tableNamesProps);
            var gcsRegion = createGCSRegion(binderModel, tableNamesProps);
            // Optional
            var oneFilePerBatch = createOneFilePerIteration(binderModel, optionsProps);
            var deletePrevious = createDeletePrevious(binderModel, optionsProps);
            var parquetGroupSize = createParquetPageSize(binderModel, optionsProps);
            var parquetRowGroupSize = createParquetRowGroupSize(binderModel, optionsProps);
            var parquetDicSize = createParquetDictionaryPageSize(binderModel, optionsProps);
            var parquetEncoding = createParquetDictionaryEncoding(binderModel, optionsProps);
            var details = createOptionalConfigs(oneFilePerBatch, deletePrevious, parquetGroupSize, parquetRowGroupSize, parquetDicSize, parquetEncoding);
            return List.of(bucket, directory, key, localFile, gcsRegion, details);
        }


        /**
         Hadoop
         */

        case HIVE -> {
            var database = createHiveDatabase(binderModel, tableNamesProps);
            var tablename = createHiveTableName(binderModel, tableNamesProps);
            var hdfsPath = createHiveHdfsFilePath(binderModel, tableNamesProps);
            var hdfsFileName = createHdfsName(binderModel, tableNamesProps);

            // Optional
            var deletePrevious = createDeletePrevious(binderModel, optionsProps);
            var tempTableName = createHiveTempTable(binderModel,tableNamesProps);
            var hiveThreadNumber  = createHiveThreadNumber(binderModel, optionsProps);
            var hiveTableType = createHiveTableType(binderModel, optionsProps);
            var hiveTableFormat = createHiveTableFormat(binderModel, optionsProps);
            var hiveOnHdfs = createHiveOnHdfs(binderModel, optionsProps);
            var hiveTezQueueName = createHiveTezQueueName(binderModel, optionsProps);
            var hiveTablePartitionCols = createHiveTablePartitionsCols(binderModel, optionsProps);
            var hiveTableBucketCols = createHiveTableBucketsCols(binderModel, optionsProps);
            var hiveTableBucketNumber = createHiveTableBucketsNumber(binderModel, optionsProps);

            var details = createOptionalConfigs(deletePrevious, tempTableName, hiveThreadNumber, hiveTableType, hiveTableFormat,
                hiveOnHdfs, hiveTezQueueName, hiveTablePartitionCols, hiveTableBucketCols, hiveTableBucketNumber);
            return List.of(database, tablename, hdfsPath, hdfsFileName, details);
        }

        case HBASE -> {
            var namespace = createHBaseNamespace(binderModel, tableNamesProps);
            var tableName = createHBaseTableName(binderModel, tableNamesProps);
            var primaryKey = createHbasePrimaryKey(binderModel, optionsProps);
            // Optional
            var deletePrevious = createDeletePrevious(binderModel, optionsProps);

            var details = createOptionalConfigs(deletePrevious);
            return List.of(namespace, tableName, primaryKey, new Span(), details);
        }

        case KAFKA -> {
            var topic = createKafkaTopic(binderModel, tableNamesProps);
            // Optional
            var deletePrevious = createDeletePrevious(binderModel, optionsProps);
            var messageKey = createKafkaMsgKey(binderModel, optionsProps);
            var acksConfig = createKafkaAcksConfig(binderModel, optionsProps);
            var retriesConfig = createKafkaRetriesConfig(binderModel, optionsProps);
            var jaasFilePath = createKafkaJaasFilePath(binderModel, optionsProps);
            var replicationFactor = createKafkaReplicationFactor(binderModel, optionsProps);
            var partitionsNumber = createKafkaPartitionsNumber(binderModel, optionsProps);
            var messageType = createKafkaMessageType(binderModel,optionsProps);

            var details = createOptionalConfigs(deletePrevious, messageKey, acksConfig, retriesConfig, jaasFilePath, replicationFactor, partitionsNumber, messageType);
            return List.of(topic, new Span(), details);
        }

        case SOLR -> {
            var collection = createSolRCollection(binderModel, tableNamesProps);

            // Optional
            var deletePrevious = createDeletePrevious(binderModel, optionsProps);
            var shards = createSolrShards(binderModel, optionsProps);
            var replicas = createSolrReplicas(binderModel, optionsProps);
            var jaasFilePath = createSolrJaasFilePath(binderModel, optionsProps);

            var details = createOptionalConfigs(deletePrevious, shards, replicas, jaasFilePath);
            return List.of(collection, new Span(), details);
        }

        case KUDU -> {
            var tableName = createKuduTableName(binderModel, tableNamesProps);
            var primaryKeys = createKuduPrimaryKeys(binderModel, optionsProps);
            // Optional
            var deletePrevious = createDeletePrevious(binderModel, optionsProps);
            var buckets = createKuduBuckets(binderModel,optionsProps);
            var buffer = createKuduBuffer(binderModel, optionsProps);
            var flush = createKuduFlush(binderModel,optionsProps);
            var replicas = createKuduReplicas(binderModel, optionsProps);
            var hashkeys = createKuduHashKeys(binderModel,optionsProps);
            var rangeKeys = createKuduRangeKeys(binderModel, optionsProps);


            var details = createOptionalConfigs(deletePrevious, buckets, buffer, flush, replicas, hashkeys, rangeKeys);
            return List.of(tableName, primaryKeys, details);
        }

        }

        return Collections.emptyList();

    }


}
