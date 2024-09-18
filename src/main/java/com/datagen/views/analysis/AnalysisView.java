package com.datagen.views.analysis;

import com.datagen.config.ConnectorParser;
import com.datagen.service.model.ModelGeneratorSevice;
import com.datagen.views.MainLayout;
import com.vaadin.flow.component.Component;
import com.vaadin.flow.component.Composite;
import com.vaadin.flow.component.HasText;
import com.vaadin.flow.component.button.Button;
import com.vaadin.flow.component.button.ButtonVariant;
import com.vaadin.flow.component.combobox.ComboBox;
import com.vaadin.flow.component.dialog.Dialog;
import com.vaadin.flow.component.formlayout.FormLayout;
import com.vaadin.flow.component.html.Span;
import com.vaadin.flow.component.notification.Notification;
import com.vaadin.flow.component.notification.NotificationVariant;
import com.vaadin.flow.component.orderedlayout.VerticalLayout;
import com.vaadin.flow.data.binder.Binder;
import com.vaadin.flow.data.binder.ValidationException;
import com.vaadin.flow.router.PageTitle;
import com.vaadin.flow.router.Route;
import com.vaadin.flow.server.StreamResource;
import jakarta.annotation.security.PermitAll;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.vaadin.lineawesome.LineAwesomeIcon;
import org.vaadin.olli.FileDownloadWrapper;

import java.io.ByteArrayInputStream;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;

import static com.datagen.views.analysis.AnalysisUtils.*;

@Slf4j
@PageTitle("Models")
@Route(value = "analysis", layout = MainLayout.class)
@PermitAll
public class AnalysisView extends Composite<VerticalLayout> {

    @Autowired
    private ModelGeneratorSevice modelGeneratorSevice;

    private Binder<InternalAnalysis> binderAnalysis;

    public AnalysisView() {
        this.binderAnalysis = new Binder<>();
        VerticalLayout layoutColumn = new VerticalLayout();
        layoutColumn.setWidth("100%");
        layoutColumn.getStyle().set("flex-grow", "1");

        var formLayout = new FormLayout();

        var launchButton = launchDataGenerationButton();
        var typeOfAnalysis = createTypeOfAnalysis();
        var cbConnector = createConnectorListButton(formLayout, launchButton);

        formLayout.add(typeOfAnalysis, cbConnector);

        layoutColumn.add(formLayout);
        layoutColumn.add(launchButton);

        getContent().add(layoutColumn);
    }

    /**
     * Type of analysis (as of now, only simple)
     * @return
     */
    private ComboBox<String> createTypeOfAnalysis() {
        var cb = new ComboBox<String>("Type of Analysis");
        cb.setRequired(true);
        cb.setItems("SIMPLE");
        cb.setValue("SIMPLE");
        this.binderAnalysis.forField(cb)
            .bind(InternalAnalysis::getTypeOfAnalysis, InternalAnalysis::setTypeOfAnalysis);
        return cb;
    }

    /**
     * Launch Analysis and provide a pop up with a download model once done
     * @return
     */
    private Button launchDataGenerationButton() {
        var button = new Button("Launch Analysis");
        button.addThemeVariants(ButtonVariant.LUMO_PRIMARY);
        button.setEnabled(false);
        button.addClickListener(e -> {
            log.debug("Asked to launch analysis of data");

            // Bind Analysis Parameters
            var analysisToMake = new InternalAnalysis();
            try {
                this.binderAnalysis.writeBean(analysisToMake);
            } catch (ValidationException exception) {
                log.warn("Fields cannot be validated, user must re-take actions ");
            }

            Notification notification = Notification.show("Launch Analysis on  ", 5000,
                Notification.Position.TOP_CENTER);
            notification.addThemeVariants(NotificationVariant.LUMO_SUCCESS);

            var modelAsJSON = modelGeneratorSevice.generateModel(
                analysisToMake.source,
                !analysisToMake.typeOfAnalysis.equalsIgnoreCase("SIMPLE"),
                analysisToMake.filepath,
                analysisToMake.database,
                analysisToMake.table,
                analysisToMake.volume,
                analysisToMake.bucket,
                analysisToMake.key
            );

            // Show model generated
            var dialog = new Dialog("Model Generated");
            var closeButton = new Button("Close", ev ->dialog.close());
            var downloadButton = new Button("Download Model Generated", LineAwesomeIcon.DOWNLOAD_SOLID.create());
            downloadButton.setTooltipText("Download Model");
            downloadButton.addThemeVariants(ButtonVariant.LUMO_PRIMARY);
            downloadButton.addClickListener(eventD -> {
                Notification notificationDownload = Notification.show("Downloaded Model Generated" );
                notificationDownload.addThemeVariants(NotificationVariant.LUMO_PRIMARY);
            });

            // Setup download resource for download button
            StreamResource
                resource = new StreamResource( "model-generated.json",
                () ->  {
                    try {
                        return new ByteArrayInputStream(modelAsJSON.getBytes());
                    } catch (Exception exception) {
                        return new ByteArrayInputStream("Model not Found".getBytes());
                    }
                }
            );
            FileDownloadWrapper downloadButtonWrapper = new FileDownloadWrapper(resource);
            downloadButtonWrapper.wrapComponent(downloadButton);

            var spanModel = new Span(modelAsJSON);
            spanModel.setWhiteSpace(HasText.WhiteSpace.PRE);
            dialog.add(spanModel);
            dialog.getFooter().add(closeButton, downloadButtonWrapper);
            dialog.open();

        });
        return button;
    }

    /**
     * Create a connector list button
     * @return
     */
    private ComboBox<ConnectorParser.Connector> createConnectorListButton(FormLayout formLayout, Button launchButton) {
        var comboBox = new ComboBox<ConnectorParser.Connector>("Connector:");
        comboBox.setItems(
            EnumSet.allOf(ConnectorParser.Connector.class).stream().toList());
        comboBox.setRequired(true);
        comboBox.setVisible(true);
        comboBox.setHelperText("Where is data to analyze ?");

        this.binderAnalysis.forField(comboBox)
            .bind(InternalAnalysis::getSource,
                InternalAnalysis::setSource);

        // Depending on the type of the field, there should be different printed values
        var listOfParametersForConnector = new LinkedList<Component>();

        comboBox.addValueChangeListener(e -> {
            formLayout.remove(listOfParametersForConnector);
            listOfParametersForConnector.clear();
            if(e.getValue()!=null) {
                launchButton.setEnabled(true);
                switch (e.getValue()) {

                /**
                 Local Files
                 */
                case JSON, ORC, AVRO, PARQUET, CSV, HDFS_JSON, HDFS_AVRO, HDFS_ORC, HDFS_PARQUET, HDFS_CSV -> {
                    var pathText = createPath(binderAnalysis);
                    listOfParametersForConnector.addAll(List.of(pathText));
                }

                /**
                 Ozone Files
                 */
                case OZONE_JSON, OZONE_AVRO, OZONE_ORC, OZONE_PARQUET, OZONE_CSV -> {
                    var volume = createVolume(binderAnalysis);
                    var bucket = createBucket(binderAnalysis);
                    var key = createKey(binderAnalysis);
                    listOfParametersForConnector.addAll(List.of(volume, bucket, key));
                }

                /**
                 S3 Files
                 */
                case S3_JSON, S3_AVRO, S3_ORC, S3_PARQUET, S3_CSV,
                     ADLS_PARQUET, ADLS_JSON, ADLS_AVRO, ADLS_ORC, ADLS_CSV,
                     GCS_PARQUET, GCS_JSON, GCS_AVRO, GCS_ORC, GCS_CSV -> {
                    var bucket = createBucket(binderAnalysis);
                    var key = createKey(binderAnalysis);
                    listOfParametersForConnector.addAll(List.of(bucket, key));
                }

                /**
                 Hadoop
                 */

                case HIVE -> {
                    var database = createDatabase(binderAnalysis);
                    var tablename = createTable(binderAnalysis);
                    listOfParametersForConnector.addAll(List.of(database,tablename));
                }

                }

            listOfParametersForConnector.forEach(f -> formLayout.add(f, 1));
        } else {
            launchButton.setEnabled(false);
        }

        });
        return comboBox;
    }

    @Getter @Setter
    protected class InternalAnalysis {
        ConnectorParser.Connector source;
        String typeOfAnalysis;
        String filepath;
        String database;
        String table;
        String volume;
        String bucket;
        String key;
    }

}
