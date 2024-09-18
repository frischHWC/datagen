package com.datagen.views.commands;

import com.datagen.config.ConnectorParser;
import com.datagen.connector.storage.utils.FileUtils;
import com.datagen.model.OptionsConverter;
import com.datagen.service.command.Command;
import com.datagen.service.command.CommandRunnerService;
import com.datagen.utils.Utils;
import com.datagen.views.MainLayout;
import com.vaadin.flow.component.Composite;
import com.vaadin.flow.component.HasText;
import com.vaadin.flow.component.button.Button;
import com.vaadin.flow.component.button.ButtonVariant;
import com.vaadin.flow.component.dialog.Dialog;
import com.vaadin.flow.component.grid.Grid;
import com.vaadin.flow.component.grid.GridSortOrder;
import com.vaadin.flow.component.html.Span;
import com.vaadin.flow.component.icon.VaadinIcon;
import com.vaadin.flow.component.notification.Notification;
import com.vaadin.flow.component.notification.NotificationVariant;
import com.vaadin.flow.component.orderedlayout.HorizontalLayout;
import com.vaadin.flow.component.orderedlayout.VerticalLayout;
import com.vaadin.flow.data.provider.SortDirection;
import com.vaadin.flow.data.renderer.ComponentRenderer;
import com.vaadin.flow.router.PageTitle;
import com.vaadin.flow.router.Route;
import com.vaadin.flow.server.StreamResource;
import jakarta.annotation.security.PermitAll;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.vaadin.lineawesome.LineAwesomeIcon;
import org.vaadin.olli.FileDownloadWrapper;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

@Slf4j
@PageTitle("Models")
@Route(value = "commands", layout = MainLayout.class)
@PermitAll
public class CommandsView extends Composite<VerticalLayout> {

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
    private CommandRunnerService commandRunnerService;

    @Autowired
    public CommandsView(CommandRunnerService commandRunnerService) {
        this.commandRunnerService = commandRunnerService;
        VerticalLayout layoutColumn = new VerticalLayout();
        layoutColumn.setWidth("100%");
        layoutColumn.getStyle().set("flex-grow", "1");

        var grid = new Grid<Command>();
        grid.addColumn(Command::getModelId)
            .setHeader("Model")
            .setSortable(true)
            .setAutoWidth(true)
            .setWidth("10rem");
        grid.addColumn(c -> c.getConnectorsList().stream().map(ConnectorParser.Connector::toString).collect(
                Collectors.joining(" ; ")))
            .setHeader("Connectors")
            .setSortable(true)
            .setAutoWidth(true)
            .setWidth("10rem");
        grid.addColumn(c -> Utils.formatNumber(c.getRowsPerBatch()*c.getNumberOfBatches()))
            .setHeader("Rows")
            .setSortable(true)
            .setAutoWidth(true)
            .setWidth("5rem");
        grid.addColumn(Command::getStatus)
            .setHeader("Status")
            .setSortable(true)
            .setAutoWidth(true)
            .setWidth("10rem")
            .setFlexGrow(0);
        grid.addColumn(c -> c.getProgress()+"%")
            .setHeader("Progress")
            .setSortable(true)
            .setAutoWidth(true)
            .setWidth("10rem")
            .setFlexGrow(0);
        grid.addColumn(c -> Utils.formatTimetaken(c.getDurationMilliSeconds()))
            .setHeader("Duration")
            .setSortable(true)
            .setAutoWidth(true)
            .setWidth("10rem");
        grid.addColumn(c -> {
            return c.getLastFinishedTimestamp()==0?
                "-":
                formatter.format(LocalDateTime.ofEpochSecond(c.getLastFinishedTimestamp()/1000, 0, ZoneOffset.UTC));
            })
            .setHeader("Finished Date")
            .setSortable(true)
            .setAutoWidth(true)
            .setWidth("20rem")
            .setFlexGrow(0);
        grid.addColumn(Command::getCommandUuid)
            .setHeader("Command ID")
            .setSortable(true)
            .setFrozen(true)
            .setAutoWidth(true)
            .setWidth("12rem")
            .setFlexGrow(0);

        // Button to print model and download it
        grid.addColumn(infoButtonWithDownload())
            .setHeader("Info")
            .setWidth("5rem")
            .setFlexGrow(0);

        grid.addClassName("styling");
        grid.setPartNameGenerator(command -> {
            if (command.getStatus()==Command.CommandStatus.FINISHED)
                return "finished";
            else if (command.getStatus()==Command.CommandStatus.FAILED)
                return "failed";
            else
                return null;
        });

        grid.setItems(this.commandRunnerService.getAllCommands());
        grid.sort(List.of(new GridSortOrder<>(grid.getColumns().get(6), SortDirection.DESCENDING)));

        var refreshButton = refreshGridButton(grid);
        var hlrefresh = new HorizontalLayout();
        hlrefresh.add(refreshButton);
        layoutColumn.add(hlrefresh);
        layoutColumn.add(grid);
        getContent().add(layoutColumn);

    }

    private Button refreshGridButton(Grid<Command> grid) {
        var refreshButton = new Button("Refresh", VaadinIcon.REFRESH.create());
        refreshButton.setIconAfterText(true);
        refreshButton.addClickListener( e -> {
            grid.setItems(this.commandRunnerService.getAllCommands());
        });
        return refreshButton;
    }

    /**
     * Create a button that pops up info on a command (by describing it) and add a button to download info as JSON and data (if on local)
     * @return
     */
    private ComponentRenderer<Button, Command> infoButtonWithDownload() {
        return new ComponentRenderer<>(Button::new, (button, command) -> {
            button.addThemeVariants(ButtonVariant.LUMO_ICON,
                ButtonVariant.LUMO_TERTIARY);
            button.addClickListener(e -> {
                Dialog dialogInfo = new Dialog();
                dialogInfo.setHeaderTitle("ID: " + command.getCommandUuid());

                var commandAsJSON = new Span(command.toMinimalString());
                commandAsJSON.setWhiteSpace(HasText.WhiteSpace.PRE);
                dialogInfo.add(commandAsJSON);

                var closeButton = new Button("Close", eventClose -> dialogInfo.close());

                var downloadDetailsButton = new Button("Details");
                downloadDetailsButton.setIcon(LineAwesomeIcon.DOWNLOAD_SOLID.create());
                downloadDetailsButton.setIconAfterText(true);
                downloadDetailsButton.setTooltipText("Download Command Details");
                downloadDetailsButton.addThemeVariants(ButtonVariant.LUMO_PRIMARY);
                downloadDetailsButton.addClickListener(eventD -> {
                    Notification notification = Notification.show("Downloaded Details on Command:" + command.getCommandUuid());
                    notification.addThemeVariants(NotificationVariant.LUMO_PRIMARY);
                });
                StreamResource detailsResource = new StreamResource(
                    command.getCommandUuid().toString() + "-details.json",
                    () -> new ByteArrayInputStream(command.toString().getBytes())
                );
                FileDownloadWrapper downloadDetailsButtonWrapper = new FileDownloadWrapper(detailsResource);
                downloadDetailsButtonWrapper.wrapComponent(downloadDetailsButton);
                dialogInfo.getFooter().add(downloadDetailsButtonWrapper);

                var downloadDataButton = new Button("Data");
                downloadDataButton.setIcon(LineAwesomeIcon.DOWNLOAD_SOLID.create());
                downloadDataButton.setIconAfterText(true);
                downloadDataButton.setTooltipText("Download Data Generated");
                downloadDataButton.addThemeVariants(ButtonVariant.LUMO_PRIMARY);
                downloadDataButton.setVisible(false);
                downloadDataButton.addClickListener(eventD -> {
                    Notification notification = Notification.show("Downloaded Data Generated by command:" + command.getCommandUuid());
                    notification.addThemeVariants(NotificationVariant.LUMO_PRIMARY);
                });

                if(command.getStatus()== Command.CommandStatus.FINISHED) {
                    if (command.getConnectorsList().contains(
                        ConnectorParser.Connector.CSV) ||
                        command.getConnectorsList().contains(
                        ConnectorParser.Connector.AVRO) ||
                        command.getConnectorsList().contains(
                            ConnectorParser.Connector.PARQUET) ||
                        command.getConnectorsList().contains(
                            ConnectorParser.Connector.JSON) ||
                        command.getConnectorsList().contains(
                            ConnectorParser.Connector.ORC)) {
                        log.debug("This command is a local one, so creating zip files on data generated");
                        File[] possiblefiles = FileUtils.listLocalFilesWithPrefix(command.getModel().getTableNames().get(
                            OptionsConverter.TableNames.LOCAL_FILE_PATH).toString(), command.getModel().getTableNames().get(
                            OptionsConverter.TableNames.LOCAL_FILE_NAME).toString());

                        if(possiblefiles!=null) {
                            var filesToZip = Arrays.stream(possiblefiles)
                                .filter(f -> f.getName().endsWith(
                                    command.getConnectorsList().get(0).toString().toLowerCase(Locale.ROOT)))
                                .collect(Collectors.toList());

                            if(!filesToZip.isEmpty()) {
                                String zipName = command.getCommandUuid().toString() + ".zip";
                                var baOs = FileUtils.zipFilesIntoAByteArray(filesToZip);
                                StreamResource resource = new StreamResource(
                                    zipName,
                                    () -> new ByteArrayInputStream(baOs.toByteArray())
                                );
                                try {
                                    baOs.close();
                                } catch (IOException ioexception) {
                                    log.warn("Could not close byte array");
                                }
                                downloadDataButton.setVisible(true);
                                FileDownloadWrapper downloadButtonWrapper =
                                    new FileDownloadWrapper(resource);
                                downloadButtonWrapper.wrapComponent(
                                    downloadDataButton);
                                dialogInfo.getFooter()
                                    .add(downloadButtonWrapper);
                            }
                        }
                    }

                }

                dialogInfo.getFooter().add(closeButton);
                dialogInfo.open();
            });
            button.setIcon(LineAwesomeIcon.INFO_CIRCLE_SOLID.create());
        });
    }

}
