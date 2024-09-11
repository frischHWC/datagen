package com.datagen.views;

import com.vaadin.flow.component.Composite;
import com.vaadin.flow.component.HasText;
import com.vaadin.flow.component.button.Button;
import com.vaadin.flow.component.html.Span;
import com.vaadin.flow.component.orderedlayout.FlexComponent;
import com.vaadin.flow.component.orderedlayout.HorizontalLayout;
import com.vaadin.flow.component.orderedlayout.VerticalLayout;
import com.vaadin.flow.router.PageTitle;
import com.vaadin.flow.router.Route;
import org.vaadin.lineawesome.LineAwesomeIcon;

@PageTitle("Home")
@Route(value = "", layout = MainLayout.class)
public class HomeView extends Composite<VerticalLayout> {

    public HomeView() {
        VerticalLayout layoutColumn = new VerticalLayout();
        layoutColumn.setWidth("100%");
        layoutColumn.getStyle().set("flex-grow", "1");

        var textArea = new Span("""
        Welcome to Datagen !
        """);
        textArea.setWidth( "100%" );
        textArea.setWhiteSpace(HasText.WhiteSpace.PRE);
        textArea.addClassName("rounded-textarea");

        layoutColumn.add(textArea);

        var hl = new HorizontalLayout();
        hl.setAlignItems(FlexComponent.Alignment.AUTO);
        hl.setHeight("50%");
        hl.setWidth("100%");

        Button buttonModelCreation = new Button("1. Shape Data to Generate", LineAwesomeIcon.PENCIL_RULER_SOLID.create());
        buttonModelCreation.setAutofocus(true);
        buttonModelCreation.setTooltipText("Redirect to a model creation");
        buttonModelCreation.setIconAfterText(true);
        buttonModelCreation.setHeight("30%");
        buttonModelCreation.setWidth("33%");
        buttonModelCreation.addClickListener(e ->
            buttonModelCreation.getUI().ifPresent(ui ->
                ui.navigate("model/creation"))
        );

        Button buttonDataGeneration = new Button("2. Generate Data", LineAwesomeIcon.ROCKET_SOLID.create());
        buttonDataGeneration.setAutofocus(true);
        buttonDataGeneration.setTooltipText("Redirect to data generation");
        buttonDataGeneration.setIconAfterText(true);
        buttonDataGeneration.setHeight("30%");
        buttonDataGeneration.setWidth("33%");
        buttonDataGeneration.addClickListener(e ->
            buttonDataGeneration.getUI().ifPresent(ui ->
                ui.navigate("generation"))
        );

        Button buttonRunningCommands = new Button("3. See Running Generations", LineAwesomeIcon.PLANE_SOLID.create());
        buttonRunningCommands.setAutofocus(true);
        buttonRunningCommands.setTooltipText("Redirect to Commands");
        buttonRunningCommands.setIconAfterText(true);
        buttonRunningCommands.setHeight("30%");
        buttonRunningCommands.setWidth("33%");
        buttonRunningCommands.addClickListener(e ->
            buttonRunningCommands.getUI().ifPresent(ui ->
                ui.navigate("commands/running"))
        );

        hl.add(buttonModelCreation, buttonDataGeneration, buttonRunningCommands);
        layoutColumn.add(hl);

        getContent().add(layoutColumn);
    }

}
