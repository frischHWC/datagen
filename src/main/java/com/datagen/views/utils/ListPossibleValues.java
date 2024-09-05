package com.datagen.views.utils;

import com.vaadin.flow.component.button.Button;
import com.vaadin.flow.component.customfield.CustomField;
import com.vaadin.flow.component.icon.VaadinIcon;
import com.vaadin.flow.component.orderedlayout.FlexComponent;
import com.vaadin.flow.component.orderedlayout.HorizontalLayout;
import com.vaadin.flow.component.orderedlayout.VerticalLayout;
import com.vaadin.flow.component.textfield.NumberField;
import com.vaadin.flow.component.textfield.TextArea;

import java.util.HashMap;


public class ListPossibleValues extends CustomField<HashMap<String, Long>> {

  private final VerticalLayout layout = new VerticalLayout();
  private final HashMap<TextArea, NumberField> possibleValuesFields = new HashMap<>();


  public String getDescription() {
    return """
        Provide an optional list of values. Values can be weighted using the weights input.
        Probability of a value to appear is: its weight divided by the sum of all weights.
        (Default weight is 1).
        """;
  }

  public ListPossibleValues() {
    layout.add(addNewButton());
    add(layout);
  }

  @Override
  protected HashMap<String, Long> generateModelValue() {
    var map = new HashMap<String, Long>();
    possibleValuesFields.forEach((key, value) -> map.put(key.getValue(),
        (value != null && value.getValue() != null)
            ? value.getValue().longValue() : 1L));
    return map;
  }

  @Override
  protected void setPresentationValue(HashMap<String, Long> values) {
    layout.removeAll();
    possibleValuesFields.clear();

    if (values != null) {
      values.forEach(this::addPossiblevalue);
    }

    layout.add(addNewButton());
  }


  private void addPossiblevalue(String possibleValue, Long weight) {
    var textField = new TextArea();
    textField.setValue(possibleValue);
    textField.setLabel("Value");
    var weightField = new NumberField();
    weightField.setValue(weight.doubleValue());
    weightField.setLabel("Weight");
    possibleValuesFields.put(textField, weightField);

    Button removeButton = new Button(VaadinIcon.TRASH.create());
    removeButton.addClickListener(e -> {
      layout.remove(textField.getParent().get());
      possibleValuesFields.remove(textField);
    });

    HorizontalLayout fieldLayout = new HorizontalLayout(textField, weightField, removeButton);
    fieldLayout.setAlignItems(FlexComponent.Alignment.BASELINE);
    layout.addComponentAtIndex(layout.getComponentCount()==0?layout.getComponentCount():layout.getComponentCount()-1, fieldLayout);
  }

  private Button addNewButton() {
    Button button = new Button(VaadinIcon.PLUS.create());
    button.addClickListener(e -> addPossiblevalue("", 1L));
    return button;
  }

}
