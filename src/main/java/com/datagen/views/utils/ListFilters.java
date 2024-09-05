package com.datagen.views.utils;

import com.vaadin.flow.component.button.Button;
import com.vaadin.flow.component.customfield.CustomField;
import com.vaadin.flow.component.html.Span;
import com.vaadin.flow.component.icon.VaadinIcon;
import com.vaadin.flow.component.orderedlayout.HorizontalLayout;
import com.vaadin.flow.component.orderedlayout.VerticalLayout;
import com.vaadin.flow.component.textfield.TextField;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class ListFilters extends CustomField<List<String>> {

  private final VerticalLayout layout = new VerticalLayout();
  private final HashMap<TextField, TextField> possibleValuesFields = new HashMap<>();


  public String getDescription() {
    return """
        Provide an optional list of filters to apply on the CSV.
        1st input is the column Name to apply the filter on
        2nd input is the column Value that this filter will apply
        Only one filter per column is supported.
        """;
  }

  public ListFilters() {
    layout.add(addNewButton());
    add(layout);
  }

  @Override
  protected List<String> generateModelValue() {
    var list = new ArrayList<String>();
    possibleValuesFields.forEach((key, value) -> list.add(key.getValue()+"="+value.getValue()));
    return list;
  }

  @Override
  protected void setPresentationValue(List<String> values) {
    layout.removeAll();
    possibleValuesFields.clear();

    if (values != null) {
      values.forEach(v -> addPossiblevalue(v.split("=")[0], v.split("=")[1]));
    }

    layout.add(addNewButton());
  }


  private void addPossiblevalue(String colNameValue, String colValueValue) {
    var colName = new TextField();
    colName.setValue(colNameValue);
    colName.setLabel("Column Name");
    var colValue = new TextField();
    colValue.setValue(colValueValue);
    colValue.setLabel("Column Value");
    possibleValuesFields.put(colName, colValue);

    Button removeButton = new Button(VaadinIcon.TRASH.create());
    removeButton.addClickListener(e -> {
      layout.remove(colName.getParent().get());
      possibleValuesFields.remove(colName);
    });

    HorizontalLayout fieldLayout = new HorizontalLayout(colName, new Span("="), colValue, removeButton);
    layout.addComponentAtIndex(layout.getComponentCount()==0?layout.getComponentCount():layout.getComponentCount()-1, fieldLayout);
  }

  private Button addNewButton() {
    Button button = new Button(VaadinIcon.PLUS.create());
    button.addClickListener(e -> addPossiblevalue("", ""));
    return button;
  }

}
