package com.datagen.views.models;

import com.datagen.model.OptionsConverter;
import com.datagen.model.type.FieldRepresentation;
import com.datagen.views.utils.ListFilters;
import com.datagen.views.utils.ListPossibleValues;
import com.vaadin.flow.component.Component;
import com.vaadin.flow.component.HasText;
import com.vaadin.flow.component.button.Button;
import com.vaadin.flow.component.combobox.ComboBox;
import com.vaadin.flow.component.combobox.MultiSelectComboBox;
import com.vaadin.flow.component.datepicker.DatePicker;
import com.vaadin.flow.component.datetimepicker.DateTimePicker;
import com.vaadin.flow.component.html.Span;
import com.vaadin.flow.component.icon.VaadinIcon;
import com.vaadin.flow.component.orderedlayout.FlexComponent;
import com.vaadin.flow.component.orderedlayout.HorizontalLayout;
import com.vaadin.flow.component.radiobutton.RadioButtonGroup;
import com.vaadin.flow.component.textfield.*;
import com.vaadin.flow.data.binder.Binder;
import com.vaadin.flow.data.validator.IntegerRangeValidator;
import com.vaadin.flow.data.value.ValueChangeMode;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class ModelsUtils {

  private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
  private static final Pattern datePatternInput = Pattern.compile("[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}");

  /**
   * Create and set info for a given parameter
   * @param param
   * @return
   */
  public static HorizontalLayout createInfoForAParameter(Component param, String description) {
    // Horizontal layout for a field and its info
    var hl = new HorizontalLayout();
    hl.setPadding(true);

    // Info
    var textArea = new Span(description);
    textArea.setWhiteSpace(HasText.WhiteSpace.PRE);
    textArea.setVisible(false);
    var infoButton = new Button(VaadinIcon.INFO_CIRCLE_O.create());
    infoButton.setWidth("5%");
    infoButton.setMaxWidth("5%");
    infoButton.setTooltipText("Helper");
    infoButton.addClickListener(clickInfo -> textArea.setVisible(!textArea.isVisible()));

    // Finally, add the parameters one by one
    hl.add(param, infoButton, textArea);
    hl.setAlignItems(FlexComponent.Alignment.BASELINE);

    return hl;
  }

  static RadioButtonGroup<Boolean> createGhost(Binder<FieldRepresentation> binder) {
    RadioButtonGroup<Boolean> radioGroup = new RadioButtonGroup<>();
    radioGroup.setLabel("Ghost");
    radioGroup.setItems(true, false);
    radioGroup.setValue(false);
    binder.forField(radioGroup)
        .bind(
            FieldRepresentation::getGhost,
            (f, m) -> f.setGhost(m != null ? m : false)
        );
    return radioGroup;
  }


  /**
   * Create an integer parameter binding to minimum of field
   * @param paramLabel
   * @param binder
   * @param min
   * @param max
   * @return
   */
  static IntegerField createMinInt(String paramLabel, Binder<FieldRepresentation> binder, int min, int max) {
    var intField = new IntegerField(paramLabel);
    intField.setClearButtonVisible(true);
    binder.forField(intField)
        .withValidator(
            new IntegerRangeValidator(
                "Provide a valid value between " + min + " & " + max,
                min, max))
        .bind(
            f -> f.getMin() != null ? f.getMin().intValue() : null,
            (f, m) -> f.setMin(m != null ? m.longValue() : null)
        );
    return intField;
  }

  /**
   * Create an integer parameter binding to maximum of field
   * @param paramLabel
   * @param binder
   * @param min
   * @param max
   * @return
   */
  static IntegerField createMaxInt(String paramLabel, Binder<FieldRepresentation> binder, int min, int max) {
    var intField = new IntegerField(paramLabel);
    intField.setClearButtonVisible(true);
    binder.forField(intField)
        .withValidator(
            new IntegerRangeValidator(
                "Provide a valid value between " + min + " & " + max,
                min, max))
        .bind(
            f -> f.getMax() != null ? f.getMax().intValue() : null,
            (f, m) -> f.setMax(m != null ? m.longValue() : null)
        );
    return intField;
  }

  /**
   * Create a long parameter binding to minimum of field
   * @param paramLabel
   * @param binder
   * @param min
   * @param max
   * @return
   */
  static NumberField createMinLong(String paramLabel, Binder<FieldRepresentation> binder, long min, long max) {
    var intField = new NumberField(paramLabel);
    intField.setClearButtonVisible(true);
    binder.forField(intField)
        .withValidator(v -> v > min && v < max,
                "Provide a valid value between " + min + " & " + max)
        .bind(
            f -> f.getMin() != null ? Double.valueOf(f.getMin()) : null,
            (f, m) -> f.setMin(m != null ? m.longValue() : null)
        );
    return intField;
  }

  /**
   * Create a long parameter binding to maximum of field
   * @param paramLabel
   * @param binder
   * @param min
   * @param max
   * @return
   */
  static NumberField createMaxLong(String paramLabel, Binder<FieldRepresentation> binder, long min, long max) {
    var intField = new NumberField(paramLabel);
    intField.setClearButtonVisible(true);
    binder.forField(intField)
        .withValidator(v -> v > min && v < max,
            "Provide a valid value between " + min + " & " + max)
        .bind(
            f -> f.getMax() != null ? Double.valueOf(f.getMax()) : null,
            (f, m) -> f.setMax(m != null ? m.longValue() : null)
        );
    return intField;
  }

  /**
   * Create a long parameter binding to minimum of field
   * @param paramLabel
   * @param binder
   * @param min
   * @param max
   * @return
   */
  static NumberField createMinFloat(String paramLabel, Binder<FieldRepresentation> binder, float min, float max) {
    var intField = new NumberField(paramLabel);
    intField.setClearButtonVisible(true);
    binder.forField(intField)
        .withValidator(v -> v > min && v < max,
            "Provide a valid value between " + min + " & " + max)
        .bind(
            f -> f.getMin() != null ? Double.valueOf(f.getMin()) : null,
            (f, m) -> f.setMin(m != null ? m.longValue() : null)
        );
    return intField;
  }

  /**
   * Create a long parameter binding to maximum of field
   * @param paramLabel
   * @param binder
   * @param min
   * @param max
   * @return
   */
  static NumberField createMaxFloat(String paramLabel, Binder<FieldRepresentation> binder, float min, float max) {
    var intField = new NumberField(paramLabel);
    intField.setClearButtonVisible(true);
    binder.forField(intField)
        .withValidator(v -> v > min && v < max,
            "Provide a valid value between " + min + " & " + max)
        .bind(
            f -> f.getMax() != null ? Double.valueOf(f.getMax()) : null,
            (f, m) -> f.setMax(m != null ? m.longValue() : null)
        );
    return intField;
  }

  /**
   * Create an integer parameter binding to length of field
   * @param paramLabel
   * @param binder
   * @param min
   * @param max
   * @return
   */
  static IntegerField createLengthInt(String paramLabel, Binder<FieldRepresentation> binder, int min, int max) {
    var intField = new IntegerField(paramLabel);
    intField.setClearButtonVisible(true);
    binder.forField(intField)
        .withValidator(
            new IntegerRangeValidator(
                "Provide a valid value between " + min + " & " + max,
                Integer.MIN_VALUE, Integer.MAX_VALUE))
        .bind(
            FieldRepresentation::getLength,
            FieldRepresentation::setLength
        );
    return intField;
  }


  /**
   * Create a list of possible values
   * @param paramLabel
   * @param binder
   * @return
   */
  static ListPossibleValues createList(String paramLabel, Binder<FieldRepresentation> binder) {
    ListPossibleValues possibleValues = new ListPossibleValues();
    possibleValues.setLabel(paramLabel);
    binder.forField(possibleValues)
        .bind(FieldRepresentation::getPossibleValuesWeighted, FieldRepresentation::setPossibleValuesWeighted);

    return possibleValues;
  }

  /**
   * Create a string parameter binding to min date
   * @param paramLabel
   * @param binder
   * @return
   */
  static DatePicker createMinBirthdate(String paramLabel, Binder<FieldRepresentation> binder) {
    DatePicker datePicker = new DatePicker(paramLabel);
    datePicker.setClearButtonVisible(true);
    datePicker.setValue(LocalDate.of(1920, 1, 1));
    binder.forField(datePicker)
        .bind(
            FieldRepresentation::getMinDate,
            FieldRepresentation::setMinDate
        );
    return datePicker;
  }

  /**
   * Create a string parameter binding to max date
   * @param paramLabel
   * @param binder
   * @return
   */
  static DatePicker createMaxBirthdate(String paramLabel, Binder<FieldRepresentation> binder) {
    DatePicker datePicker = new DatePicker(paramLabel);
    datePicker.setClearButtonVisible(true);
    datePicker.setValue(LocalDate.of(2024, 1, 1));
    binder.forField(datePicker)
        .bind(
            FieldRepresentation::getMaxDate,
            FieldRepresentation::setMaxDate
        );
    return datePicker;
  }

  /**
   * Create a multi select box with given filters
   * @param paramLabel
   * @param binder
   * @return
   */
  static  MultiSelectComboBox<String> createFilters(String paramLabel, Binder<FieldRepresentation> binder, List<String> filters) {
    MultiSelectComboBox<String> comboBoxCountry = new MultiSelectComboBox<>();
    comboBoxCountry.setLabel(paramLabel);
    comboBoxCountry.setItems(filters);
    binder.forField(comboBoxCountry)
        .bind(f -> new HashSet<>(f.getFilters()), (f, m) -> f.setFilters(m.stream().toList()));

    return comboBoxCountry;
  }

  /**
   * Create a link field
   * @param paramLabel
   * @param binder
   * @return
   */
  static TextField createLinkField(String paramLabel, Binder<FieldRepresentation> binder) {
    var textField = new TextField(paramLabel);
    textField.setClearButtonVisible(true);
    textField.setRequired(true);
    binder.forField(textField)
        .withValidator(v -> v!=null && v.startsWith("$") , "Must not be empty and start with a $.")
        .bind(
            FieldRepresentation::getLink,
            FieldRepresentation::setLink
        );
    return textField;
  }

  /**
   * Create a file field
   * @param paramLabel
   * @param binder
   * @return
   */
  static TextArea createFileField(String paramLabel, Binder<FieldRepresentation> binder) {
    var textField = new TextArea(paramLabel);
    textField.setClearButtonVisible(true);
    textField.setRequired(true);
    binder.forField(textField)
        .withValidator(v -> v!=null , "Must not be empty")
        .bind(
            FieldRepresentation::getFile,
            FieldRepresentation::setFile
        );
    return textField;
  }

  /**
   * Create a main field
   * @param paramLabel
   * @param binder
   * @return
   */
  static TextField createMainField(String paramLabel, Binder<FieldRepresentation> binder) {
    var textField = new TextField(paramLabel);
    textField.setClearButtonVisible(true);
    textField.setRequired(true);
    binder.forField(textField)
        .withValidator(v -> v!=null , "Must not be empty")
        .bind(
            FieldRepresentation::getMainField,
            FieldRepresentation::setMainField
        );
    return textField;
  }

  /**
   * Create a main field
   * @param paramLabel
   * @param binder
   * @return
   */
  static TextField createSeparatorField(String paramLabel, Binder<FieldRepresentation> binder) {
    var textField = new TextField(paramLabel);
    textField.setClearButtonVisible(true);
    binder.forField(textField)
        .bind(
            FieldRepresentation::getSeparator,
            FieldRepresentation::setSeparator
        );
    return textField;
  }

  /**
   * Create a list of filters (used for CSV)
   * @param paramLabel
   * @param binder
   * @return
   */
  static ListFilters createListOfFilters(String paramLabel, Binder<FieldRepresentation> binder) {
    ListFilters possibleValues = new ListFilters();
    possibleValues.setLabel(paramLabel);
    binder.forField(possibleValues)
        .bind(FieldRepresentation::getFilters, FieldRepresentation::setFilters);

    return possibleValues;
  }

  /**
   * Create a string parameter binding to min date
   * @param paramLabel
   * @param binder
   * @return
   */
  static DateTimePicker createMinDate(String paramLabel, Binder<FieldRepresentation> binder) {
    DateTimePicker datePicker = new DateTimePicker(paramLabel);
    datePicker.setValue(LocalDateTime.of(1970,1,1, 0,0,0));
    binder.forField(datePicker)
        .bind(
            FieldRepresentation::getMinDateTime,
            FieldRepresentation::setMinDateTime
        );
    return datePicker;
  }

  /**
   * Create a string parameter binding to max date
   * @param paramLabel
   * @param binder
   * @return
   */
  static DateTimePicker createMaxDate(String paramLabel, Binder<FieldRepresentation> binder) {
    DateTimePicker datePicker = new DateTimePicker(paramLabel);
    datePicker.setValue(LocalDateTime.of(2030,12,31, 23,59,59));
    binder.forField(datePicker)
        .bind(
            FieldRepresentation::getMaxDateTime,
            FieldRepresentation::setMaxDateTime
        );
    return datePicker;
  }

  /**
   * Create a string parameter binding to max date
   * @param paramLabel
   * @param binder
   * @return
   */
  static RadioButtonGroup<Boolean> createUseNow(String paramLabel, Binder<FieldRepresentation> binder) {
    RadioButtonGroup<Boolean> radioGroup = new RadioButtonGroup<>();
    radioGroup.setLabel(paramLabel);
    radioGroup.setItems(true, false);
    radioGroup.setValue(false);
    binder.forField(radioGroup)
        .bind(
            FieldRepresentation::getUseNow,
            (f, m) -> f.setUseNow(m != null ? m : false)
        );
    return radioGroup;
  }


  /**
   * Create a string parameter binding to pattern of a date
   * @param paramLabel
   * @param binder
   * @return
   */
  static TextField createPatterDate(String paramLabel, Binder<FieldRepresentation> binder) {
    var textField = new TextField(paramLabel);
    textField.setClearButtonVisible(true);
    binder.forField(textField)
        .bind(
            FieldRepresentation::getPattern,
            FieldRepresentation::setPattern
        );
    return textField;
  }

  /**
   * Create a string parameter binding to pattern of a date
   * @param paramLabel
   * @param binder
   * @return
   */
  static TextField createRegex(String paramLabel, Binder<FieldRepresentation> binder) {
    var textField = new TextField(paramLabel);
    textField.setClearButtonVisible(true);
    textField.setRequired(true);
    binder.forField(textField)
        .bind(
            FieldRepresentation::getRegex,
            FieldRepresentation::setRegex
        );
    return textField;
  }

  /**
   * Create a string parameter binding to url
   * @param paramLabel
   * @param binder
   * @return
   */
  static TextField createURL(String paramLabel, Binder<FieldRepresentation> binder) {
    var textField = new TextField(paramLabel);
    textField.setClearButtonVisible(true);
    binder.forField(textField)
        .bind(
            FieldRepresentation::getUrl,
            FieldRepresentation::setUrl
        );
    return textField;
  }

  /**
   * Create a string parameter binding to user
   * @param paramLabel
   * @param binder
   * @return
   */
  static TextField createUsername(String paramLabel, Binder<FieldRepresentation> binder) {
    var textField = new TextField(paramLabel);
    textField.setClearButtonVisible(true);
    binder.forField(textField)
        .bind(
            FieldRepresentation::getUser,
            FieldRepresentation::setUser
        );
    return textField;
  }

  /**
   * Create a string parameter binding to password
   * @param paramLabel
   * @param binder
   * @return
   */
  static PasswordField createPassword(String paramLabel, Binder<FieldRepresentation> binder) {
    var textField = new PasswordField(paramLabel);
    textField.setClearButtonVisible(true);
    binder.forField(textField)
        .bind(
            FieldRepresentation::getPassword,
            FieldRepresentation::setPassword
        );
    return textField;
  }

  /**
   * Create a string parameter binding to request
   * @param paramLabel
   * @param binder
   * @return
   */
  static TextArea createRequest(String paramLabel, Binder<FieldRepresentation> binder) {
    var textField = new TextArea(paramLabel);
    textField.setClearButtonVisible(true);
    textField.setRequired(true);
    binder.forField(textField)
        .bind(
            FieldRepresentation::getRequest,
            FieldRepresentation::setRequest
        );
    return textField;
  }

  /**
   * Create a string parameter binding to model type
   * @param paramLabel
   * @param binder
   * @return
   */
  static TextArea createContext(String paramLabel, Binder<FieldRepresentation> binder) {
    var textField = new TextArea(paramLabel);
    textField.setClearButtonVisible(true);
    binder.forField(textField)
        .bind(
            FieldRepresentation::getContext,
            FieldRepresentation::setContext
        );
    return textField;
  }

  /**
   * Create a string parameter binding to model type
   * @param paramLabel
   * @param binder
   * @return
   */
  static TextArea createModelType(String paramLabel, Binder<FieldRepresentation> binder, String defaultValue) {
    var textField = new TextArea(paramLabel);
    textField.setClearButtonVisible(true);
    textField.setValue(defaultValue);
    binder.forField(textField)
        .bind(
            FieldRepresentation::getModelType,
            FieldRepresentation::setModelType
        );
    return textField;
  }

  /**
   * Create a string parameter binding to temperature
   * @param paramLabel
   * @param binder
   * @return
   */
  static NumberField createTemperature(String paramLabel, Binder<FieldRepresentation> binder) {
    var floatField = new NumberField(paramLabel);
    floatField.setValue(0.5);
    floatField.setClearButtonVisible(true);
    binder.forField(floatField)
        .bind(
            f -> f.getTemperature() != null ? Double.valueOf(f.getTemperature()) : null,
            (f, m) -> f.setTemperature(m != null ? m.floatValue() : null)
        );
    return floatField;
  }

  /**
   * Create a string parameter binding to frequence penalty
   * @param paramLabel
   * @param binder
   * @return
   */
  static NumberField createFrequencyPenalty(String paramLabel, Binder<FieldRepresentation> binder) {
    var floatField = new NumberField(paramLabel);
    floatField.setClearButtonVisible(true);
    floatField.setValue(1.0);
    binder.forField(floatField)
        .bind(
            f -> f.getFrequencyPenalty() != null ? Double.valueOf(f.getFrequencyPenalty()) : null,
            (f, m) -> f.setFrequencyPenalty(m != null ? m.floatValue() : null)
        );
    return floatField;
  }

  /**
   * Create a string parameter binding to presence penalty
   * @param paramLabel
   * @param binder
   * @return
   */
  static NumberField createPresencePenalty(String paramLabel, Binder<FieldRepresentation> binder) {
    var floatField = new NumberField(paramLabel);
    floatField.setClearButtonVisible(true);
    floatField.setValue(1.0);
    binder.forField(floatField)
        .bind(
            f -> f.getPresencePenalty() != null ? Double.valueOf(f.getPresencePenalty()) : null,
            (f, m) -> f.setPresencePenalty(m != null ? m.floatValue() : null)
        );
    return floatField;
  }

  /**
   * Create a string parameter binding to topP
   * @param paramLabel
   * @param binder
   * @return
   */
  static NumberField createTopP(String paramLabel, Binder<FieldRepresentation> binder) {
    var floatField = new NumberField(paramLabel);
    floatField.setClearButtonVisible(true);
    floatField.setValue(1.0);
    binder.forField(floatField)
        .bind(
            f -> f.getTopP() != null ? Double.valueOf(f.getTopP()) : null,
            (f, m) -> f.setTopP(m != null ? m.floatValue() : null)
        );
    return floatField;
  }

  /**
   * Create a string parameter binding to max Tokens
   * @param paramLabel
   * @param binder
   * @return
   */
  static NumberField createMaxTokens(String paramLabel, Binder<FieldRepresentation> binder) {
    var floatField = new NumberField(paramLabel);
    floatField.setClearButtonVisible(true);
    floatField.setValue(256.0);
    binder.forField(floatField)
        .bind(
            f -> f.getMaxTokens() != null ? Double.valueOf(f.getMaxTokens()) : null,
            (f, m) -> f.setMaxTokens(m != null ? m.intValue() : null)
        );
    return floatField;
  }

  /**
   * Create a string parameter binding to injection
   * @param paramLabel
   * @param binder
   * @return
   */
  static TextArea createInjecter(String paramLabel, Binder<FieldRepresentation> binder) {
    var textField = new TextArea(paramLabel);
    textField.setClearButtonVisible(true);
    textField.setValueChangeMode(ValueChangeMode.EAGER);
    binder.forField(textField)
        .bind(
            f -> f.getInjection()!=null&&!f.getInjection().isEmpty()?f.getInjection():null,
            (f,m) -> f.setInjection(m!=null&&!m.isEmpty()?m:null)
        );
    return textField;
  }

  /**
   * Create a string parameter binding to formula
   * @param paramLabel
   * @param binder
   * @return
   */
  static TextArea createFormula(String paramLabel, Binder<FieldRepresentation> binder) {
    var textField = new TextArea(paramLabel);
    textField.setClearButtonVisible(true);
    binder.forField(textField)
        .bind(
            f -> f.getFormula()!=null&&!f.getFormula().isEmpty()?f.getFormula():null,
            (f,m) -> f.setFormula(m!=null&&!m.isEmpty()?m:null)
        );
    return textField;
  }


  /**
   * A generic Radio Group used by connectors configurations
   * @param label
   * @param defaultValue
   * @param optionName
   * @param binder
   * @return
   */
  static RadioButtonGroup<Boolean> createGenericBooleanOptionProps(
      String label,
      Boolean defaultValue,
      OptionsConverter.Options optionName,
      Binder<Map<OptionsConverter.Options, Object>> binder
  ) {
    RadioButtonGroup<Boolean> radioGroup = new RadioButtonGroup<>();
    radioGroup.setLabel(label);
    radioGroup.setRequired(false);
    radioGroup.setItems(true, false);
    radioGroup.setValue(defaultValue);
    binder.forField(radioGroup)
        .bind(
            c -> c.get(optionName)==null?defaultValue:Boolean.valueOf(c.get(optionName).toString()),
            (c, m) -> c.put(optionName, m)
        );
    return radioGroup;
  }

  static IntegerField createGenericIntegerOptionProps(
      String label,
      Integer defaultValue,
      OptionsConverter.Options optionName,
      Binder<Map<OptionsConverter.Options, Object>> binder
  ) {
    IntegerField integerField = new IntegerField();
    integerField.setLabel(label);
    integerField.setRequired(false);
    if (defaultValue != null) {
      integerField.setValue(defaultValue);
    }
    binder.forField(integerField)
        .bind(
            c -> c.get(optionName)==null?defaultValue:Integer.valueOf(c.get(optionName).toString()),
            (c, m) -> { if(m!=null) { c.put(optionName, m);} }
        );
    return integerField;
  }

  static TextField createGenericStringOptionProps(
      String label,
      String defaultValue,
      OptionsConverter.Options optionName,
      Binder<Map<OptionsConverter.Options, Object>> binder
  ) {
    TextField textField = new TextField();
    textField.setLabel(label);
    textField.setRequired(false);
    if (defaultValue != null) {
      textField.setValue(defaultValue);
    }
    binder.forField(textField)
        .bind(
            c -> c.get(optionName)==null?defaultValue:c.get(optionName).toString(),
            (c, m) -> { if(m!=null) { c.put(optionName, m);} }
        );
    return textField;
  }

  static ComboBox<String> createGenericComboStringOptionProps(
      String label,
      String defaultValue,
      OptionsConverter.Options optionName,
      Binder<Map<OptionsConverter.Options, Object>> binder,
      String ... values
  ) {
    ComboBox<String> textField = new ComboBox<>();
    textField.setLabel(label);
    textField.setRequired(false);
    textField.setItems(values);
    if (defaultValue != null) {
      textField.setValue(defaultValue);
    }
    binder.forField(textField)
        .bind(
            c -> c.get(optionName)==null?defaultValue:c.get(optionName).toString(),
            (c, m) -> { if(m!=null) { c.put(optionName, m);} }
        );
    return textField;
  }

  static ComboBox<String> createGenericComboStringOptionProps(
      String label,
      String defaultValue,
      OptionsConverter.TableNames optionName,
      Binder<Map<OptionsConverter.TableNames, String>> binder,
      String ... values
  ) {
    ComboBox<String> textField = new ComboBox<>();
    textField.setLabel(label);
    textField.setRequired(false);
    textField.setItems(values);
    if (defaultValue != null) {
      textField.setValue(defaultValue);
    }
    binder.forField(textField)
        .bind(
            c -> c.get(optionName)==null?defaultValue:c.get(optionName).toString(),
            (c, m) -> { if(m!=null) { c.put(optionName, m);} }
        );
    return textField;
  }

  static TextField createGenericStringTableNamesProps(
      String label,
      String defaultValue,
      OptionsConverter.TableNames optionName,
      Binder<Map<OptionsConverter.TableNames, String>> binder
  ) {
    TextField textField = new TextField();
    textField.setLabel(label);
    textField.setRequired(false);
    if (defaultValue != null) {
      textField.setValue(defaultValue);
    }
    binder.forField(textField)
        .bind(
            c -> c.get(optionName)==null?defaultValue:c.get(optionName),
            (c, m) -> { if(m!=null) { c.put(optionName, m);} }
        );
    return textField;
  }

  // TODO: Make upload of file for GCS account key
  static TextField createGenericFileUpload(
      String label,
      String defaultValue,
      OptionsConverter.TableNames optionName,
      Binder<Map<OptionsConverter.TableNames, String>> binder
  ) {
    TextField textField = new TextField();
    textField.setLabel(label);
    textField.setRequired(false);
    if (defaultValue != null) {
      textField.setValue(defaultValue);
    }
    binder.forField(textField)
        .bind(
            c -> c.get(optionName)==null?defaultValue:c.get(optionName),
            (c, m) -> { if(m!=null) { c.put(optionName, m);} }
        );
    return textField;
  }


}
