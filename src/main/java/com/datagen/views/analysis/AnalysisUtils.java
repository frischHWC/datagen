package com.datagen.views.analysis;

import com.vaadin.flow.component.textfield.TextField;
import com.vaadin.flow.data.binder.Binder;

public class AnalysisUtils {

  static TextField createPath(
      Binder<AnalysisView.InternalAnalysis> binderAnalysis) {

    var pathText = new TextField("Path:");
    pathText.setClearButtonVisible(true);
    pathText.setRequired(true);
    pathText.setTooltipText("Path to files to analyze in format: /tmp/test/myfile.parquet");
    binderAnalysis.forField(pathText)
        .bind(
            AnalysisView.InternalAnalysis::getFilepath,
            AnalysisView.InternalAnalysis::setFilepath
        );
    return pathText;
  }

  static TextField createVolume(
      Binder<AnalysisView.InternalAnalysis> binderAnalysis) {

    var pathText = new TextField("Volume:");
    pathText.setClearButtonVisible(true);
    pathText.setRequired(true);
    pathText.setTooltipText("Volume where files to analyze are, in format: volume");
    binderAnalysis.forField(pathText)
        .bind(
            AnalysisView.InternalAnalysis::getVolume,
            AnalysisView.InternalAnalysis::setVolume
        );
    return pathText;
  }

  static TextField createBucket(
      Binder<AnalysisView.InternalAnalysis> binderAnalysis) {

    var pathText = new TextField("Bucket:");
    pathText.setClearButtonVisible(true);
    pathText.setRequired(true);
    pathText.setTooltipText("Bucket where files to analyze are, in format: bucket");
    binderAnalysis.forField(pathText)
        .bind(
            AnalysisView.InternalAnalysis::getBucket,
            AnalysisView.InternalAnalysis::setBucket
        );
    return pathText;
  }

  static TextField createKey(
      Binder<AnalysisView.InternalAnalysis> binderAnalysis) {

    var pathText = new TextField("Key:");
    pathText.setClearButtonVisible(true);
    pathText.setRequired(true);
    pathText.setTooltipText("Key to analyze, in format: key");
    binderAnalysis.forField(pathText)
        .bind(
            AnalysisView.InternalAnalysis::getKey,
            AnalysisView.InternalAnalysis::setKey
        );
    return pathText;
  }

  static TextField createDatabase(
      Binder<AnalysisView.InternalAnalysis> binderAnalysis) {

    var pathText = new TextField("Database:");
    pathText.setClearButtonVisible(true);
    pathText.setRequired(true);
    pathText.setTooltipText("Database where data to analyze is, in format: database");
    binderAnalysis.forField(pathText)
        .bind(
            AnalysisView.InternalAnalysis::getDatabase,
            AnalysisView.InternalAnalysis::setDatabase
        );
    return pathText;
  }

  static TextField createTable(
      Binder<AnalysisView.InternalAnalysis> binderAnalysis) {

    var pathText = new TextField("Table:");
    pathText.setClearButtonVisible(true);
    pathText.setRequired(true);
    pathText.setTooltipText("Table where data to analyze is, in format: table");
    binderAnalysis.forField(pathText)
        .bind(
            AnalysisView.InternalAnalysis::getTable,
            AnalysisView.InternalAnalysis::setTable
        );
    return pathText;
  }





}
