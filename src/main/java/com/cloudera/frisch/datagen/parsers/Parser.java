package com.cloudera.frisch.datagen.parsers;


import com.cloudera.frisch.datagen.model.Model;

/**
 * A parser is an entity able to read one type of file and render a {@see #com.cloudera.frisch.randomdatagen.model.Model}
 * based on that file
 */
public interface Parser {

    // TODO: Implement a yaml parser

    Model renderModelFromFile();
}
