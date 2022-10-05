package com.cloudera.frisch.randomdatagen.parsers;


import com.cloudera.frisch.randomdatagen.model.Model;
import org.apache.log4j.Logger;

/**
 * A parser is an entity able to read one type of file and render a {@see #com.cloudera.frisch.randomdatagen.model.Model}
 * based on that file
 */
public interface Parser {

    // TODO: Implement a yaml parser

    Model renderModelFromFile();
}
