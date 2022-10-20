package com.cloudera.frisch.datagen.sink;

import com.cloudera.frisch.datagen.model.Row;

import java.util.List;

public interface SinkInterface {

    void sendOneBatchOfRows(List<Row> rows);

    void terminate();
}
