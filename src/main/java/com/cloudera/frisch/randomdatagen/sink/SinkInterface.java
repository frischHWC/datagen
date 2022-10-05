package com.cloudera.frisch.randomdatagen.sink;

import com.cloudera.frisch.randomdatagen.model.Row;
import org.apache.log4j.Logger;

import java.util.List;

public interface SinkInterface {

    void sendOneBatchOfRows(List<Row> rows);

    void terminate();
}
