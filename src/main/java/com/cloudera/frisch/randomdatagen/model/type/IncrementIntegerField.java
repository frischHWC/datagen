package com.cloudera.frisch.randomdatagen.model.type;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hive.jdbc.HivePreparedStatement;
import org.apache.kudu.Type;
import org.apache.kudu.client.PartialRow;
import org.apache.orc.TypeDescription;

import java.sql.SQLException;
import java.util.List;

@Slf4j
public class IncrementIntegerField extends Field<Integer> {

    private Integer counter = 0;

    IncrementIntegerField(String name, Integer length, List<Integer> possibleValues, String min, String max) {
        this.name = name;
        if(length==null || length==-1) {
            this.length = Integer.MAX_VALUE;
        } else {
            this.length = length;
        }
        if(min==null) {
            this.min = 0L;
        } else {
            this.min = Long.parseLong(min);
        }
        counter = this.min.intValue();
        this.possibleValues = possibleValues;
    }

    public Integer generateRandomValue() {
        counter++;
        return counter;
    }

    /*
    Override if needed Field function to insert into special sinks
    */

    @Override
    public String toStringValue(Integer value) {
        return value.toString();
    }
    @Override
    public Integer toCastValue(String value) {
        return Integer.valueOf(value);
    }

    @Override
    public Put toHbasePut(Integer value, Put hbasePut) {
        hbasePut.addColumn(Bytes.toBytes(hbaseColumnQualifier), Bytes.toBytes(name), Bytes.toBytes(value));
        return hbasePut;
    }

    @Override
    public PartialRow toKudu(Integer value, PartialRow partialRow) {
        partialRow.addInt(name, value);
        return partialRow;
    }

    @Override
    public Type getKuduType() {
        return Type.INT32;
    }

    @Override
    public HivePreparedStatement toHive(Integer value, int index, HivePreparedStatement hivePreparedStatement) {
        try {
            hivePreparedStatement.setInt(index, value);
        } catch (SQLException e) {
            log.warn("Could not set value : " +value.toString() + " into hive statement due to error :", e);
        }
        return hivePreparedStatement;
    }

    @Override
    public String getHiveType() {
        return "INT";
    }

    @Override
    public String getGenericRecordType() { return "int"; }

    @Override
    public ColumnVector getOrcColumnVector(VectorizedRowBatch batch, int cols) {
        return batch.cols[cols];
    }

    @Override
    public TypeDescription getTypeDescriptionOrc() {
        return TypeDescription.createInt();
    }

}
