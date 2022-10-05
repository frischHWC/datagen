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
public class IncrementLongField extends Field<Long> {

    private Long counter = 0L;

    IncrementLongField(String name, Integer length, List<Long> possibleValues, String min, String max) {
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
        counter = this.min;
        this.possibleValues = possibleValues;
    }

    public Long generateRandomValue() {
        counter++;
        return counter;
    }

    /*
     Override if needed Field function to insert into special sinks
     */
    @Override
    public String toStringValue(Long value) {
        return value.toString();
    }
    @Override
    public Long toCastValue(String value) {
        return Long.valueOf(value);
    }

    @Override
    public Put toHbasePut(Long value, Put hbasePut) {
        hbasePut.addColumn(Bytes.toBytes(hbaseColumnQualifier), Bytes.toBytes(name), Bytes.toBytes(value));
        return hbasePut;
    }

    @Override
    public PartialRow toKudu(Long value, PartialRow partialRow) {
        partialRow.addLong(name, value);
        return partialRow;
    }

    @Override
    public Type getKuduType() {
        return Type.INT64;
    }

    @Override
    public HivePreparedStatement toHive(Long value, int index, HivePreparedStatement hivePreparedStatement) {
        try {
            hivePreparedStatement.setLong(index, value);
        } catch (SQLException e) {
            log.warn("Could not set value : " +value.toString() + " into hive statement due to error :", e);
        }
        return hivePreparedStatement;
    }

    @Override
    public String getHiveType() {
        return "BIGINT";
    }

    @Override
    public String getGenericRecordType() { return "long"; }

    @Override
    public ColumnVector getOrcColumnVector(VectorizedRowBatch batch, int cols) {
        return batch.cols[cols];
    }

    @Override
    public TypeDescription getTypeDescriptionOrc() {
        return TypeDescription.createLong();
    }
}

