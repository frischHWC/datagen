package com.cloudera.frisch.randomdatagen.model.type;

import com.cloudera.frisch.randomdatagen.Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hive.jdbc.HivePreparedStatement;
import org.apache.kudu.Type;
import org.apache.kudu.client.PartialRow;
import org.apache.orc.TypeDescription;

import javax.xml.bind.DatatypeConverter;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;

@Slf4j
public class BooleanField extends Field<Boolean> {

    BooleanField(String name, Integer length, List<Boolean> possibleValues, LinkedHashMap<String, Integer> possible_values_weighted) {
        this.name = name;
        this.length = length;
        this.possibleValues = possibleValues;
        this.possible_values_weighted = possible_values_weighted;
    }

    public Boolean generateRandomValue() {
        if(!possibleValues.isEmpty()) {
            return possibleValues.get(random.nextInt(possibleValues.size()));
        } else if (!possible_values_weighted.isEmpty()){
            String result = Utils.getRandomValueWithWeights(random, possible_values_weighted);
            return result.isEmpty() ? false :  Boolean.parseBoolean(result);
        } else {
            return random.nextBoolean();
        }
    }

    /*
     Override if needed Field function to insert into special sinks
     */

    @Override
    public String toStringValue(Boolean value) {
        return value.toString();
    }
    @Override
    public Boolean toCastValue(String value) {
        return Boolean.valueOf(value);
    }

    @Override
    public Put toHbasePut(Boolean value, Put hbasePut) {
        hbasePut.addColumn(Bytes.toBytes(hbaseColumnQualifier), Bytes.toBytes(name), Bytes.toBytes(value));
        return hbasePut;
    }

    @Override
    public PartialRow toKudu(Boolean value, PartialRow partialRow) {
        partialRow.addBoolean(name, value);
        return partialRow;
    }

    @Override
    public Type getKuduType() {
        return Type.BOOL;
    }

    @Override
    public HivePreparedStatement toHive(Boolean value, int index, HivePreparedStatement hivePreparedStatement) {
        try {
            hivePreparedStatement.setBoolean(index, value);
        } catch (SQLException e) {
            log.warn("Could not set value : " +value.toString() + " into hive statement due to error :", e);
        }
        return hivePreparedStatement;
    }

    @Override
    public String getHiveType() {
        return "BOOLEAN";
    }

    @Override
    public String getGenericRecordType() { return "boolean"; }

    @Override
    public ColumnVector getOrcColumnVector(VectorizedRowBatch batch, int cols) {
        return batch.cols[cols];
    }

    @Override
    public TypeDescription getTypeDescriptionOrc() {
        return TypeDescription.createBoolean();
    }

}
