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

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class StringField extends Field<String> {

    StringField(String name, Integer length, List<String> possibleValues, LinkedHashMap<String, Integer> possible_values_weighted) {
        this.name = name;
        if(length==null || length<1) {
            this.length = 20;
        } else {
            this.length = length;
        }
        this.possibleValues = possibleValues;
        this.possible_values_weighted = possible_values_weighted;
    }

    public String generateRandomValue() {
        if(!possibleValues.isEmpty()) {
            return possibleValues.get(random.nextInt(possibleValues.size()));
        } else if (!possible_values_weighted.isEmpty()){
            return Utils.getRandomValueWithWeights(random, possible_values_weighted);
        } else {
            return Utils.getAlphaNumericString(this.length, random);
        }
    }


    /*
     Override if needed Field function to insert into special sinks
     */

    @Override
    public Put toHbasePut(String value, Put hbasePut) {
        hbasePut.addColumn(Bytes.toBytes(hbaseColumnQualifier), Bytes.toBytes(name), Bytes.toBytes(value));
        return hbasePut;
    }

    @Override
    public PartialRow toKudu(String value, PartialRow partialRow) {
        partialRow.addString(name, value);
        return partialRow;
    }

    @Override
    public Type getKuduType() {
        return Type.STRING;
    }

    @Override
    public HivePreparedStatement toHive(String value, int index, HivePreparedStatement hivePreparedStatement) {
        try {
            hivePreparedStatement.setString(index, value);
        } catch (SQLException e) {
            log.warn("Could not set value : " + value + " into hive statement due to error :", e);
        }
        return hivePreparedStatement;
    }

    @Override
    public String getHiveType() {
        return "STRING";
    }

    @Override
    public String getGenericRecordType() { return "string"; }

    @Override
    public ColumnVector getOrcColumnVector(VectorizedRowBatch batch, int cols) {
        return batch.cols[cols];
    }

    @Override
    public TypeDescription getTypeDescriptionOrc() {
        return TypeDescription.createString();
    }
}
