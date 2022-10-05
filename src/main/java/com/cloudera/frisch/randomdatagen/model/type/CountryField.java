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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;


@Slf4j
public class CountryField extends Field<String> {

    private List<String> countryDico;

    CountryField(String name, Integer length, List<String> possibleValues) {
        this.name = name;
        this.length = length;
        this.possibleValues = possibleValues;
        this.countryDico = possibleValues.isEmpty() ? loadCountryDico() : possibleValues;
    }

    public String generateRandomValue() {
        return countryDico.get(random.nextInt(countryDico.size()));
    }

    private List<String> loadCountryDico() {
        try {
            InputStream is = this.getClass().getClassLoader().getResourceAsStream(
                "dictionaries/country-dico.txt");
            return new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
                    .lines()
                    .collect(Collectors.toList());
        } catch (Exception e) {
            log.warn("Could not load country-dico with error : " + e);
            return Collections.singletonList("World");
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