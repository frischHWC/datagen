package com.cloudera.frisch.randomdatagen.model.type;

import lombok.Getter;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class PhoneField extends Field<String> {

    public class Phone {
        @Getter
        String indicator;
        @Getter
        String country;

        public Phone(String indicator, String country) {
            this.indicator = indicator;
            this.country = country;
        }

        @Override
        public String toString() {
            return "Name{" +
                "indicator='" + indicator + '\'' +
                ", country='" + country + '\'' +
                '}';
        }
    }

    private List<Phone> phoneIndicatorDico;

    PhoneField(String name, Integer length, List<String> filters) {
        this.name = name;
        this.length = length;
        this.phoneIndicatorDico = loadPhoneDico();

        this.possibleValues = new ArrayList<>();

        filters.forEach(filterOnCountry -> {
            this.possibleValues.addAll(
                phoneIndicatorDico.stream().filter(n -> n.country.equalsIgnoreCase(filterOnCountry))
                    .map(n -> n.indicator)
                    .collect(Collectors.toList()));
        });
    }

    public String generateRandomValue() {
        String indicator = possibleValues.get(random.nextInt(possibleValues.size()));
        StringBuffer sb = new StringBuffer();
        sb.append("+");
        sb.append(indicator);
        sb.append(" ");
        int counter = 1;
        while (counter <= (11-indicator.length())) {
            sb.append(random.nextInt(10));
            counter++;
        }

        return sb.toString();
    }

    private List<Phone> loadPhoneDico() {
        try {
            InputStream is = this.getClass().getClassLoader().getResourceAsStream(
                "dictionaries/phone-country-codes.csv");
            return new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
                    .lines()
                    .map(l -> {
                        String[] lineSplitted = l.split(";");
                        return new PhoneField.Phone(lineSplitted[0], lineSplitted[1]);
                    })
                    .collect(Collectors.toList());
        } catch (Exception e) {
            log.warn("Could not load names-dico with error : " + e);
            return Collections.singletonList(new PhoneField.Phone("00", ""));
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
            log.warn("Could not set value : " +value.toString() + " into hive statement due to error :", e);
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