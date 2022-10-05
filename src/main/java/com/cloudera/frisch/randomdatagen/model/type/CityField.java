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
import org.apache.solr.common.SolrInputDocument;

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
public class CityField extends Field<CityField.City> {

    public class City {
        @Getter
        String name;
        @Getter
        String latitude;
        @Getter
        String longitude;
        @Getter
        String country;

        public City(String name, String lat, String lon, String country) {
            this.name = name;
            this.latitude = lat;
            this.longitude = lon;
            this.country = country;
        }

        @Override
        public String toString() {
            return "City{" +
                "name='" + name + '\'' +
                ", latitude='" + latitude + '\'' +
                ", longitude='" + longitude + '\'' +
                ", country='" + country + '\'' +
                '}';
        }
    }

    private List<City> cityDico;


    CityField(String name, Integer length, List<String> filters) {
        this.name = name;
        this.length = length;
        this.cityDico = loadCityDico();

        List<City> possibleCities = new ArrayList<>();
        filters.forEach(filterOnCountry -> {
            possibleCities.addAll(
                this.cityDico.stream()
                .filter(c -> c.country.equalsIgnoreCase(filterOnCountry))
                .collect(Collectors.toList()));
            }
        );
        this.possibleValues = possibleCities;
    }

    private List<City> loadCityDico() {
        try {
            InputStream is = this.getClass().getClassLoader().getResourceAsStream(
                "dictionaries/worldcities.csv");
            return new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
                    .lines()
                    .map(l -> {
                        String[] lineSplitted = l.split(";");
                        return new City(lineSplitted[0], lineSplitted[1], lineSplitted[2], lineSplitted[3]);
                    })
                    .collect(Collectors.toList());
        } catch (Exception e) {
            log.warn("Could not load world cities, error : " + e);
            return Collections.singletonList(new City("world", "0", "0", "world"));
        }
    }

    public City generateRandomValue() {
        if(this.possibleValues.isEmpty()) {
            return cityDico.get(random.nextInt(cityDico.size()));
        } else {
            return possibleValues.get(random.nextInt(possibleValues.size()));
        }
    }

    @Override
    public String toString(City value) {
        return " " + name + " : " + value.getName() + " ;";
    }

    @Override
    public String toCSVString(City value) {
        return value.getName() + ",";
    }

    @Override
    public String toJSONString(City value) {
        return "\"" + name + "\" : " + "\"" + value.getName() + "\", ";
    }

    /*
     Override if needed Field function to insert into special sinks
     */
    @Override
    public String toStringValue(City value) {
        return value.getName();
    }
    @Override
    public City toCastValue(String value) {
        String[] valueSplitted = value.split(";");
        return new City(valueSplitted[0], valueSplitted[1], valueSplitted[2], valueSplitted[3]);
    }

    @Override
    public Put toHbasePut(City value, Put hbasePut) {
        hbasePut.addColumn(Bytes.toBytes(hbaseColumnQualifier), Bytes.toBytes(name), Bytes.toBytes(value.name));
        return hbasePut;
    }

    @Override
    public SolrInputDocument toSolrDoc(City value, SolrInputDocument doc) {
        doc.addField(name, value.getName());
        return doc;
    }

    @Override
    public String toOzone(City value) {
        return value.getName();
    }

    @Override
    public PartialRow toKudu(City value, PartialRow partialRow) {
        partialRow.addString(name, value.getName());
        return partialRow;
    }

    @Override
    public Type getKuduType() {
        return Type.STRING;
    }

    @Override
    public HivePreparedStatement toHive(City value, int index, HivePreparedStatement hivePreparedStatement) {
        try {
            hivePreparedStatement.setString(index, value.getName());
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
    public Object toAvroValue(City value) {
        return value.getName();
    }

    @Override
    public ColumnVector getOrcColumnVector(VectorizedRowBatch batch, int cols) {
        return batch.cols[cols];
    }

    @Override
    public TypeDescription getTypeDescriptionOrc() {
        return TypeDescription.createString();
    }


}