#!/bin/bash

CMD=$1
SERVER_PORT=$2
TLS_ENABLED=$3
ADMIN_USER=$4
ADMIN_PASSWORD=$5

set -x
. ${COMMON_SCRIPT}
PYTHON_COMMAND_INVOKER=${PYTHON_COMMAND_INVOKER:-/usr/bin/python}

case $CMD in
 (gen_customer_hdfs_ozone_hive)
     echo "Starting to Generate Customer data to HDFS in Parquet & Hive & Ozone in Parquet"

     ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/customer/customer-china-model.json 10000 12 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} hdfs-parquet ozone-parquet hive
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-china-model.json"
       exit 1
     fi

     ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/customer/customer-france-model.json 10000 9 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} hdfs-parquet ozone-parquet hive
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-france-model.json"
       exit 1
     fi

     ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/customer/customer-germany-model.json 10000 9 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} hdfs-parquet ozone-parquet hive
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-germany-model.json"
       exit 1
     fi

     ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/customer/customer-india-model.json 10000 9 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} hdfs-parquet ozone-parquet hive
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-india-model.json"
       exit 1
     fi

     ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/customer/customer-italy-model.json 10000 3 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} hdfs-parquet ozone-parquet hive
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-italy-model.json"
       exit 1
     fi

     ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/customer/customer-japan-model.json 10000 11 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} hdfs-parquet ozone-parquet hive
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-japan-model.json"
       exit 1
     fi

     ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/customer/customer-spain-model.json 10000 4 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} hdfs-parquet ozone-parquet hive
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-spain-model.json"
       exit 1
     fi

    ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/customer/customer-turkey-model.json 10000 12 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} hdfs-parquet ozone-parquet hive
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-turkey-model.json"
       exit 1
     fi

    ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/customer/customer-usa-model.json 10000 21 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} hdfs-parquet ozone-parquet hive
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-usa-model.json"
       exit 1
     fi

     echo "Finished to Generate Customer data to HDFS in Parquet & Hive & Ozone in Parquet"
     exit 0
     ;;

 (gen_customer_hdfs)
     echo "Starting to Generate Customer data to HDFS in Parquet"

     ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/customer/customer-china-model.json 10000 12 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} hdfs-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-china-model.json"
       exit 1
     fi

     ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/customer/customer-france-model.json 10000 9 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} hdfs-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-france-model.json"
       exit 1
     fi

     ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/customer/customer-germany-model.json 10000 9 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} hdfs-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-germany-model.json"
       exit 1
     fi

     ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/customer/customer-india-model.json 10000 9 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} hdfs-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-india-model.json"
       exit 1
     fi

     ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/customer/customer-italy-model.json 10000 3 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} hdfs-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-italy-model.json"
       exit 1
     fi

     ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/customer/customer-japan-model.json 10000 11 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} hdfs-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-japan-model.json"
       exit 1
     fi

     ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/customer/customer-spain-model.json 10000 4 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} hdfs-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-spain-model.json"
       exit 1
     fi

    ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/customer/customer-turkey-model.json 10000 12 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} hdfs-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-turkey-model.json"
       exit 1
     fi

    ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/customer/customer-usa-model.json 10000 21 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} hdfs-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-usa-model.json"
       exit 1
     fi

     echo "Finished to Generate Customer data to HDFS in Parquet"
     exit 0
     ;;

 (gen_customer_ozone)
     echo "Starting to Generate Customer data to OZONE in Parquet"

     ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/customer/customer-china-model.json 10000 12 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} ozone-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-china-model.json"
       exit 1
     fi

     ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/customer/customer-france-model.json 10000 9 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} ozone-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-france-model.json"
       exit 1
     fi

     ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/customer/customer-germany-model.json 10000 9 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} ozone-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-germany-model.json"
       exit 1
     fi

     ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/customer/customer-india-model.json 10000 9 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} ozone-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-india-model.json"
       exit 1
     fi

     ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/customer/customer-italy-model.json 10000 3 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} ozone-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-italy-model.json"
       exit 1
     fi

     ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/customer/customer-japan-model.json 10000 11 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} ozone-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-japan-model.json"
       exit 1
     fi

     ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/customer/customer-spain-model.json 10000 4 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} ozone-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-spain-model.json"
       exit 1
     fi

    ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/customer/customer-turkey-model.json 10000 12 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} ozone-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-turkey-model.json"
       exit 1
     fi

    ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/customer/customer-usa-model.json 10000 21 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} ozone-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-usa-model.json"
       exit 1
     fi

     echo "Finished to Generate Customer data to OZONE in Parquet"
     exit 0
     ;;

  (gen_transaction_hbase)
    echo "Starting to Generate Transaction Data into HBase"
    ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/finance/transaction-model.json 10000 100 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} hbase
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for transaction-model.json"
       exit 1
     fi
     echo "Finished to Generate Transaction Data into HBase"
     exit 0
    ;;

  (gen_sensor_hive)
    echo "Starting to Generate sensor Data into Hive"
    ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/industry/plant-model.json 100 1 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} hive
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for plant-model.json"
       exit 1
     fi

    ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/industry/sensor-model.json 10000 10 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} hive
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for sensor-model.json"
       exit 1
     fi

    ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/industry/sensor-data-model.json 10000 100 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} hive
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for sensor-data-model.json"
       exit 1
     fi
    echo "Finished to Generate sensor Data into Hive"
    exit 0
    ;;

  (gen_public_service_kudu)
    echo "Starting to Generate public service Data into Kudu"
    ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/public_service/intervention-team-model.json 50000 20 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} kudu
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for intervention-team-model.json"
       exit 1
     fi
    echo "Finished to Generate public service Data into Kudu"
    exit 0
    ;;

  (gen_public_service_kafka)
    echo "Starting to Generate public service Data into Kafka"
    ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/public_service/incident-model.json 1000 1000 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} kafka
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for incident-model.json"
       exit 1
     fi
    echo "Finished to Generate public service Data into Kafka"
    ;;

  (gen_weather_solr)
    echo "Starting to Generate Weather Data into SolR"
    ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/public_service/weather-model.json 50000 20 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} solr
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for weather-model.json"
       exit 1
     fi
    echo "Finished to Generate Weather Data into SolR"
    ;;

  (gen_weather_kafka)
   echo "Starting to Generate Weather Measures Data into Kafka"
    ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/public_service/weather-sensor-model.json 50000 20 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} kafka
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for weather-sensor-model.json"
       exit 1
     fi
    echo "Finished to Generate Weather Measures Data into Kafka"
    exit 0
    ;;

  (gen_local_data)
    echo "Starting to Generate Local data for test purposes"
    ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/customer/customer-france-model.json 10 1 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} json
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-france-model.json"
       exit 1
     fi
    ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/finance/transaction-model.json 10 1 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} csv
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for transaction-model.json"
       exit 1
     fi
    ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/industry/plant-model.json 10 1 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} avro
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for plant-model.json"
       exit 1
     fi
    ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/industry/sensor-model.json 10 1 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for sensor-model.json"
       exit 1
     fi
    ${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/industry/sensor-data-model.json 10 1 3600 ${TLS_ENABLED} ${ADMIN_USER} ${ADMIN_PASSWORD} orc
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for sensor-data-model.json"
       exit 1
     fi

    echo "Finished to Generate Local data for test purposes"
    exit 0
    ;;
  (*)
    echo "Don't understand [$CMD]"
    exit 1
    ;;
esac
