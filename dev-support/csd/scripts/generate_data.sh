#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

CMD=$1
SERVER_PORT=$2
TLS_ENABLED=$3
DATAGEN_USER=$4
DATAGEN_PASSWORD=$5
USE_EXTRA_HEADER=$6

set -x
. ${COMMON_SCRIPT}

# Checking that jq command is present to process
if ! command -v jq &> /dev/null
then
    echo "'jq' command could not be found. Please install it on this node to generate data. (For RHEL-like system: yum install jq)"
    exit 1
fi

DATAGEN_URL="http://localhost:${SERVER_PORT}/api/v1"
if [ "${TLS_ENABLED}" = "true" ]
then
  DATAGEN_URL="https://localhost:${SERVER_PORT}/api/v1"
fi 

# Generic Function to generate Data
generate_data() {
  MODEL_FILE=$1
  shift
  ROWS=$1
  shift
  BATCHES=$1
  shift
  THREADS=$1
  shift
  TIMEOUT=$1
  shift
  CONNECTORS=$@

  echo "Launching command for model: ${MODEL_FILE} to server ${DATAGEN_URL}"

  UNIQUE_CONNECTORS=$( echo $CONNECTORS | uniq )
  CONNECTOR_ARRAY=( $UNIQUE_CONNECTORS )
  number_of_connectors="${#CONNECTOR_ARRAY[@]}"

  if [ $number_of_connectors == 1 ]
  then
    URL_TO_CALL="${DATAGEN_URL}/datagen/${CONNECTOR_ARRAY[0]}?batches=${BATCHES}&rows=${ROWS}&threads=${THREADS}"
  else
    CONNECTOR_STRING=""
    for i in ${CONNECTOR_ARRAY[@]}
    do
      CONNECTOR_STRING="${CONNECTOR_STRING}connectors=${i}&"
    done
    URL_TO_CALL="${DATAGEN_URL}/datagen/multipleconnectors?${CONNECTOR_STRING}batches=${BATCHES}&rows=${ROWS}&threads=${THREADS}"
  fi

  echo "Will call URL: ${URL_TO_CALL}"

  EXTRA_HEADER=""
  if [ ${USE_EXTRA_HEADER} == "true" ]
  then
    EXTRA_HEADER='-H "Content-Type: multipart/form-data"'
  fi

  COMMAND_RESPONSE=$(curl -s -k -X POST -H "Accept: */*" ${EXTRA_HEADER} -F "model_file=@${MODEL_FILE}" -u ${DATAGEN_USER}:${DATAGEN_PASSWORD} "$URL_TO_CALL")
  echo "Received Command Response: ${COMMAND_RESPONSE}"
  COMMAND_ID=$(echo $COMMAND_RESPONSE | jq -r '.commandUuid')

  echo "Received Command UUID: ${COMMAND_ID}"

  # Checking command result
  start_time=$(date +%s)
  echo "Checking status of the command"
  while true
  do
      STATUS=$(curl -s -k -X POST -H "Accept: application/json" -u ${DATAGEN_USER}:${DATAGEN_PASSWORD} \
          "${DATAGEN_URL}/command/getCommandStatus?commandUuid=${COMMAND_ID}" | jq -r ".status")
      printf '.'
      if [ "${STATUS}" == "FINISHED" ]
      then
          echo ""
          echo "SUCCESS: Command for model ${MODEL_FILE}"
          break
      elif [ "${STATUS}" == "FAILED" ]
      then
          echo ""
          echo "FAILURE: Command for model ${MODEL_FILE}"
          exit 1
      else
          sleep 5
      fi
      current_time=$(date +%s)
      elapsed_time=$((current_time - start_time))

      if [ "$elapsed_time" -ge "$TIMEOUT" ]; then
          echo " Command Timed Out "
          exit 1
      fi
  done


}


case $CMD in
 (gen_customer_hdfs_ozone_hive)
     echo "Starting to Generate Customer data to HDFS in Parquet & Hive & Ozone in Parquet"

     generate_data /opt/cloudera/parcels/DATAGEN/models/customer/customer-china-model.json 120000 1 10 600  hdfs-parquet hive ozone-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-china-model.json"
       exit 1
     fi

     generate_data /opt/cloudera/parcels/DATAGEN/models/customer/customer-france-model.json 90000 1 10 600  hdfs-parquet hive ozone-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-france-model.json"
       exit 1
     fi

     generate_data /opt/cloudera/parcels/DATAGEN/models/customer/customer-germany-model.json 90000 1 10 600  hdfs-parquet hive ozone-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-germany-model.json"
       exit 1
     fi

     generate_data /opt/cloudera/parcels/DATAGEN/models/customer/customer-india-model.json 190000 1 10 600  hdfs-parquet hive ozone-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-india-model.json"
       exit 1
     fi

     generate_data /opt/cloudera/parcels/DATAGEN/models/customer/customer-italy-model.json 30000 1 10 600  hdfs-parquet hive ozone-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-italy-model.json"
       exit 1
     fi

     generate_data /opt/cloudera/parcels/DATAGEN/models/customer/customer-japan-model.json 110000 1 10 600  hdfs-parquet hive ozone-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-japan-model.json"
       exit 1
     fi

     generate_data /opt/cloudera/parcels/DATAGEN/models/customer/customer-spain-model.json 40000 1 10 600  hdfs-parquet hive ozone-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-spain-model.json"
       exit 1
     fi

    generate_data /opt/cloudera/parcels/DATAGEN/models/customer/customer-turkey-model.json 120000 1 10 600  hdfs-parquet hive ozone-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-turkey-model.json"
       exit 1
     fi

    generate_data /opt/cloudera/parcels/DATAGEN/models/customer/customer-usa-model.json 210000 1 10 600  hdfs-parquet hive ozone-parquet
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

     generate_data /opt/cloudera/parcels/DATAGEN/models/customer/customer-china-model.json 10000 12 10 600  hdfs-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-china-model.json"
       exit 1
     fi

     generate_data /opt/cloudera/parcels/DATAGEN/models/customer/customer-france-model.json 10000 9 10 600  hdfs-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-france-model.json"
       exit 1
     fi

     generate_data /opt/cloudera/parcels/DATAGEN/models/customer/customer-germany-model.json 10000 9 10 600  hdfs-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-germany-model.json"
       exit 1
     fi

     generate_data /opt/cloudera/parcels/DATAGEN/models/customer/customer-india-model.json 10000 9 10 600  hdfs-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-india-model.json"
       exit 1
     fi

     generate_data /opt/cloudera/parcels/DATAGEN/models/customer/customer-italy-model.json 10000 3 10 600  hdfs-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-italy-model.json"
       exit 1
     fi

     generate_data /opt/cloudera/parcels/DATAGEN/models/customer/customer-japan-model.json 10000 11 10 600  hdfs-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-japan-model.json"
       exit 1
     fi

     generate_data /opt/cloudera/parcels/DATAGEN/models/customer/customer-spain-model.json 10000 4 10 600  hdfs-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-spain-model.json"
       exit 1
     fi

    generate_data /opt/cloudera/parcels/DATAGEN/models/customer/customer-turkey-model.json 10000 12 10 600  hdfs-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-turkey-model.json"
       exit 1
     fi

    generate_data /opt/cloudera/parcels/DATAGEN/models/customer/customer-usa-model.json 10000 21 10 600  hdfs-parquet
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

     generate_data /opt/cloudera/parcels/DATAGEN/models/customer/customer-china-model.json 10000 12 10 600  ozone-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-china-model.json"
       exit 1
     fi

     generate_data /opt/cloudera/parcels/DATAGEN/models/customer/customer-france-model.json 10000 9 10 600  ozone-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-france-model.json"
       exit 1
     fi

     generate_data /opt/cloudera/parcels/DATAGEN/models/customer/customer-germany-model.json 10000 9 10 600  ozone-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-germany-model.json"
       exit 1
     fi

     generate_data /opt/cloudera/parcels/DATAGEN/models/customer/customer-india-model.json 10000 9 10 600  ozone-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-india-model.json"
       exit 1
     fi

     generate_data /opt/cloudera/parcels/DATAGEN/models/customer/customer-italy-model.json 10000 3 10 600  ozone-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-italy-model.json"
       exit 1
     fi

     generate_data /opt/cloudera/parcels/DATAGEN/models/customer/customer-japan-model.json 10000 11 10 600  ozone-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-japan-model.json"
       exit 1
     fi

     generate_data /opt/cloudera/parcels/DATAGEN/models/customer/customer-spain-model.json 10000 4 10 600  ozone-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-spain-model.json"
       exit 1
     fi

    generate_data /opt/cloudera/parcels/DATAGEN/models/customer/customer-turkey-model.json 10000 12 10 600  ozone-parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-turkey-model.json"
       exit 1
     fi

    generate_data /opt/cloudera/parcels/DATAGEN/models/customer/customer-usa-model.json 10000 21 10 600  ozone-parquet
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
    generate_data /opt/cloudera/parcels/DATAGEN/models/finance/transaction-model.json 100000 10 10 600  hbase
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
    generate_data /opt/cloudera/parcels/DATAGEN/models/industry/plant-model.json 100 1 10 600  hive
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for plant-model.json"
       exit 1
     fi

    generate_data /opt/cloudera/parcels/DATAGEN/models/industry/sensor-model.json 10000 10 10 600  hive
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for sensor-model.json"
       exit 1
     fi

    generate_data /opt/cloudera/parcels/DATAGEN/models/industry/sensor-data-model.json 100000 10 10 600  hive
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for sensor-data-model.json"
       exit 1
     fi
    echo "Finished to Generate sensor Data into Hive"
    exit 0
    ;;

  (gen_sensor_hive_iceberg)
    echo "Starting to Generate sensor Data into Hive in Iceberg Format"
    generate_data /opt/cloudera/parcels/DATAGEN/models/industry/plant-model-iceberg.json 100 1 10 600  hive
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for plant-model-iceberg.json"
       exit 1
     fi

    generate_data /opt/cloudera/parcels/DATAGEN/models/industry/sensor-model-iceberg.json 10000 10 10 600  hive
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for sensor-model-iceberg.json"
       exit 1
     fi

    generate_data /opt/cloudera/parcels/DATAGEN/models/industry/sensor-data-model-iceberg.json 100000 10 10 600  hive
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for sensor-data-model-iceberg.json"
       exit 1
     fi
    echo "Finished to Generate sensor Data into Hive in Iceberg Format"
    exit 0
    ;;

  (gen_public_service_kudu)
    echo "Starting to Generate public service Data into Kudu"
    generate_data /opt/cloudera/parcels/DATAGEN/models/public_service/intervention-team-model.json 50000 20 10 600  kudu
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
    generate_data /opt/cloudera/parcels/DATAGEN/models/public_service/incident-model.json 1000 1000 10 600  kafka
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for incident-model.json"
       exit 1
     fi
    echo "Finished to Generate public service Data into Kafka"
    ;;

  (gen_weather_solr)
    echo "Starting to Generate Weather Data into SolR"
    generate_data /opt/cloudera/parcels/DATAGEN/models/public_service/weather-model.json 50000 20 10 600  solr
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for weather-model.json"
       exit 1
     fi
    echo "Finished to Generate Weather Data into SolR"
    ;;

  (gen_weather_kafka)
   echo "Starting to Generate Weather Measures Data into Kafka"
    generate_data /opt/cloudera/parcels/DATAGEN/models/public_service/weather-sensor-model.json 50000 20 10 600  kafka
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
    generate_data /opt/cloudera/parcels/DATAGEN/models/customer/customer-france-model.json 10 1 10 600  json
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for customer-france-model.json"
       exit 1
     fi
    generate_data /opt/cloudera/parcels/DATAGEN/models/finance/transaction-model.json 10 1 10 600  csv
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for transaction-model.json"
       exit 1
     fi
    generate_data /opt/cloudera/parcels/DATAGEN/models/industry/plant-model.json 10 1 10 600  avro
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for plant-model.json"
       exit 1
     fi
    generate_data /opt/cloudera/parcels/DATAGEN/models/industry/sensor-model.json 10 1 10 600  parquet
     ret=$?
     if [ $ret -ne 0 ]; then
       echo " Unable to generate data for sensor-model.json"
       exit 1
     fi
    generate_data /opt/cloudera/parcels/DATAGEN/models/industry/sensor-data-model.json 10 1 10 600  orc
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

