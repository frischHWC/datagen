#!/bin/bash

echo "STARTING INIT OF DATAGEN"

echo "RANGER_SERVICE: ${RANGER_SERVICE_NAME}"
echo "RANGER_URL: ${RANGER_URL}"
echo "RANGER_ADMIN_USER: ${RANGER_ADMIN_USER}"

if [ "${RANGER_SERVICE_NAME}" != "" ] && [ "${RANGER_SERVICE_NAME}" != "none" ] && [ "${RANGER_URL}" != "" ] && [ "${RANGER_ADMIN_USER}" != "" ] && [ "${RANGER_ADMIN_PASSWORD}" != "" ]
then
  echo " Starting to push Ranger policies as Ranger is selected as a dependency"


    echo "Pushing policy to HBase"
    curl -k -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d "@scripts/policies/hbase.json" -u ${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}  ${RANGER_URL}/service/public/v2/api/policy

    echo "Pushing policy to HDFS"
    curl -k -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d "@scripts/policies/hdfs.json" -u ${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}  ${RANGER_URL}/service/public/v2/api/policy

    echo "Pushing policy to Hive"
    curl -k -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d "@scripts/policies/hive.json" -u ${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}  ${RANGER_URL}/service/public/v2/api/policy

    echo "Pushing policy to Kafka"
    curl -k -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d "@scripts/policies/kafka.json" -u ${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}  ${RANGER_URL}/service/public/v2/api/policy

    echo "Pushing policy to Kafka Stream"
    curl -k -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d "@scripts/policies/kafka-stream.json" -u ${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}  ${RANGER_URL}/service/public/v2/api/policy

    echo "Pushing policy to Kudu"
    curl -k -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d "@scripts/policies/kudu.json" -u ${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}  ${RANGER_URL}/service/public/v2/api/policy

    echo "Pushing policy to Ozone"
    curl -k -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d "@scripts/policies/ozone.json" -u ${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}  ${RANGER_URL}/service/public/v2/api/policy

    echo "Pushing policy to Schema Registry"
    curl -k -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d "@scripts/policies/schemaregistry.json" -u ${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}  ${RANGER_URL}/service/public/v2/api/policy

    echo "Pushing policy to Schema Registry Service"
    curl -k -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d "@scripts/policies/schemaregistry_service.json" -u ${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}  ${RANGER_URL}/service/public/v2/api/policy

    echo "Pushing policy to Schema Registry Serde"
    curl -k -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d "@scripts/policies/schemaregistry_serde.json" -u ${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}  ${RANGER_URL}/service/public/v2/api/policy

    echo "Pushing policy to SolR"
    curl -k -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d "@scripts/policies/solr.json" -u ${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}  ${RANGER_URL}/service/public/v2/api/policy

fi


echo "FINISHED INIT OF DATAGEN"

exit 0
