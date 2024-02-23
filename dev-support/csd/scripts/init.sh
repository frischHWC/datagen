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
#!/bin/bash

echo "STARTING INIT OF DATAGEN"

echo "Creating required local files"

mkdir -p /home/${DATAGEN_USER}/
mkdir -p /home/${DATAGEN_USER}/jaas/
mkdir -p ${DATA_MODEL_RECEIVED}
mkdir -p ${DATA_MODEL_GENERATED}
touch ${SCHEDULER_FILE_PATH}
chown -R ${DATAGEN_USER}:${DATAGEN_USER} /home/${DATAGEN_USER}/

echo "Finished to create required local files"

echo "RANGER_SERVICE: ${RANGER_SERVICE_NAME}"
echo "RANGER_URL: ${RANGER_URL}"
echo "RANGER_ADMIN_USER: ${RANGER_ADMIN_USER}"


if [ "${RANGER_SERVICE_NAME}" != "" ] && [ "${RANGER_SERVICE_NAME}" != "none" ] && [ "${RANGER_URL}" != "" ] && [ "${RANGER_ADMIN_USER}" != "" ] && [ "${RANGER_ADMIN_PASSWORD}" != "" ]
then
  echo " Starting to push Ranger policies as Ranger is selected as a dependency"

    echo ""
    echo "Pushing policy to HBase"
    echo ""
    curl -k -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d "@scripts/policies/hbase.json" -u ${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}  ${RANGER_URL}/service/public/v2/api/policy
    echo ""
    echo ""
    echo "Pushing policy to HDFS"
    echo ""
    curl -k -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d "@scripts/policies/hdfs.json" -u ${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}  ${RANGER_URL}/service/public/v2/api/policy
    echo ""
    echo ""
    echo "Pushing policy to Hive"
    echo ""
    curl -k -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d "@scripts/policies/hive.json" -u ${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}  ${RANGER_URL}/service/public/v2/api/policy
    echo ""
    echo ""
    echo "Pushing policy to Hive URL "
    echo ""
    curl -k -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d "@scripts/policies/hive-url.json" -u ${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}  ${RANGER_URL}/service/public/v2/api/policy
    echo ""
    echo ""
    echo "Pushing policy to Hive Storage"
    echo ""
    curl -k -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d "@scripts/policies/hive-storage.json" -u ${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}  ${RANGER_URL}/service/public/v2/api/policy
    echo ""
    echo ""
    echo "Pushing policy to Kafka"
    echo ""
    curl -k -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d "@scripts/policies/kafka.json" -u ${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}  ${RANGER_URL}/service/public/v2/api/policy
    echo ""
    echo ""
    echo "Pushing policy to Kafka for cluster"
    echo ""
    curl -k -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d "@scripts/policies/kafka-cluster.json" -u ${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}  ${RANGER_URL}/service/public/v2/api/policy
    echo ""
    echo ""
    echo "Pushing policy to Kafka for transaction Id"
    echo ""
    curl -k -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d "@scripts/policies/kafka-transactionalid.json" -u ${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}  ${RANGER_URL}/service/public/v2/api/policy
    echo ""
    echo ""
    echo "Pushing policy to Kafka Stream"
    echo ""
    curl -k -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d "@scripts/policies/kafka-stream.json" -u ${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}  ${RANGER_URL}/service/public/v2/api/policy
    echo ""
    echo ""
    echo "Pushing policy to Kafka Stream for cluster"
    echo ""
    curl -k -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d "@scripts/policies/kafka-cluster-stream.json" -u ${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}  ${RANGER_URL}/service/public/v2/api/policy
    echo ""
    echo ""
    echo "Pushing policy to Kudu"
    echo ""
    curl -k -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d "@scripts/policies/kudu.json" -u ${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}  ${RANGER_URL}/service/public/v2/api/policy
    echo ""
    echo ""
    echo "Pushing policy to Ozone"
    echo ""
    curl -k -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d "@scripts/policies/ozone.json" -u ${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}  ${RANGER_URL}/service/public/v2/api/policy
    echo ""
    echo ""
    echo "Pushing policy to Schema Registry"
    echo ""
    curl -k -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d "@scripts/policies/schemaregistry.json" -u ${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}  ${RANGER_URL}/service/public/v2/api/policy
    echo ""
    echo ""
    echo "Pushing policy to Schema Registry Service"
    echo ""
    curl -k -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d "@scripts/policies/schemaregistry_service.json" -u ${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}  ${RANGER_URL}/service/public/v2/api/policy
    echo ""
    echo ""
    echo "Pushing policy to Schema Registry Serde"
    echo ""
    curl -k -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d "@scripts/policies/schemaregistry_serde.json" -u ${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}  ${RANGER_URL}/service/public/v2/api/policy
    echo ""
    echo ""
    echo "Pushing policy to SolR"
    echo ""
    curl -k -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d "@scripts/policies/solr.json" -u ${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}  ${RANGER_URL}/service/public/v2/api/policy
    echo ""
    echo ""
    echo "Pushing policy to SolR Admin"
    echo ""
    curl -k -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d "@scripts/policies/solr-admin.json" -u ${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}  ${RANGER_URL}/service/public/v2/api/policy
    echo ""
    echo ""
    echo "Pushing policy to SolR Config"
    echo ""
    curl -k -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d "@scripts/policies/solr-config.json" -u ${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}  ${RANGER_URL}/service/public/v2/api/policy
    echo ""
    echo ""
    echo "Pushing policy to SolR Schema"
    echo ""
    curl -k -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d "@scripts/policies/solr-schema.json" -u ${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}  ${RANGER_URL}/service/public/v2/api/policy
fi


echo "FINISHED INIT OF DATAGEN"

exit 0
