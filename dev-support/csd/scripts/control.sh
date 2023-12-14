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
CMD=$1

case $CMD in
  (start)
    echo "Starting DATAGEN"
    if [ -z ${KEYTAB_PATH} ]
    then
      export KEYTAB_PATH="${CONF_DIR}/${USER}.keytab"
    fi

    envsubst < "${CONF_DIR}/service.properties" > "${CONF_DIR}/service.properties.tmp"
    mv "${CONF_DIR}/service.properties.tmp" "${CONF_DIR}/service.properties"
    chmod 700 "${CONF_DIR}/service.properties"

    TRUSTSTORE_CONFIG=""
    if [ -n "${TRUSTSTORE_LOCATION}" ]
    then
      TRUSTSTORE_CONFIG="-Djavax.net.ssl.trustStore=${TRUSTSTORE_LOCATION} "
    fi

    if [ -n "${TRUSTSTORE_PASSWORD}" ]
    then
      TRUSTSTORE_CONFIG="${TRUSTSTORE_CONFIG} -Djavax.net.ssl.trustStorePassword=${TRUSTSTORE_PASSWORD} "
    fi

    JAVA_HOME_FOR_DATAGEN=""
    if [ -n "${JAVA_HOME_CUSTOM}" ]
    then
      JAVA_HOME_FOR_DATAGEN=${JAVA_HOME_CUSTOM}
    else
      JAVA_HOME_FOR_DATAGEN=${JAVA_HOME}
    fi

    exec ${JAVA_HOME_FOR_DATAGEN}/bin/java -Dnashorn.args=--no-deprecation-warning --add-opens java.base/jdk.internal.ref=ALL-UNNAMED ${TRUSTSTORE_CONFIG} -Dspring.profiles.active=cdp -Dserver.port=${SERVER_PORT} \
     -jar -Xmx${MAX_HEAP_SIZE}G ${DATAGEN_JAR_PATH} --spring.config.location=file:${CONF_DIR}/service.properties
    ;;

  (*)
    echo "Don't understand [$CMD]"
    ;;
esac