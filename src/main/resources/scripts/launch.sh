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
#!/usr/bin/env bash

# Base configuration
export JAR_PATH="datagen.jar"
export MIN_MEMORY="1G"
export MAX_MEMORY="2G"
export PORT="4242"

# Advanced Configuration
export PROFILE="standalone"
export CONFIG_FILE="application-standalone.properties"
export LOG_FILE="logback-spring.xml"
export LOG_DIR=""
export LOAD_DEFAULT_MODELS="true"

# For Background launch
export LAUNCH_WITH_NOHUP="false"


function usage()
{
    echo "This script launches Datagen"
    echo ""
    echo "Usage is the following : "
    echo ""
    echo "./launch.sh"
    echo "  -h --help"
    echo ""
    echo "  --jar-path=$JAR_PATH : defines where datagen.jar file is located"
    echo "  --min-mem=$MIN_MEMORY : Minimum memory allocated to datagen"
    echo "  --max-mem=$MAX_MEMORY : Maximum memory allocated to datagen "
    echo "  --port=$PORT : On which port to start the server "
    echo "  --profile=$PROFILE : On which profile to launch "
    echo "  --config-file=$CONFIG_FILE : Path to the configuration file (application.properties), use it if a new configuration file must be used with updated configurations"
    echo "  --log-file=$LOG_FILE : Path to the loggging configuration file (logback.xml), use it if a new logging configuration file must be used due to changes made"
    echo "  --log-dir=$LOG_DIR : WARNING: this setting is modifying the log file to change the place where log files are sent"
    echo "  --launch-with-nohup=$LAUNCH_WITH_NOHUP : To launch datagen in background"
    echo "  --load-default-models=$LOAD_DEFAULT_MODELS : WARNING: This setting modifies the properties file to load the default models"
    echo ""
}


while [ "$1" != "" ]; do
    PARAM=`echo $1 | awk -F= '{print $1}'`
    VALUE=`echo $1 | awk -F= '{print $2}'`
    case $PARAM in
        -h | --help)
            usage
            exit
            ;;
        --jar-path)
            JAR_PATH=$VALUE
            ;;
        --min-mem)
            MIN_MEMORY=$VALUE
            ;;
        --max-mem)
            MAX_MEMORY=$VALUE
            ;;
        --port)
            PORT=$VALUE
            ;;
        --profile)
            PROFILE=$VALUE
            ;;
        --config-file)
            CONFIG_FILE=$VALUE
            ;;
        --log-file)
            LOG_FILE=$VALUE
            ;;
        --log-dir)
            LOG_DIR=$VALUE
            ;;
        --launch-with-nohup)
            LAUNCH_WITH_NOHUP=$VALUE
            ;;
        --load-default-models)
          LOAD_DEFAULT_MODELS=$VALUE
          ;;
        *)
            ;;
    esac
    shift
done

export DATE_FOR_FILES=$(date +%d_%m_%Y_%H_%M_%S)
if [ -n "${LOG_DIR}" ]
then
  cp ${LOG_FILE} ${LOG_FILE}.${DATE_FOR_FILES}.bckup
  sed -i "s;/var/log/datagen/;${LOG_DIR};g" ${LOG_FILE}
fi

if [ "${LOAD_DEFAULT_MODELS}" = "false" ]
then
  cp ${CONFIG_FILE} ${CONFIG_FILE}.${DATE_FOR_FILES}.bckup
  sed -i "s;datagen.load.default.models=true;datagen.load.default.models=false;g" ${CONFIG_FILE}
fi

echo "Launching jar using following java command:"
echo "
    java \
    -Dnashorn.args=--no-deprecation-warning --add-opens java.base/jdk.internal.ref=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED \
    -Xmx${MAX_MEMORY} -Xms${MIN_MEMORY} \
    -Dserver.port=${PORT} \
    -Dlogging.config=file:${LOG_FILE} \
    -Dspring.profiles.active=${PROFILE} \
    -jar ${JAR_PATH} --spring.config.location=file:${CONFIG_FILE}
"

if [ "${LAUNCH_WITH_NOHUP}" = "true" ]
then

  nohup java \
            -Dnashorn.args=--no-deprecation-warning --add-opens java.base/jdk.internal.ref=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED \
            -Xmx${MAX_MEMORY} -Xms${MIN_MEMORY} \
            -Dserver.port=${PORT} \
            -Dlogging.config=file:${LOG_FILE} \
            -Dspring.profiles.active=${PROFILE} \
            -jar ${JAR_PATH} --spring.config.location=file:${CONFIG_FILE} >&/dev/null &

else

    java \
    -Dnashorn.args=--no-deprecation-warning --add-opens java.base/jdk.internal.ref=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED \
    -Xmx${MAX_MEMORY} -Xms${MIN_MEMORY} \
    -Dserver.port=${PORT} \
    -Dlogging.config=file:${LOG_FILE} \
    -Dspring.profiles.active=${PROFILE} \
    -jar ${JAR_PATH} --spring.config.location=file:${CONFIG_FILE}

fi


