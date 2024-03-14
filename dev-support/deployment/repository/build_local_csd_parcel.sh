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
export DATAGEN_VERSION="0.4.15"
export CDP_VERSION="7.1.9.4"

export DISTRIBUTIONS_TO_BUILD="el7 el8 el9 sles15"

export BUILD_DATAGEN_JAR="true"
export BUILD_CSD="true"
export BUILD_PARCEL="true"
export BUILD_STANDALONE="true"

# DEBUG
export DEBUG=false
export LOG_DIR="/tmp/datagen-build-csd-parcel-logs/"$(date +%m-%d-%Y-%H-%M-%S)

function usage()
{
    echo "This script aims at building locally CSD & Parcels of Datagen"
    echo ""
    echo "Usage: "
    echo ""
    echo "./build_local_csd_parcel.sh"
    echo "  -h --help"
    echo ""
    echo "  --cdp-version=$CDP_VERSION : Version of CDP on which Datagen is deployed (Default) $CDP_VERSION "
    echo "  --datagen-version=$DATAGEN_VERSION : Version of Datagen that will be set for this deployment  (Default) $DATAGEN_VERSION"
    echo "  --distributions-to-build=$DISTRIBUTIONS_TO_BUILD : Space separated list of distributions to build (Default) $DISTRIBUTIONS_TO_BUILD"
    echo ""
    echo "  --build-datagen-jar=$BUILD_DATAGEN_JAR : To build the Datagen jar file or not (Default) $BUILD_DATAGEN_JAR "
    echo "  --build-csd=$BUILD_CSD : To build the CSD or not (Default) $BUILD_CSD "
    echo "  --build-parcel=$BUILD_PARCEL : To build the parcels or not (Default) $BUILD_PARCEL "
    echo "  --build-standalone=$BUILD_STANDALONE : To build the standalone files or not (Default) $BUILD_STANDALONE "
    echo ""
    echo "  --temp-dir=$TEMP_DIR : Temporary directory used for generation of files (Default) $TEMP_DIR "
    echo "  --csd-dir=$CSD_DIR : Directory where CSD will be generated  (Default) $CSD_DIR"
    echo "  --parcel-dir=$PARCEL_DIR : Directory where parcels will be generated  (Default) $PARCEL_DIR"
    echo "  --standalone-dir=$STANDALONE_DIR : Directory where standalone files will be generated  (Default) $STANDALONE_DIR"
    echo ""
    echo "  --debug=$DEBUG : To set DEBUG log-level (Default) $DEBUG "
    echo "  --log-dir=$LOG_DIR : Log directory (Default) $LOG_DIR "
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
        --cdp-version)
            CDP_VERSION=$VALUE
            ;;
        --datagen-version)
            DATAGEN_VERSION=$VALUE
            ;;
        --distributions-to-build)
            DISTRIBUTIONS_TO_BUILD=$VALUE
            ;;
        --build-datagen-jar)
            BUILD_DATAGEN_JAR=$VALUE
            ;;
        --build-csd)
            BUILD_CSD=$VALUE
            ;;
        --build-parcel)
            BUILD_PARCEL=$VALUE
            ;;
        --temp-dir)
            TEMP_DIR=$VALUE
            ;;
        --csd-dir)
            CSD_DIR=$VALUE
            ;;
        --parcel-dir)
            PARCEL_DIR=$VALUE
            ;;
        --standalone-dir)
            STANDALONE_DIR=$VALUE
            ;;
        --debug)
            DEBUG=$VALUE
            ;;
        --log-dir)
            LOG_DIR=$VALUE
            ;;
        *)
            ;;
    esac
    shift
done

# Output Directories
if [ -z ${TEMP_DIR} ]
then
  export TEMP_DIR="/tmp/datagen_parcel_files-${DATAGEN_VERSION}-${CDP_VERSION}"
fi
if [ -z ${PARCEL_DIR} ]
then
  export PARCEL_DIR="/tmp/datagen_parcel-${DATAGEN_VERSION}-${CDP_VERSION}"
fi
if [ -z ${CSD_DIR} ]
then
  export CSD_DIR="/tmp/datagen_csd-${DATAGEN_VERSION}-${CDP_VERSION}"
fi
if [ -z ${STANDALONE_DIR} ]
then
  export STANDALONE_DIR="/tmp/datagen_standalone-${DATAGEN_VERSION}-${CDP_VERSION}"
fi

# INTERNAL: Do not touch these
export DEPLOY_DIR=$(pwd)
export DATAGEN_FULL_VERSION="${DATAGEN_VERSION}.${CDP_VERSION}"
export DATAGEN_FULL_NAME="DATAGEN-${DATAGEN_VERSION}.${CDP_VERSION}"

echo " Start building Releases files for Datagen $DATAGEN_VERSION for CDP version $CDP_VERSION"
export DISTRIBUTIONS_LIST=$( echo ${DISTRIBUTIONS_TO_BUILD} | uniq )
export DISTRIBUTIONS=( ${DISTRIBUTIONS_LIST} )

# Print Env variables
if [ "${DEBUG}" = "true" ]
then
    echo ""
    echo "****************************** ENV VARIABLES ******************************"
    env | sort
    echo "***************************************************************************"
    echo ""
fi

# Clean potential old directories
rm -rf ${CSD_DIR}
rm -rf ${PARCEL_DIR}
rm -rf ${TEMP_DIR}

################# Build Datagen Jar #################
if [ "${BUILD_DATAGEN_JAR}" = "true" ]
then
  cd ../../../

  mvn clean package

  cd $DEPLOY_DIR
fi

################# Standalone Files #################
if [ "${BUILD_STANDALONE}" = "true" ]
then
    mkdir -p ${STANDALONE_DIR}
    mkdir -p  ${STANDALONE_DIR}/models/
    mkdir -p  ${STANDALONE_DIR}/dictionaries/

    cp -Rp ../../../src/main/resources/logback-spring.xml "${STANDALONE_DIR}/"
    cp -Rp ../../../src/main/resources/application.properties "${STANDALONE_DIR}/"
    cp -Rp ../../../target/datagen*.jar "${STANDALONE_DIR}/datagen-${DATAGEN_VERSION}.jar"
    cp -Rp ../../../src/main/resources/dictionaries/* "${STANDALONE_DIR}/dictionaries/"
    cp -Rp ../../../src/main/resources/models/* "${STANDALONE_DIR}/models/"

    # For BSD-like, tar includes some files which should not be present to be GNU-compliant
    export COPYFILE_DISABLE=true
    tar -czv --no-xattrs --exclude '._*' -f "${STANDALONE_DIR}/datagen-standalone-files.tar.gz" "${STANDALONE_DIR}/"

fi

################# CSD #################
if [ "${BUILD_CSD}" = "true" ]
then

  cd ../../csd/

  # Create CSD Temp directory with all CSD files
  mkdir -p ${CSD_DIR}
  mkdir -p  ${CSD_DIR}/descriptor/
  mkdir -p  ${CSD_DIR}/scripts/
  mkdir -p  ${CSD_DIR}/aux/templates/

  cp -R scripts/* ${CSD_DIR}/scripts/
  cp -R descriptor/* ${CSD_DIR}/descriptor/
  cp -R aux/templates/* ${CSD_DIR}/aux/templates

  # Create CSD
  cd ${CSD_DIR}
  jar -cvf "DATAGEN-${DATAGEN_VERSION}.${CDP_VERSION}.jar" *

  cd $DEPLOY_DIR

fi

################# PARCELS #################
if [ "${BUILD_PARCEL}" = "true" ]
then

  cd ../../parcel

  mkdir -p "${TEMP_DIR}"
  mkdir -p "${PARCEL_DIR}"

  export PARCEL_VERSION="${DATAGEN_VERSION}.${CDP_VERSION}"
  export PARCEL_NAME="DATAGEN-${PARCEL_VERSION}"
  LOCAL_PARCEL_DIR="${TEMP_DIR}/${PARCEL_NAME}"
  echo "Local Parcel Directory is: $LOCAL_PARCEL_DIR"

  rm -rf "${LOCAL_PARCEL_DIR}"
  rm -rf "${TEMP_DIR}/${PARCEL_NAME}.parcel"
  mkdir -p "${LOCAL_PARCEL_DIR}"
  mkdir -p "${LOCAL_PARCEL_DIR}/meta/"
  mkdir -p "${LOCAL_PARCEL_DIR}/models/"
  mkdir -p "${LOCAL_PARCEL_DIR}/dictionaries/"

  # Place all needed files in a temp_dir with right structure
  cp -p meta/parcel.json "${LOCAL_PARCEL_DIR}/meta/"
  cp -p meta/release-notes.txt "${LOCAL_PARCEL_DIR}/meta/"
  cp -p meta/datagen_env.sh "${LOCAL_PARCEL_DIR}/meta/"
  cp -Rp ../../src/main/resources/dictionaries/* "${LOCAL_PARCEL_DIR}/dictionaries/"
  cp -Rp ../../src/main/resources/models/* "${LOCAL_PARCEL_DIR}/models/"
  cp -Rp ../../src/main/resources/logback-spring.xml "${LOCAL_PARCEL_DIR}/"
  cp -Rp ../../src/main/resources/application.properties "${LOCAL_PARCEL_DIR}/"
  cp -Rp ../../target/datagen*.jar "${LOCAL_PARCEL_DIR}/${PARCEL_NAME}.jar"

  envsubst < "${LOCAL_PARCEL_DIR}/meta/parcel.json" > "${LOCAL_PARCEL_DIR}/meta/parcel.json.tmp" \
    && mv "${LOCAL_PARCEL_DIR}/meta/parcel.json.tmp" "${LOCAL_PARCEL_DIR}/meta/parcel.json"

  envsubst < "${LOCAL_PARCEL_DIR}/meta/release-notes.txt" > "${LOCAL_PARCEL_DIR}/meta/release-notes.txt.tmp" \
    && mv "${LOCAL_PARCEL_DIR}/meta/release-notes.txt.tmp" "${LOCAL_PARCEL_DIR}/meta/release-notes.txt"

  sed -i '' "s/DATAGEN_FULL_VERSION/${DATAGEN_FULL_VERSION}/g" "${LOCAL_PARCEL_DIR}/meta/parcel.json"
  sed -i '' "s/DATAGEN_FULL_NAME/${DATAGEN_FULL_NAME}/g" "${LOCAL_PARCEL_DIR}/meta/parcel.json"
  sed -i '' "s/DATAGEN_FULL_NAME/${DATAGEN_FULL_NAME}/g" "${LOCAL_PARCEL_DIR}/meta/datagen_env.sh"

  # Tar all this tmp dir in one with owner/group being root
  cd ${TEMP_DIR}
  rm -rf ${PARCEL_DIR}/*.parcel
  # For BSD-like, tar includes some files which should not be present to be GNU-compliant
  export COPYFILE_DISABLE=true
  tar -czv --no-xattrs --exclude '._*' -f ${PARCEL_DIR}/${PARCEL_NAME}-XXX.parcel ${PARCEL_NAME}/

  for i in "${DISTRIBUTIONS[@]}"
  do
    cp ${PARCEL_DIR}/${PARCEL_NAME}-XXX.parcel ${PARCEL_DIR}/${PARCEL_NAME}-${i}.parcel
  done
  rm -rf ${PARCEL_DIR}/${PARCEL_NAME}-XXX.parcel

  # Create manifest.json for parcel repo
  rm -rf /tmp/cm_ext/
  mkdir -p /tmp/cm_ext/
  git clone https://github.com/cloudera/cm_ext /tmp/cm_ext/
  cd /tmp/cm_ext/make_manifest/
  python make_manifest.py ${PARCEL_DIR}/

  cat ${PARCEL_DIR}/manifest.json |  jq -r '.parcels[0].hash' > ${PARCEL_DIR}/${PARCEL_NAME}-XXX.parcel.sha1
  for i in "${DISTRIBUTIONS[@]}"
  do
    cp ${PARCEL_DIR}/${PARCEL_NAME}-XXX.parcel.sha1 ${PARCEL_DIR}/${PARCEL_NAME}-${i}.parcel.sha1
  done
  rm -rf ${PARCEL_DIR}/${PARCEL_NAME}-XXX.parcel.sha1

  cd $DEPLOY_DIR

fi

echo "CSD is at ${CSD_DIR}"
echo "Parcel is at ${PARCEL_DIR}"
echo "Standalone files are at ${STANDALONE_DIR}"

echo " Finish building Releases files for Datagen $DATAGEN_VERSION for CDP version $CDP_VERSION"