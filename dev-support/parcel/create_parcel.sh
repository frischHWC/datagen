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
CDP_VERSION="7.1.7.1000"
RD_VERSION="0.1.4"
TEMP_DIR="/tmp/datagen_parcel_files"
TEMP_PARCEL_DIR="/tmp/datagen_parcel"
DISTRO_SUFFIX="el7"

mkdir -p "${TEMP_DIR}"
mkdir -p "${TEMP_PARCEL_DIR}"

export PARCEL_VERSION="${RD_VERSION}.${CDP_VERSION}"
export PARCEL_NAME="DATAGEN-${PARCEL_VERSION}"
PARCEL_DIR="${TEMP_DIR}/${PARCEL_NAME}"

rm -rf "${PARCEL_DIR}"
rm -rf "${TEMP_DIR}/${PARCEL_NAME}.parcel"
mkdir -p "${PARCEL_DIR}"
mkdir -p "${PARCEL_DIR}/meta/"
mkdir -p "${PARCEL_DIR}/models/"
mkdir -p "${PARCEL_DIR}/dictionaries/"

# Place all needed files in a temp_dir with right structure
cp meta/parcel.json "${PARCEL_DIR}/meta/"
cp meta/release-notes.txt "${PARCEL_DIR}/meta/"
cp meta/datagen_env.sh "${PARCEL_DIR}/meta/"
cp ../src/main/resources/dictionaries/* "${PARCEL_DIR}/dictionaries/"
cp ../src/main/resources/models/* "${PARCEL_DIR}/models/"
cp ../src/main/resources/logback.xml "${PARCEL_DIR}/"
cp ../src/main/resources/application.properties "${PARCEL_DIR}/"
cp ../../../../target/random-datagen*.jar "${PARCEL_DIR}/${PARCEL_NAME}.jar"

envsubst < "${PARCEL_DIR}/meta/parcel.json" > "${PARCEL_DIR}/meta/parcel.json.tmp" \
  && mv "${PARCEL_DIR}/meta/parcel.json.tmp" "${PARCEL_DIR}/meta/parcel.json"

envsubst < "${PARCEL_DIR}/meta/release-notes.txt" > "${PARCEL_DIR}/meta/release-notes.txt.tmp" \
  && mv "${PARCEL_DIR}/meta/release-notes.txt.tmp" "${PARCEL_DIR}/meta/release-notes.txt"

# Tar all this tmp dir in one with owner/group being root
cd ${TEMP_DIR}
rm -rf ${TEMP_PARCEL_DIR}/${PARCEL_NAME}-${DISTRO_SUFFIX}.parcel
tar -czvf ${TEMP_PARCEL_DIR}/${PARCEL_NAME}-${DISTRO_SUFFIX}.parcel ${PARCEL_NAME}/
