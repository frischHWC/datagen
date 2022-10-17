#!/bin/bash


export CDP_VERSION="7.1.8.0"
export DATAGEN_VERSION="0.2.8"
export DATAGEN_FULL_VERSION="${DATAGEN_VERSION}.${CDP_VERSION}"
export DATAGEN_FULL_NAME="DATAGEN-${DATAGEN_VERSION}.${CDP_VERSION}"
export DISTRO_SUFFIX="el7"
export MULTIPLE_DISTRIBUTION_BUILD="true"
export OTHER_DISTRIBUTIONS_TO_BUILD=("el8" "sles12")

export BUILD_CSD="true"
export BUILD_PARCEL="true"

export TEMP_DIR="/tmp/datagen_parcel_files"
export TEMP_PARCEL_DIR="/tmp/datagen_parcel"
export TEMP_CSD_DIR="/tmp/datagen_csd"

export DEPLOY_DIR=$(pwd)

rm -rf ${TEMP_CSD_DIR}
rm -rf ${TEMP_PARCEL_DIR}
rm -rf ${TEMP_DIR}


if [ "${BUILD_CSD}" = "true" ]
then

  cd ../csd/

  # Create CSD Temp directory with all CSD files
  mkdir -p ${TEMP_CSD_DIR}
  mkdir -p  ${TEMP_CSD_DIR}/descriptor/
  mkdir -p  ${TEMP_CSD_DIR}/scripts/
  mkdir -p  ${TEMP_CSD_DIR}/aux/templates/

  cp -R scripts/* ${TEMP_CSD_DIR}/scripts/
  cp -R descriptor/* ${TEMP_CSD_DIR}/descriptor/
  cp -R aux/templates/* ${TEMP_CSD_DIR}/aux/templates

  # Create CSD
  cd ${TEMP_CSD_DIR}
  jar -cvf "DATAGEN-${DATAGEN_VERSION}.${CDP_VERSION}.jar" *

  cd $DEPLOY_DIR

  cp ${TEMP_CSD_DIR}/DATAGEN-*.jar ~/

fi

if [ "${BUILD_PARCEL}" = "true" ]
then

  cd ../parcel

  mkdir -p "${TEMP_DIR}"
  mkdir -p "${TEMP_PARCEL_DIR}"

  export PARCEL_VERSION="${DATAGEN_VERSION}.${CDP_VERSION}"
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
  cp -R ../../src/main/resources/dictionaries/* "${PARCEL_DIR}/dictionaries/"
  cp -R ../../src/main/resources/models/* "${PARCEL_DIR}/models/"
  cp -R ../../src/main/resources/logback-spring.xml "${PARCEL_DIR}/"
  cp -R ../../src/main/resources/application.properties "${PARCEL_DIR}/"
  cp -R ../../target/datagen*.jar "${PARCEL_DIR}/${PARCEL_NAME}.jar"

  envsubst < "${PARCEL_DIR}/meta/parcel.json" > "${PARCEL_DIR}/meta/parcel.json.tmp" \
    && mv "${PARCEL_DIR}/meta/parcel.json.tmp" "${PARCEL_DIR}/meta/parcel.json"

  envsubst < "${PARCEL_DIR}/meta/release-notes.txt" > "${PARCEL_DIR}/meta/release-notes.txt.tmp" \
    && mv "${PARCEL_DIR}/meta/release-notes.txt.tmp" "${PARCEL_DIR}/meta/release-notes.txt"

  sed -i '' "s/DATAGEN_FULL_VERSION/${DATAGEN_FULL_VERSION}/g" "${PARCEL_DIR}/meta/parcel.json"
  sed -i '' "s/DATAGEN_FULL_NAME/${DATAGEN_FULL_NAME}/g" "${PARCEL_DIR}/meta/datagen_env.sh"

  # Tar all this tmp dir in one with owner/group being root
  cd ${TEMP_DIR}
  rm -rf ${TEMP_PARCEL_DIR}/*.parcel
  tar -czvf ${TEMP_PARCEL_DIR}/${PARCEL_NAME}-${DISTRO_SUFFIX}.parcel ${PARCEL_NAME}/

  if [ "${MULTIPLE_DISTRIBUTION_BUILD}" = "true" ]
  then
    for i in "${OTHER_DISTRIBUTIONS_TO_BUILD[@]}"
    do
      cp ${TEMP_PARCEL_DIR}/${PARCEL_NAME}-${DISTRO_SUFFIX}.parcel ${TEMP_PARCEL_DIR}/${PARCEL_NAME}-${i}.parcel
    done
  fi

  # Create manifest.json for parcel repo
  rm -rf /tmp/cm_ext/
  mkdir -p /tmp/cm_ext/
  git clone https://github.com/cloudera/cm_ext /tmp/cm_ext/
  cd /tmp/cm_ext/make_manifest/
  python make_manifest.py ${TEMP_PARCEL_DIR}/

  cat ${TEMP_PARCEL_DIR}/manifest.json |  jq -r '.parcels[0].hash' > ${TEMP_PARCEL_DIR}/${PARCEL_NAME}-${DISTRO_SUFFIX}.parcel.sha1
  if [ "${MULTIPLE_DISTRIBUTION_BUILD}" = "true" ]
  then
    for i in "${OTHER_DISTRIBUTIONS_TO_BUILD[@]}"
    do
      cp ${TEMP_PARCEL_DIR}/${PARCEL_NAME}-${DISTRO_SUFFIX}.parcel.sha1 ${TEMP_PARCEL_DIR}/${PARCEL_NAME}-${i}.parcel.sha1
    done
  fi

  cd $DEPLOY_DIR

  cp ${TEMP_PARCEL_DIR}/* ~/

fi

echo "CSD is at ${TEMP_CSD_DIR}"
echo "Parcel is at ${TEMP_PARCEL_DIR}"