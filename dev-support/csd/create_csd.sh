#!/usr/bin/env bash
CDP_VERSION="7.1.7.1000"
RD_VERSION="0.1.4"

TEMP_CSD_DIR="/tmp/datagen_csd"

# Create CSD Temp directory with all CSD files
rm -rf ${TEMP_CSD_DIR}
mkdir -p ${TEMP_CSD_DIR}
mkdir -p  ${TEMP_CSD_DIR}/descriptor/
mkdir -p  ${TEMP_CSD_DIR}/scripts/

cp scripts/* ${TEMP_CSD_DIR}/scripts/
cp descriptor/service.sdl ${TEMP_CSD_DIR}/descriptor/

# Create CSD
cd ${TEMP_CSD_DIR}
jar -cvf "DATAGEN-${RD_VERSION}.${CDP_VERSION}.jar" *