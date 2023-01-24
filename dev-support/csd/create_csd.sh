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