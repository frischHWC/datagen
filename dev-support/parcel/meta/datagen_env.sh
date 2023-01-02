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
DATAGEN_DIRNAME=${PARCEL_DIRNAME:-"DATAGEN_FULL_NAME"}
export DATAGEN_JAR_PATH=${PARCELS_ROOT}/${DATAGEN_DIRNAME}/DATAGEN_FULL_NAME.jar
export DATAGEN_MODELS_DIR=${PARCELS_ROOT}/${DATAGEN_DIRNAME}/models/
export DATAGEN_DICO_DIR=${PARCELS_ROOT}/${DATAGEN_DIRNAME}/dictionaries/
export DATAGEN_PROPERTIES_FILE=${PARCELS_ROOT}/${DATAGEN_DIRNAME}/application.properties
export DATAGEN_LOG_CONF_FILE=${PARCELS_ROOT}/${DATAGEN_DIRNAME}/logback-spring.xml