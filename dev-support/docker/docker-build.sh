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

PODMAN_OR_DOCKER=podman

cd ../../
mvn clean package -Pproduction

cp target/datagen*.jar dev-support/docker/datagen.jar
cp src/main/resources/application.properties dev-support/docker/application-standalone.properties
cp src/main/resources/logback-spring.xml dev-support/docker/
cp src/main/resources/scripts/launch.sh dev-support/docker/

cd dev-support/docker/

${PODMAN_OR_DOCKER} build -t dg:v0.1 .

rm -rf datagen.jar logback-spring.xml application.properties launch.sh

echo "Run it with: "
echo "${PODMAN_OR_DOCKER} run --memory=2g -p 4242:4242 dg:v0.1 "
