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
export DEST_DIR="/home/datagen/deploy"
cd ${DEST_DIR}

echo "Launching jar via java command"

    export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-17.0.11.0.9-2.el8.x86_64

    ${JAVA_HOME}/bin/java \
    -Dnashorn.args=--no-deprecation-warning --add-opens java.base/jdk.internal.ref=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED \
    -Xmx2G -Xms1G \
    -Dserver.port=4242 \
    -Dlogging.config=file:logback-spring.xml \
    -Dspring.profiles.active=standalone \
    -jar datagen.jar --spring.config.location=file:application-standalone.properties

    sleep 5

echo "*** Finished program ***"