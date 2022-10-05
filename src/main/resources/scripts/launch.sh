#!/usr/bin/env bash

export DIR="/home/datagen/deploy/"

echo "*** Starting to launch program ***"

    cd $DIR

echo "Launching jar via java command"

    export JAVA_HOME=/usr/lib/jvm/java-11/
    ${JAVA_HOME}/bin/java -Dnashorn.args=--no-deprecation-warning --add-opens java.base/jdk.internal.ref=ALL-UNNAMED -Dspring.profiles.active=cdp -Dserver.port=4242 -Xmx16G -jar datagen.jar --spring.config.location=file:${DIR}/application-test.properties $@

    sleep 5

echo "*** Finished program ***"