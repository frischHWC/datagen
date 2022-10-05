#!/usr/bin/env bash

# Set an Hostname with below export to launch it
# export HOST=
# export SSH_KEY="-i <SSH_KEY>"

export USER=root
export DEST_DIR="/home/datagen/deploy/"

echo "Create needed directory on platform and send required files there"

ssh ${SSH_KEY} ${USER}@${HOST} "mkdir -p ${DEST_DIR}/"
ssh ${SSH_KEY} ${USER}@${HOST} "mkdir -p ${DEST_DIR}/resources/"

scp ${SSH_KEY} src/main/resources/models/*.json ${USER}@${HOST}:${DEST_DIR}/
scp ${SSH_KEY} src/main/resources/dictionnaries/* ${USER}@${HOST}:${DEST_DIR}/
scp ${SSH_KEY} src/main/resources/*.properties ${USER}@${HOST}:${DEST_DIR}/
scp ${SSH_KEY} src/main/resources/scripts/launch.sh ${USER}@${HOST}:${DEST_DIR}/

ssh ${SSH_KEY} ${USER}@${HOST} "chmod +x ${DEST_DIR}/launch.sh"

cd target/
tar -cvzf datagen.tar.gz datagen-*.jar
cd ../
scp ${SSH_KEY} target/datagen.tar.gz ${USER}@${HOST}:${DEST_DIR}/datagen.tar.gz
ssh ${SSH_KEY} ${USER}@${HOST} "tar -xvzf ${DEST_DIR}/ ${DEST_DIR}/datagen.tar.gz"

echo "Finished to send required files"

echo "Launch script on platform to launch program properly"
ssh ${SSH_KEY} ${USER}@${HOST} 'bash -s' < src/main/resources/scripts/launch.sh $@
echo "Program finished"


