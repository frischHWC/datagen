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

echo "******************************************"
echo "     Deployment of DATAGEN Started "
echo "******************************************"

# Required variables
export CLUSTER_NAME=""

# Host where to deploy DATAGEN
export EDGE_HOST=""

# Auth to use to connect to cluster machines (if auth is required, otherwise let it empty)
export SSH_KEY=""
export SSH_PASSWORD=""
export SSH_USER="root"

# Cloudera Manager related
export CM_HOST=""
export CM_PORT="7183"
export CM_PROTOCOL="https"
export CM_USER="admin"
export CM_PASSWORD="admin"

# Ranger related
export RANGER_PASSWORD="admin"

# Version of RD to create (to inject datagen-env.sh)
export CDP_VERSION="7.1.9.2"
export DATAGEN_VERSION="0.4.12"
export DISTRO_SUFFIX="el7"

# Steps to launch
export CREATE_DATAGEN=true
export INSTALL_DATAGEN=true
export LAUNCH_DATAGEN=true

# If using a streaming cluster for Kafka and Schema Registry (specify its name using this variable)
# If not, it will default to base cluster name
export CLUSTER_NAME_STREAMING=""

# DEBUG
export DEBUG=false
export LOG_DIR="/tmp/datagen-deploy-logs/"$(date +%m-%d-%Y-%H-%M-%S)
export LAUNCH_GENERATION=false
export USE_ANSIBLE_PYTHON_3="false"

# Target Directory
export TARGET_DIR="/tmp/datagen"

# Datagen Repository
export DATA_GEN_GIT_URL="https://github.com/frischHWC/datagen"
export DATA_GEN_GIT_BRANCH="main"
export CREATE_IN_LOCAL="false"


function usage()
{
    echo "This script launches full deployment of Datagen on a running CDP Cluster"
    echo ""
    echo "Usage is the following : "
    echo ""
    echo "./deploy.sh"
    echo "  -h --help"
    echo ""
    echo "  --cluster-name=$CLUSTER_NAME The name of the cluster to interact with (Default) "
    echo "  --edge-host=$EDGE_HOST : To create data generation from this node and install the service on it (Default) "
    echo "  --ssh-user=$SSH_USER : To connect to clusters' host (Default) $SSH_USER"
    echo "  --ssh-key=$SSH_KEY : To connect to clusters' host or provide a password (Default) "
    echo "  --ssh-password=$SSH_PASSWORD : To connect to clusters' host or provide a ssh-key (Default) "
    echo ""
    echo "  --cm-host=$CM_HOST : Cloudera Manger Host  (Default) "
    echo "  --cm-port=$CM_PORT : Cloudera Manger Host  (Default) $CM_PORT"
    echo "  --cm-protocol=$CM_PROTOCOL : Cloudera Manger Host  (Default) $CM_PROTOCOL"
    echo "  --cm-user=$CM_USER : Cloudera Manger User (Default) $CM_USER "
    echo "  --cm-password=$CM_PASSWORD : Cloudera Manger Password (associated to the above user) (Default) $CM_PASSWORD "
    echo ""
    echo "  --ranger-password=$RANGER_PASSWORD : Ranger Password (associated to the admin user) (Default) $RANGER_PASSWORD "
    echo ""
    echo "  --cdp-version=$CDP_VERSION : Version of CDP on which Datagen is deployed (Default) $CDP_VERSION "
    echo "  --datagen-version=$DATAGEN_VERSION : Version of Datagen that will be set for this deployment  (Default) $DATAGEN_VERSION"
    echo "  --distro-suffix=$DISTRO_SUFFIX : Version of OS to deploy datagen  (Default) $DISTRO_SUFFIX"
    echo ""
    echo "  --cluster-name-streaming=$CLUSTER_NAME_STREAMING The name of the streaming cluster to interact with (that contains Kafka) (Default) "
    echo ""
    echo "  --debug=$DEBUG : To set DEBUG log-level (Default) $DEBUG "
    echo "  --log-dir=$LOG_DIR : Log directory (Default) $LOG_DIR "
    echo "  --launch-generation=$LAUNCH_GENERATION : To launch API calls to generate data after installation (Default) $LAUNCH_GENERATION "
    echo "  --ansible-python-3=$USE_ANSIBLE_PYTHON_3 : (Optional) To use python3 for ansible (Default) $USE_ANSIBLE_PYTHON_3 "
    echo ""
    echo "  --target-dir=$TARGET_DIR : Target directory on edge machine (Default) $TARGET_DIR "
    echo ""
    echo "  --data-gen-git-url=$DATA_GEN_GIT_URL : Datagen Git URL to use (if different of official one) (Default) $DATA_GEN_GIT_URL "
    echo "  --data-gen-git-branch=$DATA_GEN_GIT_BRANCH : Datagen Git Branch to use (if different of official one) (Default) $DATA_GEN_GIT_BRANCH "
    echo "  --create-in-local=$CREATE_IN_LOCAL : To create locally the CSD and parcel instead of using a repository (Default) $CREATE_IN_LOCAL "
    echo ""
    echo "  --create-datagen=$CREATE_DATAGEN : To launch playbooks for creation of datagen (Default) $CREATE_DATAGEN "
    echo "  --install-datagen=$INSTALL_DATAGEN : To launch playbooks for installation of datagen (Default) $INSTALL_DATAGEN "
    echo "  --launch-datagen=$LAUNCH_DATAGEN : To launch playbooks for launch of datagen (Default) $LAUNCH_DATAGEN "
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
        --cluster-name)
            CLUSTER_NAME=$VALUE
            ;;
        --edge-host)
            EDGE_HOST=$VALUE
            ;;
        --ssh-user)
            SSH_USER=$VALUE
            ;;
        --ssh-key)
            SSH_KEY=$VALUE
            ;;
        --ssh-password)
            SSH_PASSWORD=$VALUE
            ;;
        --cm-host)
            CM_HOST=$VALUE
            ;;
        --cm-port)
            CM_PORT=$VALUE
            ;;
        --cm-protocol)
            CM_PROTOCOL=$VALUE
            ;;
        --cm-user)
            CM_USER=$VALUE
            ;;
        --cm-password)
            CM_PASSWORD=$VALUE
            ;;
        --ranger-password)
            RANGER_PASSWORD=$VALUE
            ;;
        --cdp-version)
            CDP_VERSION=$VALUE
            ;;
        --datagen-version)
            DATAGEN_VERSION=$VALUE
            ;;
        --distro-suffix)
            DISTRO_SUFFIX=$VALUE
            ;;
        --cluster-name-streaming)
            CLUSTER_NAME_STREAMING=$VALUE
            ;;
        --debug)
            DEBUG=$VALUE
            ;;
        --log-dir)
            LOG_DIR=$VALUE
            ;;
        --launch-generation)
            LAUNCH_GENERATION=$VALUE
            ;;
        --use-ansible-python-3)
            USE_ANSIBLE_PYTHON_3=$VALUE
            ;;
        --target-dir)
            TARGET_DIR=$VALUE
            ;;
        --data-gen-git-url)
            DATA_GEN_GIT_URL=$VALUE
            ;;
        --data-gen-git-branch)
            DATA_GEN_GIT_BRANCH=$VALUE
            ;;
        --create-in-local)
            CREATE_IN_LOCAL=$VALUE
            ;;
        --create-datagen)
            CREATE_DATAGEN=$VALUE
            ;;
        --install-datagen)
            INSTALL_DATAGEN=$VALUE
            ;;
        --launch-datagen)
            LAUNCH_DATAGEN=$VALUE
            ;;
        *)
            ;;
    esac
    shift
done


# Prepare hosts file to interact with cluster
HOSTS_TEMP=$(mktemp)
EXTRA_VARS_TEMP=$(mktemp)

if [ "${EDGE_HOST}" = "" ]
then
  export EDGE_HOST=${CM_HOST}
fi

mkdir -p ${LOG_DIR}

echo "
[cloudera_manager]
${CM_HOST}

[edge]
${EDGE_HOST}

[all:vars]
ansible_connection=ssh
ansible_user=${SSH_USER}
" >> ${HOSTS_TEMP}

if [ "${SSH_KEY}" != "" ]
then
    echo "ansible_ssh_private_key_file=${SSH_KEY}" >> ${HOSTS_TEMP}
elif [ "${SSH_PASSWORD}" != "" ]
then
    echo "ansible_ssh_pass=${SSH_PASSWORD}" >> ${HOSTS_TEMP}
fi

if [ -z "${CLUSTER_NAME_STREAMING}" ] || [ "${CLUSTER_NAME_STREAMING}" == "" ]
then
  export CLUSTER_NAME_STREAMING="${CLUSTER_NAME}"
fi

export CSD_LOCAL_DIR="/tmp/datagen_csd-${DATAGEN_VERSION}-${CDP_VERSION}/"
export PARCEL_LOCAL_DIR="/tmp/datagen_parcel-${DATAGEN_VERSION}-${CDP_VERSION}/"

envsubst < extra-vars.yml > ${EXTRA_VARS_TEMP}

if [ "${DEBUG}" == "true" ]
then
    echo "HOSTS File content: " 
    cat ${HOSTS_TEMP}
    echo ""
    echo "EXTRA_VARS file content: "
    cat ${EXTRA_VARS_TEMP}
    echo ""
fi

if [ "${USE_ANSIBLE_PYTHON_3}" == "true" ]
then
    export ANSIBLE_PYTHON_3_PARAMS='-e ansible_python_interpreter=/usr/bin/python3'
fi

if [ "${CREATE_DATAGEN}" == "true" ]
then
# Launch playbook to clone repo on edge host, mvn clean package, create the CSD, create the parcel
  echo "################### Creation of Datagen ###################"
  if [ "${CREATE_IN_LOCAL}" == "true" ] ; then
    cd repository/
    ./build_local_csd_parcel.sh \
      --cdp-version=${CDP_VERSION} \
      --datagen-version=${DATAGEN_VERSION} \
      --distributions-to-build=${DISTRO_SUFFIX} \
      --csd-dir=${CSD_LOCAL_DIR} \
      --parcel-dir=${PARCEL_LOCAL_DIR} \
      --debug=${DEBUG}
    cd ../
  fi
  if [ "${DEBUG}" = "true" ]
  then
      echo " Command launched: ansible-playbook -i ${HOSTS_TEMP} -e @${EXTRA_VARS_TEMP} playbooks/create_datagen.yml ${ANSIBLE_PYTHON_3_PARAMS}  "
      echo " Follow advancement at: ${LOG_DIR}/create_datagen.log "
  fi
  ansible-playbook -i ${HOSTS_TEMP} -e @${EXTRA_VARS_TEMP} playbooks/create_datagen.yml ${ANSIBLE_PYTHON_3_PARAMS}  2>&1 > ${LOG_DIR}/create_datagen.log
  OUTPUT=$(tail -20 ${LOG_DIR}/create_datagen.log | grep -A20 RECAP | grep -v "failed=0" | wc -l | xargs)
  if [ "${OUTPUT}" == "2" ]
  then
    echo " SUCCESS: Datagen Creation made "
  else
    echo " FAILURE: Could not create Datagen "
    echo " See details in file: ${LOG_DIR}/create_datagen.log "
    exit 1
  fi
fi


if [ "${INSTALL_DATAGEN}" == "true" ]
then
# Launch playbook to remove any parcel, csd if existing, install the parcel and the csd
echo "################### Installation of Datagen ###################"
if [ "${DEBUG}" = "true" ]
then
    echo " Command launched: ansible-playbook -i ${HOSTS_TEMP} -e @${EXTRA_VARS_TEMP} playbooks/install_datagen.yml ${ANSIBLE_PYTHON_3_PARAMS}  "
    echo " Follow advancement at: ${LOG_DIR}/install_datagen.log "
fi
ansible-playbook -i ${HOSTS_TEMP} -e @${EXTRA_VARS_TEMP} playbooks/install_datagen.yml ${ANSIBLE_PYTHON_3_PARAMS}  2>&1 > ${LOG_DIR}/install_datagen.log
OUTPUT=$(tail -20 ${LOG_DIR}/install_datagen.log | grep -A20 RECAP | grep -v "failed=0" | wc -l | xargs)
if [ "${OUTPUT}" == "2" ]
then
  echo " SUCCESS: Datagen Installation made "
else
  echo " FAILURE: Could not install Datagen " 
  echo " See details in file: ${LOG_DIR}/install_datagen.log "
  exit 1
fi
fi


if [ "${LAUNCH_DATAGEN}" == "true" ]
then
# Launch playbook to install the service on edge host using CM apis and launch any required command
echo "################### Launch of Datagen ###################"
if [ "${DEBUG}" = "true" ]
then
    echo " Command launched: ansible-playbook -i ${HOSTS_TEMP} -e @${EXTRA_VARS_TEMP} playbooks/launch_datagen.yml ${ANSIBLE_PYTHON_3_PARAMS}  "
    echo " Follow advancement at: ${LOG_DIR}/launch_datagen.log "
fi
ansible-playbook -i ${HOSTS_TEMP} -e @${EXTRA_VARS_TEMP} playbooks/launch_datagen.yml ${ANSIBLE_PYTHON_3_PARAMS}  2>&1 > ${LOG_DIR}/launch_datagen.log
OUTPUT=$(tail -20 ${LOG_DIR}/launch_datagen.log | grep -A20 RECAP | grep -v "failed=0" | wc -l | xargs)
if [ "${OUTPUT}" == "2" ]
then
  echo " SUCCESS: Launch of Datagen made "
else
  echo " FAILURE: Could not launch Datagen " 
  echo " See details in file: ${LOG_DIR}/launch_datagen.log "
  exit 1
fi
fi


# Clean files

rm -rf ${HOSTS_TEMP}
rm -rf ${EXTRA_VARS_TEMP}

echo "******************************************"
echo "     Deployment of DATAGEN Finished "
echo "******************************************"