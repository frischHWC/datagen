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
# Credentials
export AWS_ACCESS_KEY_ID=""
export AWS_SECRET_ACCESS_KEY=""
export AWS_S3_BUCKET="datagen-repo"
export AWS_S3_REPO="s3.eu-west-3.amazonaws.com"

# Versions
export CDP_VERSION="7.1.9.4"
export DATAGEN_VERSION="1.0.0"

# CSD & Parcels Directory
export CSD_DIR="/tmp/datagen_csd"
export PARCEL_DIR="/tmp/datagen_parcel"

# Standalone Directory
export STANDALONE_DIR="/tmp/datagen_standalone"
export MAIN_STANDALONE_VERSION="false"
export MODELS_DIR="/tmp/datagen_models"
export K8S_DIR="/tmp/datagen_k8s"

# DEBUG
export DEBUG=false
export LOG_DIR="/tmp/datagen-push-to-repo-logs/"$(date +%m-%d-%Y-%H-%M-%S)

# Steps to DO
export UPLOAD="true"
export INDEX="true"

function usage()
{
    echo "This script push Datagen already generated CSD & Parcels to AWS S3"
    echo ""
    echo "Usage is the following : "
    echo ""
    echo "./push_parcel_csd_to_s3.sh"
    echo "  -h --help"
    echo ""
    echo "  --aws-access-key=$AWS_ACCESS_KEY_ID : Mandatory to get access to AWS account"
    echo "  --aws-secret-access-key=$AWS_SECRET_ACCESS_KEY : Mandatory to get access to AWS account"
    echo ""
    echo "  --cdp-version=$CDP_VERSION : Version of CDP on which Datagen is deployed (Default) $CDP_VERSION "
    echo "  --datagen-version=$DATAGEN_VERSION : Version of Datagen that will be set for this deployment  (Default) $DATAGEN_VERSION"
    echo ""
    echo "  --csd-dir=$CSD_DIR : CSD Directory where it has been generated (Default) $CSD_DIR "
    echo "  --parcel-dir=$PARCEL_DIR : Directory where parcels have been generated  (Default) $PARCEL_DIR"
    echo "  --standalone-dir=$STANDALONE_DIR : Directory where standalone files have been generated  (Default) $STANDALONE_DIR"
    echo "  --model-dir=$MODEL_DIR : Directory where model files have been generated  (Default) $MODEL_DIR"
    echo "  --k8s-dir=$K8S_DIR : Directory where k8s files have been generated  (Default) $K8S_DIR"
    echo "  --main-standalone-version=$MAIN_STANDALONE_VERSION : If it is the main standalone version to publish to root of datagen version (Default) $MAIN_STANDALONE_VERSION"
    echo ""
    echo "  --debug=$DEBUG : To set DEBUG log-level (Default) $DEBUG "
    echo "  --log-dir=$LOG_DIR : Log directory (Default) $LOG_DIR "
    echo ""
    echo "  --upload=$UPLOAD : To upload parcels & CSD or not (Default) $UPLOAD "
    echo "  --index=$INDEX : To push index generated files or not (Default) $INDEX "
}


while [ "$1" != "" ]; do
    PARAM=`echo $1 | awk -F= '{print $1}'`
    VALUE=`echo $1 | awk -F= '{print $2}'`
    case $PARAM in
        -h | --help)
            usage
            exit
            ;;
        --aws-access-key)
            AWS_ACCESS_KEY_ID=$VALUE
            ;;
        --aws-secret-access-key)
            AWS_SECRET_ACCESS_KEY=$VALUE
            ;;
        --cdp-version)
            CDP_VERSION=$VALUE
            ;;
        --datagen-version)
            DATAGEN_VERSION=$VALUE
            ;;
        --csd-dir)
            CSD_DIR=$VALUE
            ;;
        --parcel-dir)
            PARCEL_DIR=$VALUE
            ;;
        --standalone-dir)
            STANDALONE_DIR=$VALUE
            ;;
        --model-dir)
            MODEL_DIR=$VALUE
            ;;
        --k8s-dir)
            K8S_DIR=$VALUE
            ;;
        --main-standalone-version)
            MAIN_STANDALONE_VERSION=$VALUE
            ;;
        --debug)
            DEBUG=$VALUE
            ;;
        --log-dir)
            LOG_DIR=$VALUE
            ;;
        --upload)
            UPLOAD=$VALUE
            ;;
        --index)
            INDEX=$VALUE
            ;;
        *)
            ;;
    esac
    shift
done

mkdir -p ${LOG_DIR}


# 1. Upload Parcels & CSD files to AWS
if [ ${UPLOAD} = "true" ]
then
    # Upload CSD
    CSD_JAR_FILE_NAME=$(ls ${CSD_DIR} | grep jar)
    if [ ! -z ${CSD_JAR_FILE_NAME} ]
    then
      echo "Upload to AWS ${AWS_S3_BUCKET}/${DATAGEN_VERSION}/CDP/${CDP_VERSION}/ file: ${CSD_DIR}/${CSD_JAR_FILE_NAME}"
      aws s3 cp ${CSD_DIR}/${CSD_JAR_FILE_NAME} s3://${AWS_S3_BUCKET}/${DATAGEN_VERSION}/CDP/${CDP_VERSION}/csd/
    fi
    
    # Upload Parcel
    PARCEL_FILES=$(ls ${PARCEL_DIR})
    if [ ! -z "${PARCEL_FILES}" ]
    then
      echo "Upload to AWS ${AWS_S3_BUCKET}/${DATAGEN_VERSION}/CDP/${CDP_VERSION}/ files: ${PARCEL_DIR}/${PARCEL_FILES}"
      aws s3 cp ${PARCEL_DIR}/ s3://${AWS_S3_BUCKET}/${DATAGEN_VERSION}/CDP/${CDP_VERSION}/parcels/ --recursive
    fi

    # Upload Standalone files as MAIN Standalone Files of DATAGEN RELEASE
    STANDALONE_FILES=$(ls ${STANDALONE_DIR})
    if [[ ! -z "${STANDALONE_FILES}" ]] && [[ "${MAIN_STANDALONE_VERSION}" == "true" ]]
    then
      echo "Upload to AWS ${AWS_S3_BUCKET}/${DATAGEN_VERSION}/standalone/ files: ${STANDALONE_DIR}/${STANDALONE_FILES}"
      aws s3 cp ${STANDALONE_DIR}/ s3://${AWS_S3_BUCKET}/${DATAGEN_VERSION}/standalone/ --recursive
    fi

    # Upload Models files
    MODEL_FILES=$(ls ${MODEL_DIR})
    if [[ ! -z "${MODEL_FILES}" ]] && [[ "${MAIN_STANDALONE_VERSION}" == "true" ]]
    then
      echo "Upload to AWS ${AWS_S3_BUCKET}/${DATAGEN_VERSION}/models/ files: ${MODEL_DIR}/${MODEL_FILES}"
      aws s3 cp ${MODEL_DIR}/ s3://${AWS_S3_BUCKET}/${DATAGEN_VERSION}/models/ --recursive
    fi

    # Upload Docker-K8s files
    K8S_FILES=$(ls ${K8S_DIR})
    if [[ ! -z "${K8S_FILES}" ]] && [[ "${MAIN_STANDALONE_VERSION}" == "true" ]]
    then
      echo "Upload to AWS ${AWS_S3_BUCKET}/${DATAGEN_VERSION}/docker/ files: ${K8S_DIR}/${K8S_FILES}"
      aws s3 cp ${K8S_DIR}/ s3://${AWS_S3_BUCKET}/${DATAGEN_VERSION}/docker/ --recursive
    fi

fi


# Upload and update index.html files to add new values
function create_index_file()
{
  title=$1
  subtitle=$2
  bucket_dir=$3
  s3_repo="${AWS_S3_REPO}"

  # Get previous directory
  bucket_dir_file=$(mktemp)
  echo "$3" > $bucket_dir_file
  bucket_previous_dir=$(rev $bucket_dir_file | cut -d'/' -f3- | rev)
  if [ ! -z $bucket_previous_dir ]
  then
    bucket_previous_dir=${bucket_previous_dir}"/"
  fi

    export INDEX_TEMP_FILE=$(mktemp)
    echo "<!DOCTYPE html>
          <html>
          <head>
              <title>${title}</title>
          </head>
          <html>
          <h2>${subtitle}:</h2>
          <hr/>
          <table border="0">
              <tr>
                  <th>Name</th><th>Last Modified</th><th>Size</th>
              </tr>
              <tr>
                  <td><a href="https://${AWS_S3_BUCKET}.${s3_repo}/${bucket_previous_dir}index.html">Parent Directory</a></td>
              </tr>" > ${INDEX_TEMP_FILE}

    FILES_TO_INDEX=$(aws s3 ls s3://${AWS_S3_BUCKET}/${bucket_dir} | grep -v PRE)
    DIR_TO_INDEX=$(aws s3 ls s3://${AWS_S3_BUCKET}/${bucket_dir} | grep PRE)

    while IFS= read -r line; do
        DIR_NAME=$( echo $line | awk '{ print $2 }')
        echo "<tr>
                      <td><a href="https://${AWS_S3_BUCKET}.${s3_repo}/${bucket_dir}${DIR_NAME}index.html">${DIR_NAME}</a></td>
                      <td>-</td>
                      <td>-</td>
                  </tr>" >> ${INDEX_TEMP_FILE}
    done <<< "$DIR_TO_INDEX"

    while IFS= read -r line; do
        FILE_NAME=$( echo $line | awk '{ print $4 }')
        if [ ! -z "${FILE_NAME}" ] && [ ${FILE_NAME} != "index.html" ]
        then
          FILE_DATE=$( echo $line | awk '{ print $1 $2 }')
          FILE_SIZE=$( echo $line | awk '{ print $3 }')
          FILE_SIZE_FORMATTED=$( numfmt --to=iec-i --suffix=B --format="%9.2f" $FILE_SIZE)
          echo "<tr>
                              <td><a href="https://${AWS_S3_BUCKET}.${s3_repo}/${bucket_dir}${FILE_NAME}">${FILE_NAME}</a></td>
                              <td>${FILE_DATE}</td>
                              <td>${FILE_SIZE_FORMATTED}</td>
                          </tr>" >> ${INDEX_TEMP_FILE}
        fi
    done <<< "$FILES_TO_INDEX"

    echo "</table>
          <hr/>
          </html>" >> ${INDEX_TEMP_FILE}

    if [ ${DEBUG} = "true" ]
    then
      echo "index.html for bucket directory: ${bucket_dir} has following content: "
      cat ${INDEX_TEMP_FILE}
    fi

    aws s3 cp ${INDEX_TEMP_FILE} s3://${AWS_S3_BUCKET}/${bucket_dir}index.html --content-type "text/html"
}


if [ ${INDEX} = "true" ]
then
  create_index_file "Datagen Repository" "Datagen Versions"
  create_index_file "Datagen Repository" "Available Versions for Datagen: ${DATAGEN_VERSION}" ${DATAGEN_VERSION}/

  create_index_file "Datagen Repository" "Available CDP Versions for Datagen: ${DATAGEN_VERSION}" ${DATAGEN_VERSION}/CDP/

  create_index_file "Datagen Repository" "CSD & Parcels for CDP Version: ${CDP_VERSION} of Datagen: ${DATAGEN_VERSION}" ${DATAGEN_VERSION}/CDP/${CDP_VERSION}/
  create_index_file "Datagen Repository" "CSD files for CDP Version: ${CDP_VERSION} of Datagen: ${DATAGEN_VERSION}" ${DATAGEN_VERSION}/CDP/${CDP_VERSION}/csd/
  create_index_file "Datagen Repository" "Parcels files for CDP Version: ${CDP_VERSION} of Datagen: ${DATAGEN_VERSION}" ${DATAGEN_VERSION}/CDP/${CDP_VERSION}/parcels/

  create_index_file "Datagen Repository" "Standalone files of Datagen: ${DATAGEN_VERSION}" ${DATAGEN_VERSION}/standalone/

  create_index_file "Datagen Repository" "Docker/K8s files of Datagen: ${DATAGEN_VERSION}" ${DATAGEN_VERSION}/docker/

  create_index_file "Datagen Repository" "Model files of Datagen: ${DATAGEN_VERSION}" ${DATAGEN_VERSION}/models/
  create_index_file "Datagen Repository" "Model files of Datagen: ${DATAGEN_VERSION}" ${DATAGEN_VERSION}/models/use-cases/
  create_index_file "Datagen Repository" "Model files of Datagen: ${DATAGEN_VERSION}" ${DATAGEN_VERSION}/models/use-cases/stores/
  create_index_file "Datagen Repository" "Model files of Datagen: ${DATAGEN_VERSION}" ${DATAGEN_VERSION}/models/customer/
  create_index_file "Datagen Repository" "Model files of Datagen: ${DATAGEN_VERSION}" ${DATAGEN_VERSION}/models/finance/
  create_index_file "Datagen Repository" "Model files of Datagen: ${DATAGEN_VERSION}" ${DATAGEN_VERSION}/models/industry/
  create_index_file "Datagen Repository" "Model files of Datagen: ${DATAGEN_VERSION}" ${DATAGEN_VERSION}/models/public_service/
  create_index_file "Datagen Repository" "Model files of Datagen: ${DATAGEN_VERSION}" ${DATAGEN_VERSION}/models/travel/

fi