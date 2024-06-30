#!/usr/bin/env bash

cd "$(dirname "$0")"
if [ "${LOCAL_IMAGE_NAME}" == "" ]; then 
    LOCAL_TAG=`date +"%Y-%m-%d-%H-%M"`
    export LOCAL_IMAGE_NAME="stream-model-duration:${LOCAL_TAG}"
    echo "LOCAL_IMAGE_NAME is not set, building a new image with tag ${LOCAL_IMAGE_NAME}"
    docker build -t ${LOCAL_IMAGE_NAME} ..
else
    echo "no need to build image ${LOCAL_IMAGE_NAME}"
fi


export PREDICTIONS_STREAM_NAME="ride_predictions"
export KINESIS_ENDPOINT_URL="http://localhost:4566/"



docker-compose up -d
sleep 5

aws --endpoint-url=http://localhost:4566 \
    kinesis create-stream \
    --stream-name ${PREDICTIONS_STREAM_NAME} \
    --shard-count 1

echo "Completed the Kinesis stream"
aws --endpoint-url=${KINESIS_ENDPOINT_URL} kinesis list-streams

# pipenv run python test_local_kinesis.py
echo "Kinessis endpoint is ${KINESIS_ENDPOINT_URL}"
pipenv run python test_docker.py

ERROR_CODE=$?
if [${ERROR_CODE} !=0 ]; then
    docker-compose logs
    docker-compose down
    exit ${ERROR_CODE}
fi
pipenv run python test_local_kinesis.py

ERROR_CODE=$?
if [${ERROR_CODE} !=0 ]; then
    docker-compose logs
    docker-compose down
    exit ${ERROR_CODE}
fi
pipenv run python test_kinesis.py
ERROR_CODE=$?
if [${ERROR_CODE} !=0 ]; then
    docker-compose logs
    docker-compose down
    exit ${ERROR_CODE}
fi
docker-compose down