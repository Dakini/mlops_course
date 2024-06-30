## Build and testing locally

```bash
docker build -t stream-model-duration:v2 .

docker run -it --rm \
    -p 8080:8080 \
    -e PREDICTIONS_STREAM_NAME="ride_predictions" \
    -e RUN_ID="6c3c482fc1144bbfa3f9263d4ab0349d" \
    -e TEST_RUN="True" \
    -v ~/.aws/:/root/.aws \
    stream-model-duration:v2
```

## local model

```bash
docker run -it --rm \
    -p 8080:8080 \
    -e PREDICTIONS_STREAM_NAME="ride_predictions" \
    -e RUN_ID="6c3c482fc1144bbfa3f9263d4ab0349d" \
    -e TEST_RUN="True" \
    -e MODEL_LOCATION="/app/model" \
    -v ./integration-test/model:/app/model \
    stream-model-duration:v2

```

check localstack kinese

```bash
aws --endpoint-url=http://localhost:4566 kinesis list-streams
```

## create stream

```bash
aws --endpoint-url=http://localhost:4566 \
kinesis create-stream \
--stream-name  ride_predictions \
--shard-count 1
```

aws --endpoint-url=http://localhost:4566 \
 kinesis create-stream \
 --stream-name ride_predictions \  
 --shard-count 1
