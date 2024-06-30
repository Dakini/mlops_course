import json
import os

import boto3

PREDICTIONS_STREAM_NAME = os.getenv("PREDICTIONS_STREAM_NAME")
RUN_ID = os.getenv("RUN_ID")


prediction_event = {
    "model": "ride_duration_prediction_model",
    "version": "Test123",
    "prediction": {"ride_duration": 21.3, "ride_id": 256},
}
endpoint_url = os.getenv("KINESIS_ENDPOINT_URL")
print(f"End point url is: {endpoint_url}")
if endpoint_url is None:
    client = boto3.client("kinesis")
else:

    client = boto3.client("kinesis", endpoint_url=endpoint_url)

ride_id = prediction_event["prediction"]["ride_id"]
print(ride_id, PREDICTIONS_STREAM_NAME)
client.put_record(
    StreamName=PREDICTIONS_STREAM_NAME,
    Data=json.dumps(prediction_event),
    PartitionKey=str(ride_id),
)


shard_id = "shardId-000000000000"


shard_iterator_response = client.get_shard_iterator(
    StreamName=PREDICTIONS_STREAM_NAME,
    ShardId=shard_id,
    ShardIteratorType="TRIM_HORIZON",
)
shard_iterator_id = shard_iterator_response["ShardIterator"]


records_response = client.get_records(
    ShardIterator=shard_iterator_id,
    Limit=1,
)


records = records_response["Records"]
print(records)
