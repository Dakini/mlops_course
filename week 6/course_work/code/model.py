import base64
import json
import os

import boto3
import mlflow

# kinesis_client = boto3.client("kinesis")


def get_model_location(run_id):
    model_location = os.getenv("MODEL_LOCATION")
    if model_location is not None:
        print(f"Loading model locally from: {model_location}")
        return model_location

    model_bucket = os.getenv("EXPERIMENT_BUCKET", "mlflow--bucket")
    experiment_id = os.getenv("MLFLOW_EXPERIMENT_ID", "1")
    model_path = f"s3://{model_bucket}/{experiment_id}/{run_id}/artifacts/model"
    return model_path


def load_model(run_id: str):
    model_path = get_model_location(run_id)
    model = mlflow.pyfunc.load_model(model_path)

    return model


def debase64_decode(encoded_data):
    decoded_data = base64.b64decode(encoded_data).decode("utf-8")
    ride_event = json.loads(decoded_data)
    return ride_event


class KinesisCallback:
    # pylint: disable=too-few-public-methods
    def __init__(self, kinesis_client, prediction_stream_name):
        self.kinesis_client = kinesis_client
        self.prediction_stream_name = prediction_stream_name

    def put_record(self, prediction_event):
        ride_id = prediction_event["predictions"]["ride_id"]

        self.kinesis_client.put_record(
            StreamName=self.prediction_stream_name,
            Data=json.dumps(prediction_event),
            PartitionKey=str(ride_id),
        )


def create_kinesis_client():
    endpoint_url = os.getenv("KINESIS_ENPOINT_URL")
    if endpoint_url is None:
        return boto3.client("kinesis")
    return boto3.client("kinesis", endpoint_url=endpoint_url)


def init(prediction_stream_name: str, run_id: str, test_run: bool):

    model = load_model(run_id)

    callbacks = []
    if not test_run:
        kinesis_client = create_kinesis_client()
        print(prediction_stream_name)
        kinesis_callback = KinesisCallback(kinesis_client, prediction_stream_name)
        # callbacks.append(kinesis_callback.put_record)
    model_service = ModelService(model=model, model_version=run_id, callbacks=callbacks)
    return model_service


class ModelService:

    def __init__(self, model, model_version=None, callbacks=None):
        self.model = model
        self.model_version = model_version
        self.callbacks = callbacks or []

    def prepare_features(self, ride):
        features = {}
        features["PU_DO"] = f"{ride["PULocationID"]}_{ride["DOLocationID"]}"
        features["trip_distance"] = ride["trip_distance"]
        return features

    def predict(self, features):
        pred = self.model.predict(features)
        return float(pred[0])

    def lambda_handler(self, event):

        predictions_events = []

        for record in event["Records"]:
            encoded_data = record["kinesis"]["data"]
            ride_event = debase64_decode(encoded_data)

            # print(ride_event)
            ride = ride_event["ride"]
            ride_id = ride_event["ride_id"]

            features = self.prepare_features(ride)
            prediction = self.predict(features)

            prediction_event = {
                "model": "ride_duration_prediction_model",
                "version": self.model_version,
                "prediction": {"ride_duration": prediction, "ride_id": ride_id},
            }

            for callback in self.callbacks:
                print("mmo")
                # callback(prediction_event)

            predictions_events.append(prediction_event)

        return {"predictions": predictions_events}
