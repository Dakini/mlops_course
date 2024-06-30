from pathlib import Path
import model


class ModelMock:
    def __init__(self, value):
        self.value = value

    def predict(self, x):
        # len_x = len(x)
        return [self.value]


def test_predict():
    model_service = model.ModelService(None)
    model_mock = ModelMock(10.0)

    ride = {"PULocationID": 130, "DOLocationID": 205, "trip_distance": 3.66}

    features = model_service.prepare_features(ride)

    actual_preds = model_mock.predict(features)
    expected_preds = [10.0]
    assert actual_preds == expected_preds


def read_text(file):
    test_directory = Path(__file__).parent
    with open(f"{test_directory}/{file}", "rt", encoding="utf-8") as f_in:
        return f_in.read().strip()


def test_base64_decode():
    base64_data = read_text("data.b64")
    decoded_data = model.debase64_decode(base64_data)

    expected_result = {
        "ride": {"PULocationID": 130, "DOLocationID": 205, "trip_distance": 3.66},
        "ride_id": 256,
    }
    assert decoded_data == expected_result


def test_prepare_features():
    model_service = model.ModelService(10.0)
    ride = {"PULocationID": 130, "DOLocationID": 205, "trip_distance": 3.66}

    features = model_service.prepare_features(ride)

    expected_features = {"PU_DO": "130_205", "trip_distance": 3.66}

    assert features == expected_features


def test_lambda_handler():

    model_mock = ModelMock(10.0)
    model_verison = "Test123"

    model_service = model.ModelService(model_mock, model_version=model_verison)
    base_64 = read_text("data.b64")
    event = event = {
        "Records": [
            {
                "kinesis": {
                    "data": base_64,
                },
            }
        ]
    }
    actual_predictions = model_service.lambda_handler(event)
    expected_predictions = {
        "predictions": [
            {
                "model": "ride_duration_prediction_model",
                "version": model_verison,
                "prediction": {"ride_duration": 10.0, "ride_id": 256},
            }
        ]
    }

    assert actual_predictions == expected_predictions
    # features = model_service.prepare_features(ride)

    # actual_preds = model_mock.predict(features)
    # expected_preds = 10.0
    # assert actual_preds == expected_preds
