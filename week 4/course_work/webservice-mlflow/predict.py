import pickle

import mlflow
from flask import Flask, jsonify, request

RUN_ID = "a1397aaac61141a9a77920a9e42ce066"
logged_model = f"s3://mlflow--bucket/1/{RUN_ID}/artifacts/model"
model = mlflow.pyfunc.load_model(logged_model)

app = Flask("Duration-prediction")


def preprocess(ride):
    features = {}
    features["PU_DO"]: f"{ride['PULocationID']}_{ride['DOLocationID']}"
    features["trip_distance"] = ride["trip_distance"]

    return features


def predict(features):
    preds = model.predict(features)
    return float(preds[0])


@app.route("/predict", methods={"POST"})
def predict_endpoint():
    # prediction end point

    ride = request.get_json()

    features = preprocess(ride)
    pred = predict(features)
    result = {"duration": pred, "model_version": RUN_ID}
    return jsonify(result)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=9696)
