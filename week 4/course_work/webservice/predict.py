import pickle

from flask import Flask, jsonify, request

with open("lin_reg.bin", "rb") as file:
    dv, model = pickle.load(file)

app = Flask("Duration-prediction")


def preprocess(ride):
    features = {}
    features["PU_DO"]: f"{ride['PULocationID']}_{ride['DOLocationID']}"
    features["trip_distance"] = ride["trip_distance"]

    return features


def predict(features):
    X = dv.transform(features)
    preds = model.predict(X)
    return float(preds[0])


@app.route("/predict", methods={"POST"})
def predict_endpoint():
    # prediction end point

    ride = request.get_json()

    features = preprocess(ride)
    pred = predict(features)
    result = {"duration": pred}
    return jsonify(result)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=9696)
