#!/usr/bin/env python
# coding: utf-8


import os
import pickle
import sys
import uuid

import mlflow
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.feature_extraction import DictVectorizer
from sklearn.metrics import mean_squared_error


def read_dataframe(filename: str):
    df = pd.read_parquet(filename)

    df["duration"] = df.lpep_dropoff_datetime - df.lpep_pickup_datetime
    df.duration = df.duration.dt.total_seconds() / 60
    df = df[(df.duration >= 1) & (df.duration <= 60)]
    df["ride_ids"] = create_uuids(len(df))

    return df


def prepare_dictionaries(df: pd.DataFrame):
    categorical = ["PULocationID", "DOLocationID"]
    df[categorical] = df[categorical].astype(str)
    df["PU_DO"] = df["PULocationID"] + "_" + df["DOLocationID"]
    categorical = ["PU_DO"]
    numerical = ["trip_distance"]
    dicts = df[categorical + numerical].to_dict(orient="records")
    return dicts


def create_uuids(n):

    ride_ids = []
    for i in range(n):
        ride_ids.append(str(uuid.uuid4()))

    return ride_ids


def load_model(run_id):
    logged_model = f"s3://mlflow--bucket/1/{run_id}/artifacts/model"
    model = mlflow.pyfunc.load_model(logged_model)
    return model


def apply_model(input_file, run_id, output_file):
    print(f"Loading the data from {input_file}")
    df = read_dataframe(input_file)
    dict_df = prepare_dictionaries(df)
    print("Loading the model with RUN_ID: {run_id}")
    model = load_model(run_id)
    print("Applying the model")
    y_preds = model.predict(dict_df)

    df_results = pd.DataFrame()
    df_results["ride_id"] = df["ride_ids"]
    df_results["lpep_pickup_datetime"] = df["lpep_pickup_datetime"]
    df_results["PULocationID"] = df["PULocationID"]
    df_results["DOLocationID"] = df["DOLocationID"]
    df_results["predicted_duration"] = y_preds
    df_results["actual_duration"] = df["duration"]
    df_results["duration_difference"] = (
        df["duration"] - df_results["predicted_duration"]
    )
    df_results["model_version"] = run_id
    print(f"Saving the results to {output_file}")
    df.to_parquet(f"{output_file}", index=False)


# In[6]:


def run():
    taxi_type = sys.argv[1]  # "green"
    year = int(sys.argv[2])  # 2021
    month = int(sys.argv[3])
    run_id = sys.argv[4]

    input_file = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year:04d}-{month:02d}.parquet"
    output_folder = "output"
    output_path = f"{output_folder}/{taxi_type}"
    output_file = f"{output_path}/{os.path.basename(input_file)}"

    os.makedirs(output_path, exist_ok=True)
    apply_model(input_file, run_id, output_file)


if __name__ == "__main__":
    run()
