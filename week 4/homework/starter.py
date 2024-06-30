#!/usr/bin/env python
# coding: utf-8


import argparse
import logging
import os
import pickle

import boto3
import pandas as pd
from botocore.exceptions import ClientError


def load_model():
    with open("model.bin", "rb") as f_in:
        dv, model = pickle.load(f_in)
    return dv, model


dv, model = load_model()


def read_data(filename, categorical):
    df = pd.read_parquet(filename)

    df["duration"] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df["duration"] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype("int").astype("str")

    return df


def save_output(df, y_pred, bucket):
    output_file = "results.parquet"

    df_result = pd.DataFrame()
    df_result["ride_id"] = df["ride_id"]
    df_result["predictions"] = y_pred

    df_result.to_parquet(output_file, engine="pyarrow", compression=None, index=False)

    if bucket is not None:
        upload_file(output_file, bucket)


def download_data(year, month, categorical):
    df = read_data(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet",
        categorical,
    )

    df["ride_id"] = f"{year:04d}/{month:02d}_" + df.index.astype("str")
    return df


def preprocess_data(df, dv, categorical):
    dicts = df[categorical].to_dict(orient="records")
    X_val = dv.transform(dicts)
    return X_val


def apply_model(year, month, bucket):
    print("loading model")
    categorical = ["PULocationID", "DOLocationID"]
    dv, model = load_model()

    print("Downloading Data")
    df = download_data(year, month, categorical)

    print("Preprocessing Data")
    X_val = preprocess_data(df, dv, categorical)

    y_pred = model.predict(X_val)

    save_output(df, y_pred, bucket)
    print(f"Pred mean duration is :{y_pred.mean()}")


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client("s3")
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-y", "--year", type=int, required=True)
    parser.add_argument("-m", "--month", type=int, required=True)
    parser.add_argument("-b", "--bucket", type=str, required=False)
    args = parser.parse_args()

    apply_model(args.year, args.month, args.bucket)
