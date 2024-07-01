import pandas as pd
import os
import batch

from datetime import datetime


def dt(hour, minute, second=0):
    return datetime(2023, 1, 1, hour, minute, second)


S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
print(S3_ENDPOINT_URL)
year = 2023
month = 1

input_file = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet"
# print(input_file)
# df_input = pd.read_parquet(input_file)
data = [
    (None, None, dt(1, 1), dt(1, 10)),
    (1, 1, dt(1, 2), dt(1, 10)),
    (1, None, dt(1, 2, 0), dt(1, 2, 59)),
    (3, 4, dt(1, 2, 0), dt(2, 2, 1)),
]

columns = [
    "PULocationID",
    "DOLocationID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
]
df_input = pd.DataFrame(data, columns=columns)

options = {"client_kwargs": {"endpoint_url": S3_ENDPOINT_URL}}
input_file = batch.get_input_path(year, month)
print(input_file)
df = df_input.to_parquet(
    input_file, engine="pyarrow", compression=None, index=False, storage_options=options
)
## Read file and print file size
batch.main(year, month)
df = batch.read_data(input_file, ["PULocationID", "DOLocationID"])
print(df)
# Get the memory usage of the dataframe
memory_usage = df.memory_usage(deep=True).sum() * 1024

print(f"Size of the DataFrame is {memory_usage} ")
