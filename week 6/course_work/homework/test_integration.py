import pandas as pd
import os
import batch

S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
print(S3_ENDPOINT_URL)
year = 2023
month = 1

input_file = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet"
print(input_file)
df_input = pd.read_parquet(input_file)
options = {"client_kwargs": {"endpoint_url": S3_ENDPOINT_URL}}
input_file = batch.get_input_path(year, month)
print(input_file)
df = df_input.to_parquet(
    input_file, engine="pyarrow", compression=None, index=False, storage_options=options
)
## Read file and print file size

df = batch.read_data(input_file, ["PULocationID", "DOLocationID"])
# Get the memory usage of the dataframe
memory_usage = df.memory_usage(deep=True).sum()

print(f"Size of the DataFrame is {memory_usage/1024 /1024} kilo bytes")
