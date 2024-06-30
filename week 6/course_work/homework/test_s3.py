import pandas as pd
import boto3
from io import BytesIO
import logging

# Set up logging to see detailed output
logging.basicConfig(level=logging.INFO)

# Create a sample dataframe
df = pd.DataFrame({"column1": [1, 2, 3, 4], "column2": ["A", "B", "C", "D"]})

# Save the dataframe to a parquet file in memory
buffer = BytesIO()
df.to_parquet(buffer, engine="pyarrow")
buffer.seek(0)  # Reset buffer position to the beginning

# Configure boto3 to use LocalStack
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1",
)

# Check if the bucket exists; if not, create it
bucket_name = "my-local-bucket"
try:
    s3.head_bucket(Bucket=bucket_name)
    logging.info(f"Bucket '{bucket_name}' already exists.")
except boto3.exceptions.botocore.client.ClientError as e:
    error_code = int(e.response["Error"]["Code"])
    if error_code == 404:
        # The bucket does not exist, create it
        logging.info(f"Bucket '{bucket_name}' does not exist. Creating it.")
        s3.create_bucket(Bucket=bucket_name)
    else:
        # Re-raise the exception if it's not a 404 error
        raise

# Upload the parquet file to the S3 bucket
try:
    s3.upload_fileobj(buffer, bucket_name, "sample.parquet")
    logging.info("Parquet file uploaded successfully to LocalStack S3!")
    s3_path = f"s3://{bucket_name}/sample.parquet"

    # Define storage options
    storage_options = {
        "key": "test",
        "secret": "test",
        "client_kwargs": {"endpoint_url": "http://localhost:4566"},
    }
    df.to_parquet(s3_path, engine="pyarrow", storage_options=storage_options)
    print("success")
except Exception as e:
    logging.error(f"Error uploading file: {e}")
    raise
