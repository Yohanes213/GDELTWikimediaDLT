# Databricks notebook source
from dotenv import load_dotenv
import os
import sys
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient

project_root = os.path.abspath(os.path.join('..'))
sys.path.append(project_root)

from src.download_and_upload_gdelt import download_and_upload_gdelt_data_range

# COMMAND ----------

# Load environment variables from .env file
load_dotenv()

# Read and validate required environment variables
container_name = os.getenv("GDLET_CONTAINER_NAME")
storage_account_name = os.getenv("STORAGE_ACCOUNT_NAME")
storage_account_key = os.getenv("STORAGE_KEY")

# Ensure all required environment variables are set
required_vars = {
    "CONTAINER_NAME": container_name,
    "STORAGE_ACCOUNT_NAME": storage_account_name,
    "STORAGE_KEY": storage_account_key,
}

missing_vars = [var for var, value in required_vars.items() if value is None]

if missing_vars:
    raise EnvironmentError(
        f"Missing required environment variables: {', '.join(missing_vars)}"
    )

# COMMAND ----------

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
    storage_account_key
)

# COMMAND ----------

# Initialize Blob Storage client
connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={storage_account_key};EndpointSuffix=core.windows.net"
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(container_name)

if not container_client.exists():
    raise ValueError(f"The container '{container_name}' does not exist or the connection is invalid.")

print("Connection to Blob Storage is valid.")

# COMMAND ----------

download_and_upload_gdelt_data_range("20250701030000", "20250713000000", container_client)

# COMMAND ----------

