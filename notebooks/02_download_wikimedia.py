# Databricks notebook source
from dotenv import load_dotenv
import os
import sys
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient

# COMMAND ----------

# Load environment variables from .env file
load_dotenv()

# Read and validate required environment variables
container_name = os.getenv("WIKI_CONTAINER_NAME")
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



# COMMAND ----------

import requests
import time
import io
from azure.storage.blob import ContainerClient

def stream_and_upload_wikimedia(
    container_client: ContainerClient,
    batch_size: int = 100,
    max_batches: int = None,
    sleep_interval: float = 0.1
):
    """
    Connects to the Wikimedia recent-change SSE feed, buffers `batch_size`
    JSON lines, and uploads each batch as a blob.
    
    Args:
      container_client: Azure Blob ContainerClient already authenticated.
      batch_size: number of events per blob.
      max_batches: if set, stops after uploading this many blobs.
      sleep_interval: pause (in seconds) when the stream is idle.
    """
    url = "https://stream.wikimedia.org/v2/stream/recentchange"
    batch = []
    batches_uploaded = 0
    
    try:
        with requests.get(url, stream=True) as resp:
            resp.raise_for_status()
            for line in resp.iter_lines():
                if not line:
                    time.sleep(sleep_interval)
                    continue

                text = line.decode("utf-8", errors="ignore")

                # only handle JSON payloads, not the "event:" or "id:" lines
                if text.startswith("data:"):
                    # remove the "data:" prefix and any leading space
                    json_str = text[len("data:"):].lstrip()
                    batch.append(json_str)

                # once we reach batch_size, upload and reset
                if len(batch) >= batch_size:
                    timestamp = int(time.time())
                    blob_name = f"batch_{timestamp}.json"
                    blob_data = "\n".join(batch).encode("utf-8")
                    
                    print(f"Uploading {blob_name} with {len(batch)} events...")
                    container_client.upload_blob(name=blob_name, data=blob_data, overwrite=True)
                    print("Upload complete.")
                    
                    batch.clear()
                    batches_uploaded += 1
                    if max_batches and batches_uploaded >= max_batches:
                        print("Reached max_batches limit; stopping.")
                        break

    except Exception as e:
        print(f"An error occurred: {e}")
        raise


# COMMAND ----------

stream_and_upload_wikimedia(container_client, batch_size=100, max_batches=10)

# COMMAND ----------

