import os
import io
import requests
import zipfile
from datetime import datetime, timedelta

def download_and_upload_gdelt_data_range(start_time_str, end_time_str, container_client):
    """
    Downloads and uploads GDELT data over a time range to Azure Blob Storage.

    Args:
        start_time_str (str): Start time in format 'YYYYMMDDHHMMSS'
        end_time_str (str): End time in format 'YYYYMMDDHHMMSS'
        container_client (ContainerClient): Azure Blob Storage container client
    """
    start_time = datetime.strptime(start_time_str, "%Y%m%d%H%M%S")
    end_time = datetime.strptime(end_time_str, "%Y%m%d%H%M%S")
    current_time = start_time
    interval = timedelta(minutes=15)

    while current_time <= end_time:
        timestamp = current_time.strftime("%Y%m%d%H%M%S")
        gdelt_url = f"http://data.gdeltproject.org/gdeltv2/{timestamp}.export.CSV.zip"
        # http://data.gdeltproject.org/gdeltv2/20250714061500.export.CSV.zip

        try:
            print(f"\nDownloading: {gdelt_url}")
            response = requests.get(gdelt_url, timeout=10)
            if response.status_code != 200:
                print(f"Failed to download GDELT data. HTTP Status Code: {response.status_code}")
                current_time += interval
                continue

            print(f"{gdelt_url} data downloaded successfully.")

            # Extract the ZIP file and process CSV files
            print("Extracting ZIP file and processing CSV files...")
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                for file_name in z.namelist():
                    print(f"Processing file: {file_name}")
                    if file_name.endswith('.CSV'):
                        csv_content = z.read(file_name)

                        # Upload to Azure Blob Storage
                        print(f"Uploading file '{file_name}' to Blob Storage...")
                        blob_client = container_client.get_blob_client(file_name)
                        try:
                            blob_client.upload_blob(csv_content, overwrite=True)
                            print(f"Successfully uploaded '{file_name}' to Blob Storage.")
                        except Exception as upload_err:
                            print(f"Failed to upload '{file_name}' to Blob Storage: {upload_err}")
        except Exception as e:
            print(f"Error occurred for {gdelt_url}: {e}")

        current_time += interval
