import requests
import zipfile
import io

# Download GDELT data and upload to Blob Storage
def download_and_upload_gdelt_data(gdelt_url, container_client):
    try:
        # Step 1: Download the GDELT data
        print("Attempting to download GDELT data...")
        response = requests.get(gdelt_url)
        if response.status_code != 200:
            raise Exception(f"Failed to download GDELT data. HTTP Status Code: {response.status_code}")

        print(f"{gdelt_url} data downloaded successfully.")


        # Step 2: Extract the ZIP file and process CSV files
        print("Extracting ZIP file and processing CSV files...")

        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            for file_name in z.namelist():
                print(f"Processing file: {file_name}")
                
                if file_name.endswith('.CSV'):
                    csv_content = z.read(file_name)
                    
                    # Step 3: Upload CSV file to Blob Storage
                    print(f"Uploading file '{file_name}' to Blob Storage...")
                    blob_client = container_client.get_blob_client(file_name)
                    
                    try:
                        blob_client.upload_blob(csv_content, overwrite=True)
                        print(f"Successfully uploaded '{file_name}' to Blob Storage.")
                    except Exception as e:
                        raise Exception(f"Failed to upload '{file_name}' to Blob Storage: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
        raise