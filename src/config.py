import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Read and validate required environment variables
gdlet_storage_name = os.getenv("GDLET_CONTAINER_NAME")
wiki_storage_name = os.getenv("WIKI_CONTAINER_NAME")
storage_account_name = os.getenv("STORAGE_ACCOUNT_NAME")
storage_account_key = os.getenv("STORAGE_KEY")

# Ensure all required environment variables are set
required_vars = {
    "GDLET_CONTAINER_NAME": gdlet_storage_name,
    "WIKI_CONTAINER_NAME": wiki_storage_name,
    "STORAGE_ACCOUNT_NAME": storage_account_name,
    "STORAGE_KEY": storage_account_key,
}

missing_vars = [var for var, value in required_vars.items() if value is None]

if missing_vars:
    raise EnvironmentError(
        f"Missing required environment variables: {', '.join(missing_vars)}"
    )



# Define storage paths
GDELT_PATH = f"wasbs://{gdlet_storage_name}@{storage_account_name}.blob.core.windows.net"
WIKIMEDIA_PATH = f"wasbs://{wiki_storage_name}@{storage_account_name}.blob.core.windows.net"