# create_dataset.py

from google.cloud import bigquery
from google.oauth2 import service_account

# ================================================
# CONFIGURATION
# ================================================
SERVICE_ACCOUNT_FILE = "analytics-prod-476204-9058be20a779.json"
PROJECT_ID = "analytics-prod-476204"  # Replace if different
DATASET_NAME = "my_test_dataset"  # You can rename this


# ================================================
# CONNECT TO BIGQUERY
# ================================================
def get_bigquery_client():
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE
    )
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    print("✅ Connected to BigQuery")
    return client


# ================================================
# CREATE DATASET FUNCTION
# ================================================
def create_dataset(dataset_name):
    client = get_bigquery_client()
    dataset_id = f"{PROJECT_ID}.{dataset_name}"

    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"  # You can choose: US, EU, asia-south1, etc.

    try:
        dataset = client.create_dataset(dataset, exists_ok=True)
        print(f"🎉 Dataset created successfully: {dataset.project}.{dataset.dataset_id}")
    except Exception as e:
        print(f"❌ Failed to create dataset: {e}")


# ================================================
# MAIN EXECUTION
# ================================================
if __name__ == "__main__":
    create_dataset(DATASET_NAME)