# bigquery_connect.py
"""
BigQuery connection + idempotent dataset/table setup + detailed streaming errors.
"""

from typing import List, Dict, Any, Tuple
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound
import pandas as pd
import os

# =========================
# EDIT THESE FOR YOUR ENV:
# =========================
PROJECT_ID = "analytics-prod-476204"                  # e.g. "analytics-prod-476204"
#SERVICE_ACCOUNT_FILE = "analytics-prod-476204-9058be20a779.json"    # path to your service account JSON
SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "analytics-prod-476204-9058be20a779.json")
DATASET_ID = "ops_data"
LOCATION = "asia-south1"                             # Mumbai

# ---------- Connection ----------
def get_bq_client() -> bigquery.Client:
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
    return bigquery.Client(project=PROJECT_ID, credentials=creds)

# ---------- Dataset/Table setup (idempotent) ----------
def ensure_dataset(client: bigquery.Client, dataset_id: str = DATASET_ID, location: str = LOCATION) -> None:
    ds_ref = bigquery.Dataset(f"{client.project}.{dataset_id}")
    try:
        client.get_dataset(ds_ref)
        print(f"✅ Dataset already exists: {dataset_id}")
    except NotFound:
        ds_ref.location = location
        client.create_dataset(ds_ref)
        print(f"🎉 Created dataset: {dataset_id} ({location})")

def ensure_tables(client: bigquery.Client, dataset_id: str = DATASET_ID) -> None:
    def exists(table_id: str) -> bool:
        try:
            client.get_table(table_id); return True
        except NotFound:
            return False

    # tenantdata
    tenant_schema = [
        bigquery.SchemaField("tenantid", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("tenantname", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("onboardeddate", "DATE"),
        bigquery.SchemaField("cataloglastupdated", "DATE"),
        bigquery.SchemaField("lasttraineddate", "DATE"),
        bigquery.SchemaField("active", "BOOL"),
    ]
    t1 = f"{PROJECT_ID}.{dataset_id}.tenantdata"
    if not exists(t1):
        client.create_table(bigquery.Table(t1, schema=tenant_schema)); print(f"🎉 Created table: {t1}")
    else:
        print(f"✅ Table exists: {t1}")

    # public_festive_events
    fe_schema = [
        bigquery.SchemaField("tenantid", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("festival_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("start_date", "DATE"),
        bigquery.SchemaField("end_date", "DATE"),
        bigquery.SchemaField("region", "STRING"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("weight", "FLOAT64"),
        bigquery.SchemaField("is_active", "BOOL"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
        bigquery.SchemaField("no_of_days", "INT64"),
    ]
    t2 = f"{PROJECT_ID}.{dataset_id}.public_festive_events"
    if not exists(t2):
        client.create_table(bigquery.Table(t2, schema=fe_schema)); print(f"🎉 Created table: {t2}")
    else:
        print(f"✅ Table exists: {t2}")

    # productcatalog
    product_schema = [
        bigquery.SchemaField("productid", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("tenantid", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("opuraproductid", "STRING"),
        bigquery.SchemaField("productname", "STRING"),
        bigquery.SchemaField("productcategory", "STRING"),
        bigquery.SchemaField("productdescription", "STRING"),
        bigquery.SchemaField("productprice", "FLOAT64"),
        bigquery.SchemaField("productimages", "STRING"),
        bigquery.SchemaField("productreviews", "INT64"),
        bigquery.SchemaField("producttags", "STRING"),
        bigquery.SchemaField("productquantity", "INT64"),
        bigquery.SchemaField("productbrand", "STRING"),
        bigquery.SchemaField("productvariants", "STRING"),
    ]
    t3 = f"{PROJECT_ID}.{dataset_id}.productcatalog"
    if not exists(t3):
        client.create_table(bigquery.Table(t3, schema=product_schema)); print(f"🎉 Created table: {t3}")
    else:
        print(f"✅ Table exists: {t3}")

    # customerdata
    customer_schema = [
        bigquery.SchemaField("customerid", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("opuracustomerid", "STRING"),
        bigquery.SchemaField("tenantid", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("customername", "STRING"),
        bigquery.SchemaField(
            "customerpersonaldetails", "RECORD", fields=[
                bigquery.SchemaField("age", "INT64"),
                bigquery.SchemaField("gender", "STRING"),
            ]
        ),
        bigquery.SchemaField(
            "customergeolocations", "RECORD", fields=[
                bigquery.SchemaField("city", "STRING"),
            ]
        ),
        bigquery.SchemaField("customerwishlist", "STRING", mode="REPEATED"),
        bigquery.SchemaField(
            "purchaseorders", "RECORD", mode="REPEATED", fields=[
                bigquery.SchemaField("productId", "STRING"),
                bigquery.SchemaField("purchaseDate", "DATE"),
                bigquery.SchemaField("opuraProductID", "STRING"),
                bigquery.SchemaField("purchaseQuantity", "INT64"),
                bigquery.SchemaField("mrp", "FLOAT64"),
                bigquery.SchemaField("saleprice", "FLOAT64"),
            ]
        ),
        bigquery.SchemaField(
            "customertags", "RECORD", fields=[
                bigquery.SchemaField("high_spender", "BOOL"),
            ]
        ),
        bigquery.SchemaField("opuracustomertags", "JSON"),
        bigquery.SchemaField("recommendedproducts", "STRING", mode="REPEATED"),
    ]
    t4 = f"{PROJECT_ID}.{dataset_id}.customerdata"
    if not exists(t4):
        client.create_table(bigquery.Table(t4, schema=customer_schema)); print(f"🎉 Created table: {t4}")
    else:
        print(f"✅ Table exists: {t4}")

# ---------- Streaming insert with detailed errors ----------
def insert_rows_streaming_detailed(
    client: bigquery.Client,
    table_id: str,
    rows: List[Dict[str, Any]],
    batch_size: int = 500,
) -> Tuple[int, List[Dict[str, Any]]]:
    """
    Returns (success_count, error_details)
    error_details: list of dicts with index, errors (each with reason, location, message)
    """
    total_ok = 0
    details: List[Dict[str, Any]] = []

    for i in range(0, len(rows), batch_size):
        chunk = rows[i:i + batch_size]
        errors = client.insert_rows_json(
            table_id,
            chunk,
            skip_invalid_rows=True,
            ignore_unknown_values=True,
        )
        if errors:
            # BigQuery returns a list of {'index': <row_index_in_batch>, 'errors': [{reason, location, message}, ...]}
            for e in errors:
                details.append({
                    "absolute_row": i + e.get("index", 0),
                    "errors": e.get("errors", []),
                    "row_preview": chunk[e.get("index", 0)],
                })
        else:
            total_ok += len(chunk)

    return total_ok, details

def format_bq_errors(errors: List[Dict[str, Any]], limit: int = 20) -> str:
    """Pretty-print the first N errors for UI."""
    lines = [f"Found {len(errors)} rows with errors. Showing first {min(limit, len(errors))}:"]
    for j, item in enumerate(errors[:limit], 1):
        lines.append(f"\nRow #{item['absolute_row']}:")
        for err in item["errors"]:
            reason = err.get("reason", "Unknown")
            location = err.get("location", "N/A")
            message = err.get("message", "")
            lines.append(f"  - {reason} @ {location}: {message}")
        # Optional tiny preview
        preview = item.get("row_preview", {})
        lines.append(f"  Row preview: {str(preview)[:300]}")
    return "\n".join(lines)

def fetch_tenants_df(client: bigquery.Client) -> pd.DataFrame:
    """
    Fetch tenants from BigQuery as a DataFrame {tenantid, tenantname}
    Used for displaying tenant names and filtering throughout the application.
    """
    try:
        query = f"""
        SELECT tenantid, tenantname
        FROM `{PROJECT_ID}.{DATASET_ID}.tenantdata`
        ORDER BY tenantname
        """
        return client.query(query).result().to_dataframe()
    except Exception as e:
        print(f"⚠ Warning: Failed to fetch tenants: {e}")
        return pd.DataFrame()

def get_next_tenant_id(client: bigquery.Client) -> int:
    """
    Get the next auto-incremented tenantid (max + 1).
    If table is empty, start with 1.
    """
    try:
        query = f"""
        SELECT IFNULL(MAX(tenantid), 0) AS max_id
        FROM `{PROJECT_ID}.{DATASET_ID}.tenantdata`
        """
        df = client.query(query).result().to_dataframe()
        max_id = int(df['max_id'].iloc[0])
        return max_id + 1
    except Exception as e:
        print(f"⚠ Warning: Could not compute next tenantid: {e}")
        return 1