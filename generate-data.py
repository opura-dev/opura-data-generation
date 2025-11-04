# ============================================================
# Multi-tenant Data Generation + Insights — BigQuery Edition
# (Single-file app with helpful comments)
# ============================================================

# -----------------------------
# Imports
# -----------------------------
import os
import json
import uuid
import random
from typing import Optional, Dict
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px
from faker import Faker

from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# ============================================================
# CONFIGURATION — BigQuery Auth, Project, Dataset, Region
# ============================================================

# 1) Service account JSON is in the same directory
SERVICE_ACCOUNT_FILE = "analytics-prod-476204-9058be20a779.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_FILE

# 2) BigQuery project/dataset/region
PROJECT_ID = "analytics-prod"
DATASET_ID = "opura_data"
LOCATION = "asia-south1"  # closest India region

# 3) Faker instance (used for synthetic data)
faker = Faker()

# 4) Streamlit page setup
st.set_page_config(page_title="Multi-tenant Data Generation + Insights (BigQuery)", layout="wide")
st.title("🏗️ Multi-tenant Data Generation + Insights — BigQuery Edition")

# ============================================================
# BIGQUERY CLIENT + ONE-TIME DATASET/TABLE CREATION
# ============================================================

@st.cache_resource(show_spinner=False)
def get_bq_client() -> bigquery.Client:
    """Create a cached BigQuery client (persists across reruns)."""
    return bigquery.Client(project=PROJECT_ID, location=LOCATION)

client = get_bq_client()

def ensure_dataset() -> None:
    """
    Ensure the dataset exists; if not, create it.
    Shows a Streamlit message indicating the action taken.
    """
    dataset_id = f"{PROJECT_ID}.{DATASET_ID}"
    try:
        client.get_dataset(dataset_id)
        st.info(f"✅ Using existing dataset: {DATASET_ID}")
    except NotFound:
        dataset_ref = bigquery.Dataset(dataset_id)
        dataset_ref.location = LOCATION
        client.create_dataset(dataset_ref)
        st.success(f"🆕 Created new dataset: {DATASET_ID}")

def get_table_schemas() -> Dict[str, list]:
    """
    Return strict table schemas mirroring your PostgreSQL structure,
    using BigQuery RECORD/ARRAY for JSONB-like fields.
    """
    # tenantdata
    tenant_schema = [
        bigquery.SchemaField("tenantid", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("tenantname", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("onboardeddate", "DATE"),
        bigquery.SchemaField("cataloglastupdated", "DATE"),
        bigquery.SchemaField("lasttraineddate", "DATE"),
        bigquery.SchemaField("active", "BOOL"),
    ]

    # public_festive_events
    fest_schema = [
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

    # productcatalog — arrays + nested struct for variants
    product_schema = [
        bigquery.SchemaField("productid", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("tenantid", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("opuraproductid", "STRING"),
        bigquery.SchemaField("productname", "STRING"),
        bigquery.SchemaField("productcategory", "STRING"),
        bigquery.SchemaField("productdescription", "STRING"),
        bigquery.SchemaField("productprice", "FLOAT64"),
        bigquery.SchemaField("productimages", "STRING", mode="REPEATED"),
        bigquery.SchemaField("productreviews", "INT64"),
        bigquery.SchemaField("producttags", "STRING", mode="REPEATED"),
        bigquery.SchemaField("productquantity", "INT64"),
        bigquery.SchemaField("productbrand", "STRING"),
        bigquery.SchemaField("productvariants", "RECORD", fields=[
            bigquery.SchemaField("color", "STRING"),
        ]),
    ]

    # customerdata — nested purchaseorders[] and other RECORD fields
    purchase_struct = bigquery.SchemaField("purchaseorders", "RECORD", mode="REPEATED", fields=[
        bigquery.SchemaField("productId", "STRING"),
        bigquery.SchemaField("purchaseDate", "DATE"),
        bigquery.SchemaField("opuraProductID", "STRING"),
        bigquery.SchemaField("purchaseQuantity", "INT64"),
        bigquery.SchemaField("mrp", "FLOAT64"),
        bigquery.SchemaField("saleprice", "FLOAT64"),
    ])

    customer_schema = [
        bigquery.SchemaField("customerid", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("opuracustomerid", "STRING"),
        bigquery.SchemaField("tenantid", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("customername", "STRING"),
        bigquery.SchemaField("customerpersonaldetails", "RECORD", fields=[
            bigquery.SchemaField("age", "INT64"),
            bigquery.SchemaField("gender", "STRING"),
        ]),
        bigquery.SchemaField("customergeolocations", "RECORD", fields=[
            bigquery.SchemaField("city", "STRING"),
        ]),
        bigquery.SchemaField("customerwishlist", "STRING", mode="REPEATED"),
        purchase_struct,
        bigquery.SchemaField("customertags", "RECORD", fields=[
            bigquery.SchemaField("high_spender", "BOOL"),
        ]),
        bigquery.SchemaField("opuracustomertags", "RECORD", fields=[]),
        bigquery.SchemaField("recommendedproducts", "STRING", mode="REPEATED"),
    ]

    return {
        "tenantdata": tenant_schema,
        "public_festive_events": fest_schema,
        "productcatalog": product_schema,
        "customerdata": customer_schema,
    }

def ensure_tables() -> None:
    """
    Ensure each required table exists; if not, create it.
    Shows Streamlit messages indicating existing/created states.
    """
    schemas = get_table_schemas()
    for table_name, schema in schemas.items():
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
        try:
            client.get_table(table_id)
            st.info(f"✅ Using existing table: {table_name}")
        except NotFound:
            table = bigquery.Table(table_id, schema=schema)
            client.create_table(table)
            st.success(f"🆕 Created new table: {table_name}")

# Ensure dataset/tables exist (safe to call on each run; it will skip if present)
ensure_dataset()
ensure_tables()

# ============================================================
# HELPER FUNCTIONS — Query wrapper, inserts, and fetchers
# ============================================================

def run_query_df(sql: str, params: Optional[Dict] = None) -> pd.DataFrame:
    """
    Run a parameterized SQL query and return the results as a DataFrame.
    BigQuery typed parameters are inferred for common Python types.
    """
    # Build typed parameters
    bq_params = []
    if params:
        for name, value in params.items():
            # Explicit DATE handling: pass date strings ('YYYY-MM-DD') or datetime.date objects
            if isinstance(value, (pd.Timestamp,)):
                # Convert to date string if a Timestamp
                value = value.date().isoformat()
                bq_params.append(bigquery.ScalarQueryParameter(name, "DATE", value))
            elif isinstance(value, (datetime,)):
                # Convert to date string for DATE parameters if needed
                value = value.date().isoformat()
                bq_params.append(bigquery.ScalarQueryParameter(name, "DATE", value))
            elif isinstance(value, (str,)):
                # Heuristic: YYYY-MM-DD → DATE; else STRING
                if len(value) == 10 and value[4] == "-" and value[7] == "-":
                    bq_params.append(bigquery.ScalarQueryParameter(name, "DATE", value))
                else:
                    bq_params.append(bigquery.ScalarQueryParameter(name, "STRING", value))
            elif isinstance(value, bool):
                bq_params.append(bigquery.ScalarQueryParameter(name, "BOOL", value))
            elif isinstance(value, int):
                bq_params.append(bigquery.ScalarQueryParameter(name, "INT64", value))
            elif isinstance(value, float):
                bq_params.append(bigquery.ScalarQueryParameter(name, "FLOAT64", value))
            else:
                # Fallback: stringify
                bq_params.append(bigquery.ScalarQueryParameter(name, "STRING", json.dumps(value)))

    job_config = bigquery.QueryJobConfig(query_parameters=bq_params)
    query_job = client.query(sql, job_config=job_config)
    return query_job.result().to_dataframe(create_bqstorage_client=False)

def insert_rows_dataframe(df: pd.DataFrame, table_id: str) -> None:
    """
    Append a DataFrame to an existing BigQuery table.
    Supports arrays (list) and records (dict) in object dtype columns.
    """
    job = client.load_table_from_dataframe(
        df, table_id,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    )
    job.result()

# Table IDs for convenience
TABLE_TENANT   = f"{PROJECT_ID}.{DATASET_ID}.tenantdata"
TABLE_FEST     = f"{PROJECT_ID}.{DATASET_ID}.public_festive_events"
TABLE_PRODUCT  = f"{PROJECT_ID}.{DATASET_ID}.productcatalog"
TABLE_CUSTOMER = f"{PROJECT_ID}.{DATASET_ID}.customerdata"

# ============================================================
# FETCHERS — BigQuery equivalents of your original functions
# ============================================================

def fetch_tenants() -> pd.DataFrame:
    sql = f"SELECT tenantid, tenantname FROM `{TABLE_TENANT}` ORDER BY tenantid"
    try:
        return run_query_df(sql)
    except NotFound:
        return pd.DataFrame(columns=["tenantid", "tenantname"])

def fetch_festive_events(tenant_id: int) -> pd.DataFrame:
    sql = f"""
    SELECT festival_name AS event_name, start_date, end_date, category
    FROM `{TABLE_FEST}`
    WHERE tenantid = @tid
    ORDER BY start_date
    """
    return run_query_df(sql, {"tid": tenant_id})

def fetch_festivals(tenant_id: int) -> pd.DataFrame:
    sql = f"""
    SELECT festival_name, start_date, end_date, category, weight, no_of_days, is_active, region
    FROM `{TABLE_FEST}`
    WHERE tenantid = @tid AND is_active = TRUE
    ORDER BY start_date
    """
    return run_query_df(sql, {"tid": tenant_id})

def next_tenant_id() -> int:
    sql = f"SELECT IFNULL(MAX(tenantid), 0) AS max_id FROM `{TABLE_TENANT}`"
    df = run_query_df(sql)
    return int(df.iloc[0]["max_id"]) + 1 if not df.empty else 1

# ============================================================
# UI — TABS
# ============================================================

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🏢 Tenant Creation",
    "📊 Data Summary",
    "📂 Category Drilldown",
    "📈 Trends & Insights",
    "🧩 Bulk Data Gen"
])

# ============================================================
# TAB 1: TENANT CREATION + FESTIVAL MANAGEMENT
# ============================================================
with tab1:
    st.header("🏢 Tenant & Festival Management")

    tenants = fetch_tenants()
    next_id = next_tenant_id()
    st.info(f"Next Tenant ID will be: {next_id}")

    tenant_name = st.text_input("Tenant Name", placeholder="Enter tenant/company name")

    # --- Create Tenant (and clone festivals from Tenant 1) ---
    if st.button("Create Tenant"):
        # Compute some example dates (like original)
        onboard_date = (datetime.now(timezone.utc).date() - timedelta(days=random.randint(100, 1000))).isoformat()
        catalog_date = (datetime.fromisoformat(onboard_date) + timedelta(days=30)).date().isoformat()
        trained_date = (datetime.fromisoformat(onboard_date) + timedelta(days=60)).date().isoformat()

        # Insert tenant row
        insert_sql = f"""
        INSERT `{TABLE_TENANT}` (tenantid, tenantname, onboardeddate, cataloglastupdated, lasttraineddate, active)
        VALUES (@id, @name, @onboard, @catalog, @trained, TRUE)
        """
        client.query(insert_sql, job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("id", "INT64", next_id),
                bigquery.ScalarQueryParameter("name", "STRING", tenant_name),
                bigquery.ScalarQueryParameter("onboard", "DATE", onboard_date),
                bigquery.ScalarQueryParameter("catalog", "DATE", catalog_date),
                bigquery.ScalarQueryParameter("trained", "DATE", trained_date),
            ]
        )).result()

        # Clone default festivals from tenant 1
        copy_sql = f"""
        INSERT `{TABLE_FEST}` (tenantid, festival_name, start_date, end_date, region, category, weight,
                               is_active, created_at, updated_at, no_of_days)
        SELECT @new_tid, festival_name, start_date, end_date, region, category, weight,
               is_active, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), no_of_days
        FROM `{TABLE_FEST}`
        WHERE tenantid = 1
        """
        client.query(copy_sql, job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("new_tid", "INT64", next_id)]
        )).result()

        st.success(f"✅ Tenant '{tenant_name}' added and default festivals cloned from Tenant 1.")

    st.divider()
    st.subheader("🎉 Add, View, Modify, or Delete Festive Events")

    tenants = fetch_tenants()
    if tenants.empty:
        st.info("No tenants yet.")
        st.stop()

    tenant_choice = st.selectbox("Select Tenant", tenants["tenantname"].tolist(), key="tenant_festival_select")
    tenant_id = int(tenants.loc[tenants["tenantname"] == tenant_choice, "tenantid"].values[0])

    # Fetch existing festivals (active)
    try:
        events_df = fetch_festivals(tenant_id)
    except Exception as e:
        st.error(f"❌ Failed to fetch festivals: {e}")
        events_df = pd.DataFrame()

    # Ensure expected columns
    required_cols = ["festival_name", "start_date", "end_date", "category", "weight", "no_of_days", "region", "is_active"]
    for col in required_cols:
        if col not in events_df.columns:
            events_df[col] = None

    # Normalize date columns for editor
    for dc in ["start_date", "end_date"]:
        events_df[dc] = pd.to_datetime(events_df[dc], errors="coerce").dt.date

    # --- Restore defaults from Tenant 1 ---
    with st.expander("♻️ Restore Default Festivals from Tenant 1"):
        st.info("This will replace all existing festivals for this tenant with the defaults from Tenant 1.")
        if st.button("Restore from Default Tenant 1"):
            client.query(
                f"DELETE FROM `{TABLE_FEST}` WHERE tenantid = @tid",
                job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("tid", "INT64", tenant_id)])
            ).result()

            client.query(
                f"""
                INSERT `{TABLE_FEST}` (tenantid, festival_name, start_date, end_date, region, category, weight,
                                       is_active, created_at, updated_at, no_of_days)
                SELECT @new_tid, festival_name, start_date, end_date, region, category, weight,
                       is_active, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), no_of_days
                FROM `{TABLE_FEST}` WHERE tenantid = 1
                """,
                job_config=bigquery.QueryJobConfig(query_parameters=[
                    bigquery.ScalarQueryParameter("new_tid", "INT64", tenant_id)
                ])
            ).result()

            st.success(f"✅ Restored default festivals from Tenant 1 for {tenant_choice}.")
            events_df = fetch_festivals(tenant_id)

    # --- Delete a festival by name ---
    if not events_df.empty:
        st.write("🗑️ Select and delete any individual festival below:")
        delete_options = events_df["festival_name"].tolist()
        festival_to_delete = st.selectbox("Select Festival to Delete", delete_options, key="delete_festival")
        if st.button("Delete Selected Festival"):
            client.query(
                f"DELETE FROM `{TABLE_FEST}` WHERE tenantid=@tid AND festival_name=@fname",
                job_config=bigquery.QueryJobConfig(query_parameters=[
                    bigquery.ScalarQueryParameter("tid","INT64",tenant_id),
                    bigquery.ScalarQueryParameter("fname","STRING",festival_to_delete),
                ])
            ).result()
            st.success(f"🗑️ Festival '{festival_to_delete}' deleted successfully.")
            events_df = fetch_festivals(tenant_id)

    # --- Edit existing festivals (DELETE + INSERT per row as a simple upsert) ---
    if not events_df.empty:
        st.write("✏️ Modify any festival details below, then click **Update Festivals**:")
        edited_df = st.data_editor(events_df, num_rows="dynamic", use_container_width=True, key="festival_editor")

        if st.button("Update Festivals"):
            for _, row in edited_df.iterrows():
                fname = row["festival_name"]

                # Delete old record
                client.query(
                    f"DELETE FROM `{TABLE_FEST}` WHERE tenantid=@tid AND festival_name=@fname",
                    job_config=bigquery.QueryJobConfig(query_parameters=[
                        bigquery.ScalarQueryParameter("tid","INT64",tenant_id),
                        bigquery.ScalarQueryParameter("fname","STRING",fname),
                    ])
                ).result()

                # Insert updated record
                sdate = row["start_date"].isoformat() if pd.notna(row["start_date"]) else None
                edate = row["end_date"].isoformat() if pd.notna(row["end_date"]) else None
                region = row.get("region") or "All India"
                cat = row.get("category") or None
                weight = float(row.get("weight") or 0.0)
                days = int(row["no_of_days"]) if not pd.isna(row["no_of_days"]) else None
                is_active = bool(row.get("is_active") if row.get("is_active") is not None else True)

                client.query(
                    f"""
                    INSERT `{TABLE_FEST}` (tenantid, festival_name, start_date, end_date, region, category, weight,
                                           is_active, created_at, updated_at, no_of_days)
                    VALUES (@tid, @fname, @sdate, @edate, @region, @cat, @weight,
                            @is_active, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), @days)
                    """,
                    job_config=bigquery.QueryJobConfig(query_parameters=[
                        bigquery.ScalarQueryParameter("tid","INT64",tenant_id),
                        bigquery.ScalarQueryParameter("fname","STRING",fname),
                        bigquery.ScalarQueryParameter("sdate","DATE",sdate),
                        bigquery.ScalarQueryParameter("edate","DATE",edate),
                        bigquery.ScalarQueryParameter("region","STRING",region),
                        bigquery.ScalarQueryParameter("cat","STRING",cat),
                        bigquery.ScalarQueryParameter("weight","FLOAT64",weight),
                        bigquery.ScalarQueryParameter("is_active","BOOL",is_active),
                        bigquery.ScalarQueryParameter("days","INT64",days),
                    ])
                ).result()

            st.success("✅ Festivals updated successfully!")
            refreshed_df = fetch_festivals(tenant_id)
            for c in required_cols:
                if c not in refreshed_df.columns:
                    refreshed_df[c] = None
            for dc in ["start_date","end_date"]:
                refreshed_df[dc] = pd.to_datetime(refreshed_df[dc], errors="coerce").dt.date
            st.dataframe(refreshed_df, use_container_width=True)
    else:
        st.info("ℹ️ No festivals found for this tenant yet.")

    st.divider()
    st.subheader("➕ Add New Festival")

    # --- Add a new festival ---
    with st.expander("Add a New Festival for this Tenant"):
        festival_name = st.text_input("Festival Name", placeholder="Enter new festival name")
        start_date = st.date_input("Start Date", datetime.now(timezone.utc).date())
        end_date = st.date_input("End Date (Optional)", datetime.now(timezone.utc).date() + timedelta(days=7))
        region = st.text_input("Region", "All India")
        category = st.selectbox("Category", ["Sale", "Holiday", "Offer", "Cultural", "Seasonal"])
        weight = st.slider("Festival Impact Weight", 0.1, 2.0, 1.0, step=0.1)

        if st.button("Add Festival", key="add_new_fest"):
            no_of_days = (end_date - start_date).days if end_date else None
            client.query(
                f"""
                INSERT `{TABLE_FEST}`
                (tenantid, festival_name, start_date, end_date, region, category, weight,
                 is_active, created_at, updated_at, no_of_days)
                VALUES (@tid, @fname, @sdate, @edate, @region, @cat, @weight, TRUE,
                        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), @days)
                """,
                job_config=bigquery.QueryJobConfig(query_parameters=[
                    bigquery.ScalarQueryParameter("tid","INT64",tenant_id),
                    bigquery.ScalarQueryParameter("fname","STRING",festival_name),
                    bigquery.ScalarQueryParameter("sdate","DATE",start_date.isoformat()),
                    bigquery.ScalarQueryParameter("edate","DATE",end_date.isoformat() if end_date else None),
                    bigquery.ScalarQueryParameter("region","STRING",region),
                    bigquery.ScalarQueryParameter("cat","STRING",category),
                    bigquery.ScalarQueryParameter("weight","FLOAT64",float(weight)),
                    bigquery.ScalarQueryParameter("days","INT64",no_of_days),
                ])
            ).result()
            st.success(f"🎉 Festival '{festival_name}' added successfully for Tenant '{tenant_choice}'.")

# ============================================================
# TAB 2: DATA SUMMARY (JSON-aware using UNNEST)
# ============================================================
with tab2:
    st.header("📊 Tenant Data Summary")

    tenants = fetch_tenants()
    if tenants.empty:
        st.warning("No tenants.")
    else:
        tenant_choice = st.selectbox("Select Tenant", tenants["tenantname"].tolist(), key="tab2_tenant")
        tenant_id = int(tenants.loc[tenants["tenantname"] == tenant_choice, "tenantid"].values[0])

        # Flatten purchaseorders to compute metrics
        sql = f"""
        WITH flat AS (
          SELECT
            c.customerid,
            p.productid,
            po.purchaseQuantity AS qty,
            po.saleprice AS saleprice,
            po.mrp AS mrp
          FROM `{TABLE_CUSTOMER}` c,
          UNNEST(c.purchaseorders) po
          JOIN `{TABLE_PRODUCT}` p
          ON p.productid = po.productId
          WHERE c.tenantid = @tid
        )
        SELECT
          COUNT(DISTINCT customerid) AS total_customers,
          COUNT(DISTINCT productid)  AS total_products,
          COUNT(1)                   AS total_orders,
          SUM(saleprice * qty)       AS total_revenue,
          AVG(IF(mrp > 0, (1 - saleprice / mrp) * 100, 0)) AS avg_discount
        FROM flat
        """
        df_summary = run_query_df(sql, {"tid": tenant_id})

        st.subheader("📈 Key Business Metrics")
        if not df_summary.empty:
            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("👥 Customers", int(df_summary["total_customers"][0] or 0))
            c2.metric("📦 Products", int(df_summary["total_products"][0] or 0))
            c3.metric("🛒 Orders", int(df_summary["total_orders"][0] or 0))
            c4.metric("💰 Revenue", f"${(df_summary['total_revenue'][0] or 0):,.2f}")
            c5.metric("🏷️ Avg Discount", f"{(df_summary['avg_discount'][0] or 0):.2f}%")
        else:
            st.warning("No data available for this tenant.")

# ============================================================
# TAB 3: CATEGORY DRILLDOWN — Global + Per Tenant
# ============================================================
with tab3:
    st.header("📂 Category Drilldown & Product Insights")

    tenants = fetch_tenants()
    tenant_options = ["All Tenants"] + tenants["tenantname"].tolist()
    tenant_choice = st.selectbox("Select Tenant", tenant_options, key="tab3_tenant")

    today = datetime.now(timezone.utc).date()
    start_date = st.date_input("Start Date", today - timedelta(days=365))
    end_date = st.date_input("End Date", today)

    if tenant_choice == "All Tenants":
        sql = f"""
        WITH flat AS (
          SELECT
            c.tenantid,
            SAFE_CAST(po.purchaseDate AS DATE) AS purchase_date,
            po.purchaseQuantity AS purchase_quantity,
            p.productprice AS base_price,
            po.saleprice AS saleprice,
            COALESCE(p.productcategory, 'Unknown') AS category,
            p.productname AS product_name
          FROM `{TABLE_CUSTOMER}` c,
          UNNEST(c.purchaseorders) po
          JOIN `{TABLE_PRODUCT}` p ON p.productid = po.productId
          WHERE SAFE_CAST(po.purchaseDate AS DATE) BETWEEN @start AND @end
        )
        SELECT * FROM flat
        """
        params = {"start": start_date.isoformat(), "end": end_date.isoformat()}
    else:
        tenant_id = int(tenants.loc[tenants["tenantname"] == tenant_choice, "tenantid"].values[0])
        sql = f"""
        WITH flat AS (
          SELECT
            c.tenantid,
            SAFE_CAST(po.purchaseDate AS DATE) AS purchase_date,
            po.purchaseQuantity AS purchase_quantity,
            p.productprice AS base_price,
            po.saleprice AS saleprice,
            COALESCE(p.productcategory, 'Unknown') AS category,
            p.productname AS product_name
          FROM `{TABLE_CUSTOMER}` c,
          UNNEST(c.purchaseorders) po
          JOIN `{TABLE_PRODUCT}` p ON p.productid = po.productId
          WHERE c.tenantid = @tid
            AND SAFE_CAST(po.purchaseDate AS DATE) BETWEEN @start AND @end
        )
        SELECT * FROM flat ORDER BY purchase_date
        """
        params = {"tid": tenant_id, "start": start_date.isoformat(), "end": end_date.isoformat()}

    df = run_query_df(sql, params)
    if df.empty:
        st.warning("No purchase data found.")
    else:
        df["purchase_date"] = pd.to_datetime(df["purchase_date"], errors="coerce")
        df["revenue"] = df["saleprice"] * df["purchase_quantity"]
        df["discount_pct"] = np.where(df["base_price"] > 0, (1 - df["saleprice"] / df["base_price"]) * 100, 0)

        if tenant_choice == "All Tenants":
            st.subheader("🏷️ Global Category Summary Across Tenants")
            cat_summary = (
                df.groupby(["tenantid", "category"], as_index=False)
                  .agg(total_orders=("purchase_quantity","sum"),
                       total_revenue=("revenue","sum"),
                       avg_discount=("discount_pct","mean"))
            )
            st.dataframe(cat_summary, use_container_width=True)
            fig = px.bar(cat_summary, x="category", y="total_revenue", color="tenantid",
                         title="Revenue by Category (All Tenants)", text_auto=".2s")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.subheader(f"🏷️ Category Summary for {tenant_choice}")
            cat_summary = (
                df.groupby("category", as_index=False)
                  .agg(total_orders=("purchase_quantity","sum"),
                       total_revenue=("revenue","sum"),
                       avg_discount=("discount_pct","mean"))
                  .sort_values("total_revenue", ascending=False)
            )
            st.dataframe(cat_summary, use_container_width=True)
            st.plotly_chart(
                px.bar(cat_summary, x="category", y="total_revenue",
                       text_auto=".2s", title=f"Revenue by Category — {tenant_choice}"),
                use_container_width=True
            )

# ============================================================
# TAB 4: TRENDS & INSIGHTS — Global + Per Tenant
# ============================================================
with tab4:
    st.header("📈 Temporal Trends & Seasonal Insights")

    tenants = fetch_tenants()
    tenant_options = ["All Tenants"] + tenants["tenantname"].tolist()
    tenant_choice = st.selectbox("Select Tenant", tenant_options, key="tab4_tenant")

    today = datetime.now(timezone.utc).date()
    start_date = st.date_input("Start Date", today - timedelta(days=365), key="trend_start")
    end_date = st.date_input("End Date", today, key="trend_end")

    if tenant_choice == "All Tenants":
        sql = f"""
        WITH flat AS (
          SELECT
            c.tenantid,
            SAFE_CAST(po.purchaseDate AS DATE) AS purchase_date,
            po.purchaseQuantity AS purchase_quantity,
            p.productprice AS base_price,
            po.saleprice AS saleprice,
            COALESCE(p.productcategory, 'Unknown') AS category,
            p.productname AS product_name
          FROM `{TABLE_CUSTOMER}` c,
          UNNEST(c.purchaseorders) po
          JOIN `{TABLE_PRODUCT}` p ON p.productid = po.productId
          WHERE SAFE_CAST(po.purchaseDate AS DATE) BETWEEN @start AND @end
        )
        SELECT * FROM flat ORDER BY purchase_date
        """
        params = {"start": start_date.isoformat(), "end": end_date.isoformat()}
    else:
        tenant_id = int(tenants.loc[tenants["tenantname"] == tenant_choice, "tenantid"].values[0])
        sql = f"""
        WITH flat AS (
          SELECT
            c.tenantid,
            SAFE_CAST(po.purchaseDate AS DATE) AS purchase_date,
            po.purchaseQuantity AS purchase_quantity,
            p.productprice AS base_price,
            po.saleprice AS saleprice,
            COALESCE(p.productcategory, 'Unknown') AS category,
            p.productname AS product_name
          FROM `{TABLE_CUSTOMER}` c,
          UNNEST(c.purchaseorders) po
          JOIN `{TABLE_PRODUCT}` p ON p.productid = po.productId
          WHERE c.tenantid = @tid
            AND SAFE_CAST(po.purchaseDate AS DATE) BETWEEN @start AND @end
        )
        SELECT * FROM flat ORDER BY purchase_date
        """
        params = {"tid": tenant_id, "start": start_date.isoformat(), "end": end_date.isoformat()}

    df = run_query_df(sql, params)
    if df.empty:
        st.warning("No trend data available.")
    else:
        df["purchase_date"] = pd.to_datetime(df["purchase_date"], errors="coerce")
        df["revenue"] = df["saleprice"] * df["purchase_quantity"]

        if tenant_choice == "All Tenants":
            st.subheader("📊 Combined Trends Across All Tenants")
            monthly = (df.groupby([df["purchase_date"].dt.to_period("M"), "tenantid"])
                         .agg(revenue=("revenue","sum")).reset_index())
            monthly["purchase_date"] = monthly["purchase_date"].dt.to_timestamp()
            st.plotly_chart(
                px.line(monthly, x="purchase_date", y="revenue", color="tenantid",
                        markers=True, title="Monthly Revenue Trend by Tenant"),
                use_container_width=True
            )

            st.subheader("🏷️ Category Revenue Split (All Tenants)")
            cat_df = (df.groupby(["tenantid","category"], as_index=False)
                        .agg(total_revenue=("revenue","sum")))
            st.plotly_chart(
                px.sunburst(cat_df, path=["tenantid","category"], values="total_revenue",
                            title="Revenue Breakdown by Tenant and Category"),
                use_container_width=True
            )
        else:
            st.subheader(f"📈 Monthly Trends for {tenant_choice}")
            monthly = (df.groupby(df["purchase_date"].dt.to_period("M"))
                         .agg(revenue=("revenue","sum")).reset_index())
            monthly["purchase_date"] = monthly["purchase_date"].dt.to_timestamp()
            st.plotly_chart(
                px.line(monthly, x="purchase_date", y="revenue",
                        markers=True, title=f"Monthly Revenue Trend — {tenant_choice}"),
                use_container_width=True
            )

            st.subheader("🏷️ Category Revenue Split")
            cat_df = (df.groupby("category", as_index=False)
                        .agg(total_revenue=("revenue","sum"))
                        .sort_values("total_revenue", ascending=False))
            st.plotly_chart(px.pie(cat_df, values="total_revenue", names="category"),
                            use_container_width=True)

# ============================================================
# TAB 5: PRODUCT + CUSTOMER GENERATOR
# ============================================================
with tab5:
    st.header("🧪 Product Catalog → Customer Generator")

    tenant_df = fetch_tenants()
    if tenant_df.empty:
        st.warning("No tenants found. Create a tenant first in Tab 1.")
        st.stop()

    # ---------- Product generation UI ----------
    st.subheader("Step 1 — Create Product Catalog")
    selected_tenant = st.selectbox("Select tenant to add products for", tenant_df["tenantname"].tolist(), key="tab5_tenant")
    prod_tenant_id = int(tenant_df.loc[tenant_df["tenantname"] == selected_tenant, "tenantid"].values[0])

    num_products = st.number_input("Number of products to create", min_value=1, max_value=5000, value=200)
    st.markdown("Define category distribution below (must total 100%). You may use Auto-distribute to evenly split.")

    default_categories = ["Electronics", "Grocery", "Apparel", "Books", "Furniture", "Beauty"]

    # Use existing categories if present
    use_catalog_categories = st.checkbox("Use existing catalog categories for tenant (if any)", value=False)
    if use_catalog_categories:
        sql = f"SELECT DISTINCT productcategory FROM `{TABLE_PRODUCT}` WHERE tenantid=@tid"
        res = run_query_df(sql, {"tid": prod_tenant_id})
        categories = res["productcategory"].dropna().tolist() or default_categories
    else:
        categories = st.multiselect("Categories list (pick or type new)", default_categories, default=default_categories)

    if not categories:
        st.error("Please specify at least one category.")
        st.stop()

    cols = st.columns(4)
    category_weights = {}
    auto_dist = st.checkbox("Auto-distribute categories evenly", value=True)
    if auto_dist:
        even_val = int(100 / len(categories))
        remainder = 100 - even_val * len(categories)
        for i, cat in enumerate(categories):
            w = even_val + (1 if i < remainder else 0)
            category_weights[cat] = w
    else:
        for i, cat in enumerate(categories):
            col = cols[i % len(cols)]
            category_weights[cat] = col.slider(f"{cat} %", 0, 100, int(100 / len(categories)), key=f"cat_{cat}")

    total_pct = sum(category_weights.values())
    st.markdown(f"**Total distribution = {total_pct}%**")
    create_products_btn = st.button("➕ Create Products", disabled=(total_pct != 100))

    if create_products_btn:
        # Compute counts per category that sum to num_products
        cat_names = list(category_weights.keys())
        pct_list = [category_weights[c] for c in cat_names]
        proportions = [p / 100.0 for p in pct_list]
        counts = [int(round(p * num_products)) for p in proportions]
        diff = num_products - sum(counts)
        i = 0
        while diff != 0:
            counts[i % len(counts)] += 1 if diff > 0 else -1
            diff = num_products - sum(counts)
            i += 1

        # Generate product rows with arrays and a nested variant
        products_to_insert = []
        price_map = {
            "Electronics": (500, 5000),
            "Grocery": (10, 300),
            "Apparel": (100, 1500),
            "Books": (50, 500),
            "Furniture": (1500, 25000),
            "Beauty": (50, 800),
        }
        for cat, cnt in zip(cat_names, counts):
            pr_lo, pr_hi = price_map.get(cat, (50, 2000))
            for _ in range(max(0, cnt)):
                pid = str(uuid.uuid4())
                productname = faker.word().capitalize() + " " + random.choice(["Pro","Max","Plus","Lite","Basic"])
                productprice = round(random.uniform(pr_lo, pr_hi), 2)
                products_to_insert.append({
                    "productid": pid,
                    "tenantid": int(prod_tenant_id),
                    "opuraproductid": f"OP_{prod_tenant_id}_{random.randint(1000,9999)}",
                    "productname": productname,
                    "productcategory": cat,
                    "productdescription": faker.sentence(nb_words=8),
                    "productprice": float(productprice),
                    "productimages": [faker.image_url() for _ in range(random.randint(1, 3))],
                    "productreviews": random.randint(0, 2000),
                    "producttags": random.sample(["eco","popular","discount","premium","new"], random.randint(1, 3)),
                    "productquantity": random.randint(10, 200),
                    "productbrand": random.choice(["BrandA","BrandB","BrandC","BrandD"]),
                    "productvariants": {"color": random.choice(["Red","Blue","Black","White","NA"])},
                })

        prod_df = pd.DataFrame(products_to_insert)
        insert_rows_dataframe(prod_df, TABLE_PRODUCT)
        st.success(f"✅ Inserted ~{len(products_to_insert)} products for tenant {selected_tenant}.")

    st.markdown("---")

    # ---------- Customer generation UI ----------
    st.subheader("Step 2 — Generate Customers (use products above)")
    generate_for_tenant = st.selectbox("Tenant for customer generation", tenant_df["tenantname"].tolist(), key="cust_gen_tenant")
    cust_tenant_id = int(tenant_df.loc[tenant_df["tenantname"] == generate_for_tenant, "tenantid"].values[0])

    num_customers = st.number_input("Number of customers to create", min_value=1, max_value=100000, value=500)
    avg_orders = st.slider("Avg orders per customer", 1, 30, 6)
    sale_prob = st.slider("Sale-season purchase probability (%)", 0, 100, 30)
    repeat_prob = st.slider("Repeat-buyer probability (%)", 0, 100, 10)
    high_spender_prob = st.slider("High-value customer probability (%)", 0, 100, 12)

    # Simple global age distribution
    st.markdown("**Age distribution** (these are % of customers; must total 100)")
    a18 = st.number_input("18-25 %", 0, 100, 20)
    a26 = st.number_input("26-40 %", 0, 100, 40)
    a41 = st.number_input("41-60 %", 0, 100, 30)
    a60 = st.number_input("60+ %", 0, 100, 10)
    total_age = a18 + a26 + a41 + a60
    if total_age != 100:
        st.warning("Age distribution does not sum to 100%. Adjust values.")

    if st.button("➕ Generate Customers (using productcatalog for prices)"):
        # Load product catalog for this tenant
        prod_sql = f"SELECT productid, productprice, productcategory FROM `{TABLE_PRODUCT}` WHERE tenantid=@tid"
        prod_df = run_query_df(prod_sql, {"tid": cust_tenant_id})

        if prod_df.empty:
            st.error("No products found for selected tenant — create products first.")
        else:
            st.info("Generating customers and purchase orders...")

            def pick_age():
                r = random.random() * 100
                if r < a18: return random.randint(18, 25)
                if r < a18 + a26: return random.randint(26, 40)
                if r < a18 + a26 + a41: return random.randint(41, 60)
                return random.randint(61, 80)

            all_customers = []
            prod_records = prod_df.to_dict(orient="records")

            for _ in range(int(num_customers)):
                cid = str(uuid.uuid4())
                opura_cid = f"CUST_{cust_tenant_id}_{random.randint(10000, 99999)}"
                name = faker.name()
                age = pick_age()
                city = faker.city()
                gender = random.choice(["Male","Female","Other"])

                # Wishlist — choose a few product IDs
                wishlist = random.sample([r["productid"] for r in prod_records],
                                         k=min(5, len(prod_records)))

                # Build purchase orders (with optional repeats)
                num_purchases = random.randint(max(1, avg_orders - 2), avg_orders + 2)
                purchases = []
                for _p in range(num_purchases):
                    p_row = random.choice(prod_records)
                    pid = p_row["productid"]
                    mrp = float(p_row["productprice"])

                    sale_flag = (random.random() * 100) < sale_prob
                    if sale_flag:
                        discount = random.uniform(0.05, 0.40)
                        saleprice = round(max(0.01, mrp * (1 - discount)), 2)
                    else:
                        saleprice = mrp

                    qty = random.randint(1, 5) if (random.random() * 100) < high_spender_prob else random.randint(1, 3)
                    purchase_date = faker.date_between(start_date='-730d', end_date='today')

                    purchases.append({
                        "productId": str(pid),
                        "purchaseDate": purchase_date.isoformat(),
                        "opuraProductID": f"OP_{cust_tenant_id}_{random.randint(1000, 9999)}",
                        "purchaseQuantity": int(qty),
                        "mrp": float(mrp),
                        "saleprice": float(saleprice),
                    })

                    # Repeat buyer simulation
                    if (random.random() * 100) < repeat_prob:
                        repeat_days = random.randint(15, 35)
                        repeat_date = (pd.to_datetime(purchase_date) + timedelta(days=repeat_days)).date().isoformat()
                        purchases.append({
                            "productId": str(pid),
                            "purchaseDate": repeat_date,
                            "opuraProductID": f"OP_{cust_tenant_id}_{random.randint(1000, 9999)}",
                            "purchaseQuantity": int(qty),
                            "mrp": float(mrp),
                            "saleprice": float(saleprice),
                        })

                # Final row (nested RECORD columns and arrays are Python dict/list)
                row = {
                    "customerid": str(cid),
                    "opuracustomerid": opura_cid,
                    "tenantid": int(cust_tenant_id),
                    "customername": name,
                    "customerpersonaldetails": {"age": int(age), "gender": gender},
                    "customergeolocations": {"city": city},
                    "customerwishlist": [str(w) for w in wishlist],
                    "purchaseorders": purchases,
                    "customertags": {"high_spender": (random.random() * 100) < high_spender_prob},
                    "opuracustomertags": {},
                    "recommendedproducts": [],
                }
                all_customers.append(row)

            cust_df = pd.DataFrame(all_customers)
            insert_rows_dataframe(cust_df, TABLE_CUSTOMER)
            st.success(f"✅ Inserted {len(all_customers)} customers for tenant {generate_for_tenant}.")

# ============================================================
# FOOTER
# ============================================================
st.caption("Using Google BigQuery (asia-south1). JSON-like data stored as RECORD/ARRAY for analytics-friendly querying.")