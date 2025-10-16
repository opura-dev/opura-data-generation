import streamlit as st
import pandas as pd
import numpy as np
import random
import uuid
from faker import Faker
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import time
import plotly.express as px

# ============================================================
#  SECTION 1 — Silent Database Connection Setup
# ============================================================

DB_CONFIG = {
    "host": "13.51.45.172",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "OpuR@!8i854"  # <-- Replace with your real password
}

def db_url_from_config(cfg):
    safe_password = quote_plus(cfg["password"])
    return f"postgresql+psycopg2://{cfg['user']}:{safe_password}@{cfg['host']}:{cfg['port']}/{cfg['dbname']}"

def test_db_connection(engine):
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1;"))
            return True
        except Exception as e:
            if attempt == max_retries:
                st.error(f"❌ Unable to connect to the database: {str(e)}")
                return False
            time.sleep(2)

try:
    engine = create_engine(db_url_from_config(DB_CONFIG), future=True, echo=False)
    if not test_db_connection(engine):
        st.stop()
except Exception as e:
    st.error(f"❌ Database setup failed: {e}")
    st.stop()

# ============================================================
#  SECTION 2 — Database Utility Functions
# ============================================================

def fetch_tenants():
    q = text("SELECT tenantid, tenantname FROM tenantdata ORDER BY tenantid;")
    with engine.connect() as conn:
        return pd.read_sql(q, conn)

def fetch_festive_events(tenant_id):
    q = text("""
        SELECT event_name, start_date, end_date, category
        FROM public_festive_events
        WHERE tenantid = :tid
        ORDER BY start_date;
    """)
    with engine.connect() as conn:
        df = pd.read_sql(q, conn, params={"tid": tenant_id})
    return df

def fetch_festivals(tenant_id):
    query = text("""
        SELECT festival_name, start_date, no_of_days, category, weight
        FROM public_festive_events
        WHERE tenantid = :tid AND is_active = TRUE
        ORDER BY start_date;
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"tid": tenant_id})
    return df

# ============================================================
#  SECTION 3 — Data Generation Logic
# ============================================================

faker = Faker()

def generate_product_catalog(tenant_id, num_products):
    categories = ["Electronics", "Grocery", "Apparel", "Books", "Furniture", "Beauty"]
    data = []
    for _ in range(num_products):
        pid = str(uuid.uuid4())
        category = random.choice(categories)
        mrp = round(random.uniform(100, 5000), 2)
        data.append({
            "productid": pid,
            "tenantid": tenant_id,
            "productname": faker.word().capitalize(),
            "category": category,
            "mrp": mrp
        })
    return pd.DataFrame(data)

def generate_customer_data(tenant_id, num_customers, product_df, sale_distribution, repeated_buyer_ratio):
    customers = []
    purchase_orders = []

    for _ in range(num_customers):
        cid = str(uuid.uuid4())
        cust_name = faker.name()
        cust_email = faker.email()
        cust_phone = faker.phone_number()

        customers.append({
            "customerid": cid,
            "tenantid": tenant_id,
            "name": cust_name,
            "email": cust_email,
            "phone": cust_phone
        })

        # Determine number of purchases
        num_purchases = random.randint(3, 10)
        purchased_products = product_df.sample(num_purchases, replace=True)

        for _, product in purchased_products.iterrows():
            purchase_date = faker.date_between(datetime(2023, 1, 1), datetime(2025, 10, 15))
            mrp = product["mrp"]

            # Determine if discounted (based on sale_distribution)
            if random.random() < sale_distribution:
                discount_pct = random.uniform(5, 30)
                sale_price = round(mrp * (1 - discount_pct / 100), 2)
            else:
                sale_price = mrp

            purchase_orders.append({
                "customerid": cid,
                "productid": product["productid"],
                "purchaseDate": purchase_date,
                "opuraProductID": f"OP_{tenant_id}_{random.randint(1000,9999)}",
                "purchaseQuantity": random.randint(1, 5),
                "mrp": mrp,
                "saleprice": sale_price
            })

            # Handle repeated buyers
            if random.random() < repeated_buyer_ratio:
                repeat_count = random.randint(1, 3)
                for _ in range(repeat_count):
                    repeat_date = purchase_date + timedelta(days=random.randint(15, 30))
                    purchase_orders.append({
                        "customerid": cid,
                        "productid": product["productid"],
                        "purchaseDate": repeat_date,
                        "opuraProductID": f"OP_{tenant_id}_{random.randint(1000,9999)}",
                        "purchaseQuantity": random.randint(1, 5),
                        "mrp": mrp,
                        "saleprice": sale_price
                    })

    return pd.DataFrame(customers), pd.DataFrame(purchase_orders)

# ============================================================
#  SECTION 4 — Streamlit UI Setup
# ============================================================

st.set_page_config(page_title="Multi-tenant Data Generation + Insights (v7)", layout="wide")
st.title("🏗️ Multi-tenant Data Generation + Insights (v7)")

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🏢 Tenant Creation",
    "📊 Data Summary",
    "📂 Category Drilldown",
    "📈 Trends & Insights",
    "🧩 Bulk Data Gen"
])

# ============================================================
# TAB 1: TENANT CREATION + FESTIVAL MANAGEMENT (FINAL)
# ============================================================
with tab1:
    st.header("🏢 Tenant & Festival Management")

    tenants = fetch_tenants()
    next_id = int(tenants["tenantid"].max()) + 1 if not tenants.empty else 1
    st.info(f"Next Tenant ID will be: {next_id}")

    tenant_name = st.text_input("Tenant Name", placeholder="Enter tenant/company name")

    # =====================================================
    # CREATE NEW TENANT + COPY DEFAULT FESTIVALS FROM TENANT 1
    # =====================================================
    if st.button("Create Tenant"):
        onboard_date = datetime.now(timezone.utc).date() - timedelta(days=random.randint(100, 1000))
        q_insert_tenant = text("""
            INSERT INTO tenantdata 
                (tenantid, tenantname, onboardeddate, cataloglastupdated, lasttraineddate, active)
            VALUES (:id, :name, :onboard, :catalog, :trained, TRUE)
        """)

        try:
            with engine.begin() as conn:
                conn.execute(q_insert_tenant, {
                    "id": int(next_id),
                    "name": tenant_name,
                    "onboard": onboard_date,
                    "catalog": onboard_date + timedelta(days=30),
                    "trained": onboard_date + timedelta(days=60)
                })

                # Copy default festivals from Tenant 1
                q_copy_festivals = text("""
                    INSERT INTO public_festive_events 
                        (tenantid, festival_name, start_date, end_date, region, category, weight,
                         is_active, created_at, updated_at, no_of_days)
                    SELECT 
                        :new_tid, festival_name, start_date, end_date, region, category, weight,
                        is_active, NOW(), NOW(), no_of_days
                    FROM public_festive_events
                    WHERE tenantid = 1;
                """)
                conn.execute(q_copy_festivals, {"new_tid": int(next_id)})

            st.success(
                f"✅ Tenant '{tenant_name}' added successfully and cloned default festivals from Tenant 1."
            )

        except Exception as e:
            st.error(f"❌ Failed to add tenant: {e}")

    # =====================================================
    # FESTIVAL MANAGEMENT SECTION
    # =====================================================
    st.divider()
    st.subheader("🎉 Add, View, Modify, or Delete Festive Events")

    tenants = fetch_tenants()
    tenant_choice = st.selectbox("Select Tenant", tenants["tenantname"].tolist(), key="tenant_festival_select")
    tenant_id = int(tenants.loc[tenants["tenantname"] == tenant_choice, "tenantid"].values[0])

    # Fetch festivals for the selected tenant
    try:
        events_df = fetch_festivals(tenant_id)
    except Exception as e:
        st.error(f"❌ Failed to fetch festivals: {e}")
        events_df = pd.DataFrame()

    # Guarantee required columns
    required_cols = ["festival_name", "start_date", "end_date", "category", "weight", "no_of_days"]
    for col in required_cols:
        if col not in events_df.columns:
            events_df[col] = None

    for date_col in ["start_date", "end_date"]:
        if date_col in events_df.columns:
            events_df[date_col] = pd.to_datetime(events_df[date_col], errors="coerce").dt.date

    # =====================================================
    # RESTORE DEFAULT FESTIVALS FROM TENANT 1
    # =====================================================
    with st.expander("♻️ Restore Default Festivals from Tenant 1"):
        st.info(
            "This will replace all existing festivals for this tenant "
            "with the default list from Tenant 1."
        )
        if st.button("Restore from Default Tenant 1"):
            try:
                with engine.begin() as conn:
                    conn.execute(text("DELETE FROM public_festive_events WHERE tenantid = :tid;"), {"tid": tenant_id})
                    conn.execute(text("""
                        INSERT INTO public_festive_events 
                            (tenantid, festival_name, start_date, end_date, region, category, weight,
                             is_active, created_at, updated_at, no_of_days)
                        SELECT 
                            :new_tid, festival_name, start_date, end_date, region, category, weight,
                            is_active, NOW(), NOW(), no_of_days
                        FROM public_festive_events
                        WHERE tenantid = 1;
                    """), {"new_tid": tenant_id})

                st.success(f"✅ Restored default festivals from Tenant 1 for {tenant_choice}.")
                events_df = fetch_festivals(tenant_id)

            except Exception as e:
                st.error(f"❌ Failed to restore festivals: {e}")

    # =====================================================
    # DELETE INDIVIDUAL FESTIVALS
    # =====================================================
    if not events_df.empty:
        st.write("🗑️ Select and delete any individual festival below:")
        delete_options = events_df["festival_name"].tolist()
        festival_to_delete = st.selectbox("Select Festival to Delete", delete_options, key="delete_festival")

        if st.button("Delete Selected Festival"):
            try:
                with engine.begin() as conn:
                    conn.execute(
                        text("DELETE FROM public_festive_events WHERE tenantid = :tid AND festival_name = :fname;"),
                        {"tid": tenant_id, "fname": festival_to_delete}
                    )
                st.success(f"🗑️ Festival '{festival_to_delete}' deleted successfully.")
                events_df = fetch_festivals(tenant_id)
            except Exception as e:
                st.error(f"❌ Error while deleting festival: {e}")

    # =====================================================
    # EDIT EXISTING FESTIVALS
    # =====================================================
    if not events_df.empty:
        st.write("✏️ Modify any festival details below, then click **Update Festivals**:")
        edited_df = st.data_editor(
            events_df,
            num_rows="dynamic",
            use_container_width=True,
            key="festival_editor",
        )

        if st.button("Update Festivals"):
            try:
                with engine.begin() as conn:
                    for _, row in edited_df.iterrows():
                        start_date_val = row["start_date"] if pd.notna(row["start_date"]) else None
                        end_date_val = row["end_date"] if pd.notna(row["end_date"]) else None
                        days_val = int(row["no_of_days"]) if not pd.isna(row["no_of_days"]) else None

                        q_update = text("""
                            UPDATE public_festive_events
                            SET 
                                festival_name = :fname,
                                start_date = :sdate,
                                end_date = :edate,
                                category = :cat,
                                weight = :weight,
                                no_of_days = :days,
                                updated_at = NOW()
                            WHERE tenantid = :tid AND festival_name = :orig_name
                        """)
                        conn.execute(q_update, {
                            "tid": tenant_id,
                            "fname": row["festival_name"],
                            "sdate": start_date_val,
                            "edate": end_date_val,
                            "cat": row["category"],
                            "weight": row["weight"],
                            "days": days_val,
                            "orig_name": row["festival_name"],
                        })

                st.success("✅ Festivals updated successfully!")
                refreshed_df = fetch_festivals(tenant_id)
                for col in required_cols:
                    if col not in refreshed_df.columns:
                        refreshed_df[col] = None
                for date_col in ["start_date", "end_date"]:
                    refreshed_df[date_col] = pd.to_datetime(refreshed_df[date_col], errors="coerce").dt.date
                st.dataframe(refreshed_df, use_container_width=True)

            except Exception as e:
                st.error(f"❌ Error while updating festivals: {e}")

    else:
        st.info("ℹ️ No festivals found for this tenant yet.")

    # =====================================================
    # ADD NEW FESTIVAL
    # =====================================================
    st.divider()
    st.subheader("➕ Add New Festival")

    with st.expander("Add a New Festival for this Tenant"):
        festival_name = st.text_input("Festival Name", placeholder="Enter new festival name")
        start_date = st.date_input("Start Date", datetime.now(timezone.utc).date())
        end_date = st.date_input("End Date (Optional)", datetime.now(timezone.utc).date() + timedelta(days=7))
        region = st.text_input("Region", "All India")
        category = st.selectbox("Category", ["Sale", "Holiday", "Offer", "Cultural", "Seasonal"])
        weight = st.slider("Festival Impact Weight", 0.1, 2.0, 1.0, step=0.1)

        if st.button("Add Festival", key="add_new_fest"):
            try:
                no_of_days = (end_date - start_date).days if end_date else None
                q_insert_festive = text("""
                    INSERT INTO public_festive_events 
                        (tenantid, festival_name, start_date, end_date, region, category, weight, 
                         is_active, created_at, updated_at, no_of_days)
                    VALUES 
                        (:tid, :fname, :sdate, :edate, :region, :cat, :weight, TRUE, NOW(), NOW(), :days)
                """)
                with engine.begin() as conn:
                    conn.execute(q_insert_festive, {
                        "tid": tenant_id,
                        "fname": festival_name,
                        "sdate": start_date,
                        "edate": end_date if end_date else None,
                        "region": region,
                        "cat": category,
                        "weight": weight,
                        "days": no_of_days
                    })
                st.success(f"🎉 Festival '{festival_name}' added successfully for Tenant '{tenant_choice}'.")
            except Exception as e:
                st.error(f"❌ Error while adding new festival: {e}")

# ============================================================
# TAB 2: DATA SUMMARY (JSONB-AWARE)
# ============================================================
with tab2:
    st.header("📊 Tenant Data Summary")

    tenants = fetch_tenants()
    tenant_choice = st.selectbox("Select Tenant", tenants["tenantname"].tolist(), key="tab2_tenant")
    tenant_id = int(tenants.loc[tenants["tenantname"] == tenant_choice, "tenantid"].values[0])

    q_summary = text("""
        SELECT 
            COUNT(DISTINCT c.customerid) AS total_customers,
            COUNT(DISTINCT p.productid) AS total_products,
            COUNT(*) AS total_orders,
            SUM((po->>'saleprice')::float * (po->>'purchaseQuantity')::int) AS total_revenue,
            AVG(
                CASE WHEN (po->>'mrp')::float > 0 
                     THEN (1 - (po->>'saleprice')::float / (po->>'mrp')::float) * 100 
                     ELSE 0 END
            ) AS avg_discount
        FROM customerdata c
        CROSS JOIN LATERAL jsonb_array_elements(c.purchaseorders) AS po
        JOIN productcatalog p ON (po->>'productId')::uuid = p.productid
        WHERE c.tenantid = :tid;
    """)

    with engine.connect() as conn:
        df_summary = pd.read_sql(q_summary, conn, params={"tid": tenant_id})

    st.subheader("📈 Key Business Metrics")

    if not df_summary.empty:
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("👥 Customers", int(df_summary["total_customers"][0] or 0))
        c2.metric("📦 Products", int(df_summary["total_products"][0] or 0))
        c3.metric("🛒 Orders", int(df_summary["total_orders"][0] or 0))
        c4.metric("💰 Revenue", f"${df_summary['total_revenue'][0] or 0:,.2f}")
        c5.metric("🏷️ Avg Discount", f"{df_summary['avg_discount'][0] or 0:.2f}%")
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

    # Build dynamic SQL depending on choice
    if tenant_choice == "All Tenants":
        q_cat = text("""
            SELECT 
                c.tenantid,
                NULLIF((po->>'purchaseDate'), '')::date AS purchase_date,
                NULLIF((po->>'purchaseQuantity'), '')::int AS purchase_quantity,
                COALESCE(p.productprice::numeric, 0) AS base_price,
                ROUND(
                    (COALESCE(p.productprice::numeric, 0) * (1 - (RANDOM() * 0.10 + 0.10)))::numeric,
                    2
                ) AS saleprice,
                COALESCE(p.productcategory, 'Unknown') AS category,
                p.productname AS product_name
            FROM customerdata c
            CROSS JOIN LATERAL jsonb_array_elements(c.purchaseorders) AS po
            JOIN productcatalog p ON (po->>'productId')::uuid = p.productid
            WHERE NULLIF((po->>'purchaseDate'), '')::date BETWEEN :start AND :end;
        """)
        query_params = {"start": start_date.isoformat(), "end": end_date.isoformat()}
    else:
        tenant_id = int(
            tenants.loc[tenants["tenantname"] == tenant_choice, "tenantid"].values[0]
        )
        q_cat = text("""
            SELECT 
                c.tenantid,
                NULLIF((po->>'purchaseDate'), '')::date AS purchase_date,
                NULLIF((po->>'purchaseQuantity'), '')::int AS purchase_quantity,
                COALESCE(p.productprice::numeric, 0) AS base_price,
                ROUND(
                    (COALESCE(p.productprice::numeric, 0) * (1 - (RANDOM() * 0.10 + 0.10)))::numeric,
                    2
                ) AS saleprice,
                COALESCE(p.productcategory, 'Unknown') AS category,
                p.productname AS product_name
            FROM customerdata c
            CROSS JOIN LATERAL jsonb_array_elements(c.purchaseorders) AS po
            JOIN productcatalog p ON (po->>'productId')::uuid = p.productid
            WHERE c.tenantid = :tid
              AND NULLIF((po->>'purchaseDate'), '')::date BETWEEN :start AND :end
            ORDER BY purchase_date;
        """)
        query_params = {"tid": tenant_id, "start": start_date.isoformat(), "end": end_date.isoformat()}

    with engine.connect() as conn:
        df = pd.read_sql(q_cat, conn, params=query_params)

    if df.empty:
        st.warning("No purchase data found.")
    else:
        df["purchase_date"] = pd.to_datetime(df["purchase_date"], errors="coerce")
        df["revenue"] = df["saleprice"] * df["purchase_quantity"]
        df["discount_pct"] = np.where(
            df["base_price"] > 0, (1 - df["saleprice"] / df["base_price"]) * 100, 0
        )

        if tenant_choice == "All Tenants":
            st.subheader("🏷️ Global Category Summary Across Tenants")
            cat_summary = (
                df.groupby(["tenantid", "category"], as_index=False)
                .agg(
                    total_orders=("purchase_quantity", "sum"),
                    total_revenue=("revenue", "sum"),
                    avg_discount=("discount_pct", "mean"),
                )
            )
            st.dataframe(cat_summary, use_container_width=True)
            fig = px.bar(
                cat_summary,
                x="category",
                y="total_revenue",
                color="tenantid",
                title="Revenue by Category (All Tenants)",
                text_auto=".2s",
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.subheader(f"🏷️ Category Summary for {tenant_choice}")
            cat_summary = (
                df.groupby("category", as_index=False)
                .agg(
                    total_orders=("purchase_quantity", "sum"),
                    total_revenue=("revenue", "sum"),
                    avg_discount=("discount_pct", "mean"),
                )
                .sort_values("total_revenue", ascending=False)
            )
            st.dataframe(cat_summary, use_container_width=True)
            st.plotly_chart(
                px.bar(
                    cat_summary,
                    x="category",
                    y="total_revenue",
                    text_auto=".2s",
                    title=f"Revenue by Category — {tenant_choice}",
                ),
                use_container_width=True,
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
        q_trend = text("""
            SELECT 
                c.tenantid,
                NULLIF((po->>'purchaseDate'), '')::date AS purchase_date,
                NULLIF((po->>'purchaseQuantity'), '')::int AS purchase_quantity,
                COALESCE(p.productprice::numeric, 0) AS base_price,
                ROUND(
                    (COALESCE(p.productprice::numeric, 0) * (1 - (RANDOM() * 0.10 + 0.10)))::numeric,
                    2
                ) AS saleprice,
                COALESCE(p.productcategory, 'Unknown') AS category,
                p.productname AS product_name
            FROM customerdata c
            CROSS JOIN LATERAL jsonb_array_elements(c.purchaseorders) AS po
            JOIN productcatalog p ON (po->>'productId')::uuid = p.productid
            WHERE NULLIF((po->>'purchaseDate'), '')::date BETWEEN :start AND :end
            ORDER BY purchase_date;
        """)
        query_params = {"start": start_date.isoformat(), "end": end_date.isoformat()}
    else:
        tenant_id = int(
            tenants.loc[tenants["tenantname"] == tenant_choice, "tenantid"].values[0]
        )
        q_trend = text("""
            SELECT 
                c.tenantid,
                NULLIF((po->>'purchaseDate'), '')::date AS purchase_date,
                NULLIF((po->>'purchaseQuantity'), '')::int AS purchase_quantity,
                COALESCE(p.productprice::numeric, 0) AS base_price,
                ROUND(
                    (COALESCE(p.productprice::numeric, 0) * (1 - (RANDOM() * 0.10 + 0.10)))::numeric,
                    2
                ) AS saleprice,
                COALESCE(p.productcategory, 'Unknown') AS category,
                p.productname AS product_name
            FROM customerdata c
            CROSS JOIN LATERAL jsonb_array_elements(c.purchaseorders) AS po
            JOIN productcatalog p ON (po->>'productId')::uuid = p.productid
            WHERE c.tenantid = :tid
              AND NULLIF((po->>'purchaseDate'), '')::date BETWEEN :start AND :end
            ORDER BY purchase_date;
        """)
        query_params = {"tid": tenant_id, "start": start_date.isoformat(), "end": end_date.isoformat()}

    with engine.connect() as conn:
        df = pd.read_sql(q_trend, conn, params=query_params)

    if df.empty:
        st.warning("No trend data available.")
    else:
        df["purchase_date"] = pd.to_datetime(df["purchase_date"], errors="coerce")
        df["revenue"] = df["saleprice"] * df["purchase_quantity"]

        if tenant_choice == "All Tenants":
            st.subheader("📊 Combined Trends Across All Tenants")

            monthly = (
                df.groupby([df["purchase_date"].dt.to_period("M"), "tenantid"])
                .agg(revenue=("revenue", "sum"))
                .reset_index()
            )
            monthly["purchase_date"] = monthly["purchase_date"].dt.to_timestamp()

            fig = px.line(
                monthly,
                x="purchase_date",
                y="revenue",
                color="tenantid",
                markers=True,
                title="Monthly Revenue Trend by Tenant",
            )
            st.plotly_chart(fig, use_container_width=True)

            st.subheader("🏷️ Category Revenue Split (All Tenants)")
            cat_df = (
                df.groupby(["tenantid", "category"], as_index=False)
                .agg(total_revenue=("revenue", "sum"))
            )
            st.plotly_chart(
                px.sunburst(
                    cat_df,
                    path=["tenantid", "category"],
                    values="total_revenue",
                    title="Revenue Breakdown by Tenant and Category",
                ),
                use_container_width=True,
            )
        else:
            st.subheader(f"📈 Monthly Trends for {tenant_choice}")

            monthly = (
                df.groupby(df["purchase_date"].dt.to_period("M"))
                .agg(revenue=("revenue", "sum"))
                .reset_index()
            )
            monthly["purchase_date"] = monthly["purchase_date"].dt.to_timestamp()

            fig = px.line(
                monthly,
                x="purchase_date",
                y="revenue",
                markers=True,
                title=f"Monthly Revenue Trend — {tenant_choice}",
            )
            st.plotly_chart(fig, use_container_width=True)

            st.subheader("🏷️ Category Revenue Split")
            cat_df = (
                df.groupby("category", as_index=False)
                .agg(total_revenue=("revenue", "sum"))
                .sort_values("total_revenue", ascending=False)
            )
            st.plotly_chart(
                px.pie(cat_df, values="total_revenue", names="category"),
                use_container_width=True,
            )

# -------------------------
# TAB 5: PRODUCT + CUSTOMER GENERATOR
# -------------------------
import json

tab5 = st.tab("🧪 Product & Customer Generator")[0] if False else None
# We'll explicitly create a new container to keep consistency (avoids reusing earlier 'tab' vars)
with st.expander("🧪 Product & Customer Generator (create products first, then customers)", expanded=True):
    st.header("🧪 Product Catalog → Customer Generator")

    # ---------- Product generation UI ----------
    st.subheader("Step 1 — Create Product Catalog")
    tenant_df = fetch_tenants()
    if tenant_df.empty:
        st.warning("No tenants found. Create a tenant first in Tab 1.")
    else:
        selected_tenant = st.selectbox("Select tenant to add products for", tenant_df["tenantname"].tolist(), key="tab5_tenant")
        prod_tenant_id = int(tenant_df.loc[tenant_df["tenantname"] == selected_tenant, "tenantid"].values[0])

        num_products = st.number_input("Number of products to create", min_value=1, max_value=5000, value=200)
        st.markdown("Define category distribution below (must total 100%). You may use Auto-distribute to evenly split.")

        # fetch existing categories (or provide defaults)
        default_categories = ["Electronics", "Grocery", "Apparel", "Books", "Furniture", "Beauty"]
        # allow the user to choose categories (or use defaults)
        use_catalog_categories = st.checkbox("Use existing catalog categories for tenant (if any)", value=False)
        if use_catalog_categories:
            with engine.connect() as conn:
                res = pd.read_sql(text("SELECT DISTINCT productcategory FROM productcatalog WHERE tenantid = :tid"),
                                  conn, params={"tid": prod_tenant_id})
            categories = res["productcategory"].dropna().tolist() or default_categories
        else:
            categories = st.multiselect("Categories list (pick or type new)", default_categories, default=default_categories)

        if not categories:
            st.error("Please specify at least one category.")
            st.stop()

        # UI to define distribution per category
        cols = st.columns(4)
        category_weights = {}
        col_iter = iter(cols)
        # provide an 'Auto distribute' option
        auto_dist = st.checkbox("Auto-distribute categories evenly", value=True)
        if auto_dist:
            even_val = int(100 / len(categories))
            remainder = 100 - even_val * len(categories)
            for i, cat in enumerate(categories):
                # give remainder to first few
                w = even_val + (1 if i < remainder else 0)
                category_weights[cat] = w
        else:
            # custom sliders
            for i, cat in enumerate(categories):
                col = cols[i % len(cols)]
                category_weights[cat] = col.slider(f"{cat} %", 0, 100, int(100 / len(categories)), key=f"cat_{cat}")

        # check sum
        total_pct = sum(category_weights.values())
        st.markdown(f"**Total distribution = {total_pct}%**")
        if total_pct != 100:
            st.warning("Category percentages must sum to 100%. Adjust sliders or enable Auto-distribute.")
        create_products_btn = st.button("➕ Create Products", disabled=(total_pct != 100))

        if create_products_btn:
            st.info("Creating product rows — inserting into productcatalog...")
            # compute number of products per category using integer rounding while summing to num_products
            cat_names = list(category_weights.keys())
            pct_list = [category_weights[c] for c in cat_names]
            proportions = [p / 100.0 for p in pct_list]
            # naive allocation
            counts = [int(round(p * num_products)) for p in proportions]
            # fix rounding differences
            diff = num_products - sum(counts)
            i = 0
            while diff != 0:
                counts[i % len(counts)] += 1 if diff > 0 else -1
                diff = num_products - sum(counts)
                i += 1

            # generate product rows
            products_to_insert = []
            for cat, cnt in zip(cat_names, counts):
                # category-specific price ranges (you can change map)
                price_range = {
                    "Electronics": (500, 5000),
                    "Grocery": (10, 300),
                    "Apparel": (100, 1500),
                    "Books": (50, 500),
                    "Furniture": (1500, 25000),
                    "Beauty": (50, 800)
                }.get(cat, (50, 2000))

                for _ in range(max(0, cnt)):
                    pid = str(uuid.uuid4())
                    productname = faker.word().capitalize() + " " + random.choice(["Pro", "Max", "Plus", "Lite", "Basic"])
                    productprice = round(random.uniform(*price_range), 2)
                    product = {
                        "productid": pid,
                        "tenantid": int(prod_tenant_id),
                        "opuraproductid": f"OP_{prod_tenant_id}_{random.randint(1000,9999)}",
                        "productname": productname,
                        "productcategory": cat,
                        "productdescription": faker.sentence(nb_words=8),
                        "productprice": float(productprice),
                        "productimages": json.dumps([faker.image_url() for _ in range(random.randint(1, 3))]),
                        "productreviews": random.randint(0, 2000),
                        "producttags": json.dumps(random.sample(["eco", "popular", "discount", "premium", "new"], random.randint(1, 3))),
                        "productquantity": random.randint(10, 200),
                        "productbrand": random.choice(["BrandA","BrandB","BrandC","BrandD"]),
                        "productvariants": json.dumps({"color": random.choice(["Red","Blue","Black","White","NA"])})
                    }
                    products_to_insert.append(product)

            # insert into DB using executemany (ON CONFLICT DO NOTHING)
            insert_q = text("""
                INSERT INTO productcatalog (
                    productid, tenantid, opuraproductid, productname, productcategory,
                    productdescription, productprice, productimages, productreviews,
                    producttags, productquantity, productbrand, productvariants
                ) VALUES (
                    :productid, :tenantid, :opuraproductid, :productname, :productcategory,
                    :productdescription, :productprice, :productimages, :productreviews,
                    :producttags, :productquantity, :productbrand, :productvariants
                )
                ON CONFLICT (productid) DO NOTHING;
            """)
            try:
                with engine.begin() as conn:
                    conn.execute(insert_q, products_to_insert)
                st.success(f"✅ Inserted ~{len(products_to_insert)} products for tenant {selected_tenant}.")
            except Exception as e:
                st.error(f"❌ Failed to insert products: {e}")

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

    # Option to bias age distribution here globally (simple)
    st.markdown("**Age distribution** (these are % of customers; must total 100)")
    a18 = st.number_input("18-25 %", 0, 100, 20)
    a26 = st.number_input("26-40 %", 0, 100, 40)
    a41 = st.number_input("41-60 %", 0, 100, 30)
    a60 = st.number_input("60+ %", 0, 100, 10)
    total_age = a18 + a26 + a41 + a60
    if total_age != 100:
        st.warning("Age distribution does not sum to 100%. Adjust values.")

    # ============================================================
    # BUTTON — GENERATE CUSTOMERS
    # ============================================================
    from psycopg2.extras import Json

    generate_customers_btn = st.button("➕ Generate Customers (using productcatalog for prices)")

    if generate_customers_btn:
        # Load product catalog for this tenant (must exist)
        with engine.connect() as conn:
            prod_df = pd.read_sql(
                text("SELECT productid, productprice, productcategory FROM productcatalog WHERE tenantid = :tid"),
                conn,
                params={"tid": cust_tenant_id}
            )

        if prod_df.empty:
            st.error("No products found for selected tenant — create products first.")
        else:
            st.info("Generating customers and purchase orders... this may take a while for large numbers.")


            # Helper to draw age
            def pick_age():
                r = random.random() * 100
                if r < a18:
                    return random.randint(18, 25)
                if r < a18 + a26:
                    return random.randint(26, 40)
                if r < a18 + a26 + a41:
                    return random.randint(41, 60)
                return random.randint(61, 80)


            all_customers = []

            # Generate synthetic customers
            for _ in range(int(num_customers)):
                cid = str(uuid.uuid4())
                opura_cid = f"CUST_{cust_tenant_id}_{random.randint(10000, 99999)}"
                name = faker.name()
                age = pick_age()
                city = faker.city()
                gender = random.choice(["Male", "Female", "Other"])

                # Wishlist — sample a few product IDs
                wishlist = prod_df.sample(min(5, len(prod_df)))["productid"].tolist()

                # Build purchase orders
                num_purchases = random.randint(max(1, avg_orders - 2), avg_orders + 2)
                purchases = []
                for _p in range(num_purchases):
                    p_row = prod_df.sample(1).iloc[0]
                    pid = p_row["productid"]
                    mrp = float(p_row["productprice"])

                    # Determine if sale
                    sale_flag = (random.random() * 100) < sale_prob
                    if sale_flag:
                        discount = random.uniform(0.05, 0.40)
                        saleprice = round(max(0.01, mrp * (1 - discount)), 2)
                    else:
                        saleprice = mrp

                    qty = random.randint(1, 5) if (random.random() * 100) < high_spender_prob else random.randint(1, 3)
                    purchase_date = faker.date_between(start_date='-730d', end_date='today').isoformat()

                    purchases.append({
                        "productId": str(pid),  # ✅ ensure UUID → string
                        "purchaseDate": purchase_date,
                        "opuraProductID": f"OP_{cust_tenant_id}_{random.randint(1000, 9999)}",
                        "purchaseQuantity": qty,
                        "mrp": mrp,
                        "saleprice": saleprice
                    })

                    # Repeat buyer simulation
                    if (random.random() * 100) < repeat_prob:
                        repeat_days = random.randint(15, 35)
                        repeat_date = (pd.to_datetime(purchase_date) + timedelta(days=repeat_days)).date().isoformat()
                        purchases.append({
                            "productId": str(pid),
                            "purchaseDate": repeat_date,
                            "opuraProductID": f"OP_{cust_tenant_id}_{random.randint(1000, 9999)}",
                            "purchaseQuantity": qty,
                            "mrp": mrp,
                            "saleprice": saleprice
                        })

                # Prepare DB row (raw)
                row = {
                    "customerid": str(cid),
                    "opuracustomerid": opura_cid,
                    "tenantid": int(cust_tenant_id),
                    "customername": name,
                    "customerpersonaldetails": {"age": age, "gender": gender},
                    "customergeolocations": {"city": city},
                    "customerwishlist": [str(w) for w in wishlist],  # ✅ convert UUIDs → strings
                    "purchaseorders": purchases,
                    "customertags": {"high_spender": (random.random() * 100) < high_spender_prob},
                    "opuracustomertags": {},
                    "recommendedproducts": []
                }

                # ✅ Wrap JSON fields for psycopg2 compatibility
                json_fields = [
                    "customerpersonaldetails",
                    "customergeolocations",
                    "customerwishlist",
                    "purchaseorders",
                    "customertags",
                    "opuracustomertags",
                    "recommendedproducts"
                ]
                for jf in json_fields:
                    row[jf] = Json(row[jf])

                all_customers.append(row)

            # Insert all customers (bulk, batched)
            insert_query = text("""
                INSERT INTO customerdata (
                    customerid, opuracustomerid, tenantid, customername,
                    customerpersonaldetails, customergeolocations,
                    customerwishlist, purchaseorders,
                    customertags, opuracustomertags, recommendedproducts
                )
                VALUES (
                    :customerid, :opuracustomerid, :tenantid, :customername,
                    :customerpersonaldetails, :customergeolocations,
                    :customerwishlist, :purchaseorders,
                    :customertags, :opuracustomertags, :recommendedproducts
                )
                ON CONFLICT (customerid) DO NOTHING;
            """)

            try:
                chunk_size = 500
                inserted = 0
                with engine.begin() as conn:
                    for i in range(0, len(all_customers), chunk_size):
                        chunk = all_customers[i:i + chunk_size]
                        conn.execute(insert_query, chunk)
                        inserted += len(chunk)
                st.success(f"✅ Inserted {inserted} customers for tenant {generate_for_tenant}.")
            except Exception as e:
                st.error(f"❌ Failed to insert customers: {e}")