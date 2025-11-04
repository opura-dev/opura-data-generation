# streamlit_app.py

import json
import random
import uuid
from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st
import plotly.express as px
from faker import Faker
from google.cloud import bigquery

from bigquery_connect import (
    get_bq_client,
    ensure_dataset,
    ensure_tables,
    insert_rows_streaming_detailed,
    format_bq_errors,
    fetch_tenants_df,
    get_next_tenant_id,
    PROJECT_ID,
    DATASET_ID,
)

# -----------------------------
# App setup
# -----------------------------
st.set_page_config(page_title="BigQuery Data Generator + Analytics", layout="wide")
st.title("🚀 BigQuery Data Generator + Analytics (Streaming Inserts)")

faker = Faker()

# ============================================================
# ✅ Global Product Category Config (with weights)
# ============================================================
PRODUCT_CATEGORY_CONFIG = {
    "Electronics": {"price_range": (500, 5000), "weight": 5},
    "Grocery": {"price_range": (10, 300), "weight": 8},
    "Apparel": {"price_range": (100, 1500), "weight": 6},
    "Books": {"price_range": (50, 500), "weight": 3},
    "Furniture": {"price_range": (1500, 25000), "weight": 2},
    "Beauty": {"price_range": (50, 800), "weight": 4},
}

# -----------------------------
# Helpers
# -----------------------------
def iso_date(dt) -> str:
    return dt.isoformat()[:10]

def gen_products(tenantid: int, num_products: int):
    cats = ["Electronics", "Grocery", "Apparel", "Books", "Furniture", "Beauty"]
    out = []
    for _ in range(num_products):
        pid = str(uuid.uuid4())
        cat = random.choice(cats)
        rng = {
            "Electronics": (500, 5000),
            "Grocery": (10, 300),
            "Apparel": (100, 1500),
            "Books": (50, 500),
            "Furniture": (1500, 25000),
            "Beauty": (50, 800),
        }.get(cat, (50, 2000))

        price = round(random.uniform(*rng), 2)

        out.append({
            "productid": pid,
            "tenantid": tenantid,
            "opuraproductid": f"OP_{tenantid}_{random.randint(1000,9999)}",
            "productname": faker.word().capitalize() + " " + random.choice(["Pro","Max","Plus","Lite","Basic"]),
            "productcategory": cat,
            "productdescription": faker.sentence(nb_words=8),
            "productprice": float(price),

            # ✅ Convert list → JSON string
            "productimages": json.dumps([faker.image_url()]),

            # ✅ Convert tags list → JSON string
            "producttags": json.dumps(random.sample(["eco","popular","discount","premium","new"], k=2)),

            "productquantity": random.randint(10, 200),
            "productbrand": random.choice(["BrandA","BrandB","BrandC","BrandD"]),

            # ✅ Convert dict → JSON string
            "productvariants": json.dumps({"color": random.choice(["Red","Blue","Black","White"])})
        })
    return out

def gen_customers(
    tenantid: int,
    products: list[dict],
    num_customers: int,
    avg_orders: int,
    sale_prob_pct: int,
    repeat_prob_pct: int,
    high_spender_prob_pct: int,
):
    """Generate customers using existing product catalog."""
    def pick_age():
        r = random.random() * 100
        if r < 20: return random.randint(18, 25)
        if r < 60: return random.randint(26, 40)
        if r < 90: return random.randint(41, 60)
        return random.randint(61, 80)

    # Weighted category selection map (safe defaults)
    unique_cats = {p["productcategory"] for p in products}
    cat_weights = {c: PRODUCT_CATEGORY_CONFIG.get(c, {}).get("weight", 1) for c in unique_cats}

    def weighted_random_product():
        cats = list(cat_weights.keys())
        weights = [cat_weights[c] for c in cats]
        chosen = random.choices(cats, weights=weights, k=1)[0]
        eligible = [p for p in products if p["productcategory"] == chosen]
        return random.choice(eligible)

    product_ids = [p["productid"] for p in products]
    out = []

    for _ in range(num_customers):
        cid = str(uuid.uuid4())
        wishlist = random.sample(product_ids, k=min(5, len(product_ids)))
        orders = []
        num_purchases = random.randint(max(1, avg_orders - 2), avg_orders + 2)

        for __ in range(num_purchases):
            p = weighted_random_product()
            pid = p["productid"]
            mrp = float(p["productprice"])
            is_sale = (random.random() * 100) < sale_prob_pct
            saleprice = round(mrp * (1 - random.uniform(0.05, 0.40)), 2) if is_sale else mrp
            qty = random.randint(1, 5) if (random.random() * 100) < high_spender_prob_pct else random.randint(1, 3)
            base_date = faker.date_between(start_date='-730d', end_date='today')

            orders.append({
                "productId": pid,
                "purchaseDate": iso_date(base_date),  # DATE
                "opuraProductID": f"OP_{tenantid}_{random.randint(1000, 9999)}",
                "purchaseQuantity": qty,
                "mrp": mrp,
                "saleprice": saleprice,
            })

            if (random.random() * 100) < repeat_prob_pct:
                repeat_date = base_date + timedelta(days=random.randint(15, 35))
                orders.append({
                    "productId": pid,
                    "purchaseDate": iso_date(repeat_date),
                    "opuraProductID": f"OP_{tenantid}_{random.randint(1000, 9999)}",
                    "purchaseQuantity": qty,
                    "mrp": mrp,
                    "saleprice": saleprice,
                })

        out.append({
            "customerid": cid,
            "opuracustomerid": f"CUST_{tenantid}_{random.randint(10000, 99999)}",
            "tenantid": tenantid,
            "customername": faker.name(),
            "customerpersonaldetails": {"age": pick_age(), "gender": random.choice(["Male","Female","Other"])},
            "customergeolocations": {"city": faker.city()},
            "customerwishlist": wishlist,                           # ARRAY
            "purchaseorders": orders,                               # ARRAY<RECORD>
            "customertags": {"high_spender": random.random()*100 < high_spender_prob_pct},  # RECORD
            #"opuracustomertags": {"tier": random.choice(["gold","silver","bronze"])},       # RECORD ✅
            "recommendedproducts": random.sample(product_ids, min(3, len(product_ids))),     # ARRAY
        })
    return out

# ---------- Sidebar ----------
st.sidebar.header("⚙️ Setup")
if st.sidebar.button("Ensure Dataset & Tables (idempotent)"):
    try:
        client = get_bq_client()
        ensure_dataset(client)
        ensure_tables(client)
        st.sidebar.success("✅ Dataset & tables are ready.")
    except Exception as e:
        st.sidebar.error(f"❌ Setup failed: {e}")

st.sidebar.markdown(
    f"**Project:** `{PROJECT_ID}`  \n**Dataset:** `{DATASET_ID}`  \n**Location:** `asia-south1`"
)

# ---------- Tabs ----------
tab1, tab2, tab3, tab4 = st.tabs(["🏢 Tenants", "📦 Products", "👥 Customers", "📈 Analytics"])

# ============================================================
# Tab 1 — Tenants
# ============================================================
with tab1:
    st.subheader("🏢 Create Tenant (Manual Name Entry)")
    tenant_name_input = st.text_input("Enter Tenant Name", placeholder="e.g. Titan Company, Reliance Retail")

    if st.button("➕ Insert Tenant"):
        if not tenant_name_input.strip():
            st.warning("⚠ Please enter a valid tenant name.")
        else:
            client = get_bq_client()
            ensure_dataset(client)
            ensure_tables(client)
            next_id = get_next_tenant_id(client)

            tenant_row = {
                "tenantid": next_id,
                "tenantname": tenant_name_input.strip(),
                "onboardeddate": datetime.now(timezone.utc).date().isoformat(),
                "cataloglastupdated": None,
                "lasttraineddate": None,
                "active": True,
            }
            table_id = f"{PROJECT_ID}.{DATASET_ID}.tenantdata"
            ok, errs = insert_rows_streaming_detailed(client, table_id, [tenant_row])

            if errs:
                st.error("❌ Failed to insert tenant.")
                st.code(format_bq_errors(errs), language="text")
            else:
                st.success(f"✅ Tenant '{tenant_name_input}' inserted successfully (ID {next_id}).")

# ============================================================
# Tab 2 — Products
# ============================================================
with tab2:
    st.subheader("📦 Generate Products (Weighted Categories with Tenant Name)")
    try:
        client = get_bq_client()
        tdf = fetch_tenants_df(client)
        if tdf is None or tdf.empty:
            st.info("⚠ No tenants found. Please add a tenant first.")
        else:
            tenant_name = st.selectbox("Select Tenant", tdf["tenantname"].tolist(), key="prod_tenant")
            tenant_id = int(tdf.loc[tdf["tenantname"] == tenant_name, "tenantid"].iloc[0])
            num_products = st.slider("Number of products", 10, 10000, 200, step=10)

            if st.button("➕ Generate & Insert Products"):
                client = get_bq_client()
                st.info(f"📦 Generating {num_products} products for **{tenant_name}** (ID: {tenant_id})...")
                products = gen_products(tenant_id, num_products)

                table_id = f"{PROJECT_ID}.{DATASET_ID}.productcatalog"
                ok, errs = insert_rows_streaming_detailed(client, table_id, products)

                if errs:
                    st.error(f"❌ Errors while inserting products ({len(errs)} failures).")
                    st.code(format_bq_errors(errs), language="text")
                if ok:
                    st.success(f"✅ Successfully inserted {ok} products for **{tenant_name}**.")
    except Exception as e:
        st.error(f"❌ Failed to load Products tab: {e}")

# ============================================================
# Tab 3 — Customers
# ============================================================
with tab3:
    st.subheader("👥 Generate Customers & Orders (Using Real Product Catalog)")
    try:
        client = get_bq_client()
        tdf = fetch_tenants_df(client)

        if tdf is None or tdf.empty:
            st.info("No tenants found. Add a tenant in Tab 1.")
        else:
            tname = st.selectbox("Select Tenant", tdf["tenantname"].tolist(), key="cust_tenant_select")
            tid = int(tdf.loc[tdf["tenantname"] == tname, "tenantid"].iloc[0])

            num_customers = st.slider("Number of customers", 1, 100000, 500, step=50)
            avg_orders = st.slider("Average orders per customer", 1, 30, 6)
            sale_prob = st.slider("Sale-season discount probability (%)", 0, 100, 30)
            repeat_prob = st.slider("Repeat buyer probability (%)", 0, 100, 10)
            high_spender_prob = st.slider("High-value purchase probability (%)", 0, 100, 12)

            if st.button("➕ Insert Customers Using Existing Product Catalog"):
                # Fetch products
                product_query = f"""
                SELECT productid, productprice, productcategory
                FROM `{PROJECT_ID}.{DATASET_ID}.productcatalog`
                WHERE tenantid = @tid
                """
                product_df = client.query(
                    product_query,
                    job_config=bigquery.QueryJobConfig(
                        query_parameters=[bigquery.ScalarQueryParameter("tid", "INT64", tid)]
                    ),
                ).result().to_dataframe()

                if product_df.empty:
                    st.error(f"❌ No products found for tenant **{tname}**. Please add products first.")
                else:
                    products = product_df.to_dict(orient="records")
                    customers = gen_customers(
                        tid, products, num_customers, avg_orders, sale_prob, repeat_prob, high_spender_prob
                    )

                    cust_table = f"{PROJECT_ID}.{DATASET_ID}.customerdata"
                    ok, errs = insert_rows_streaming_detailed(client, cust_table, customers)

                    if errs:
                        st.error(f"⚠️ Inserted {ok}, but {len(errs)} rows failed")
                        st.code(format_bq_errors(errs), language="text")
                    if ok:
                        st.success(f"✅ Successfully inserted {ok} customers for **{tname}**")
    except Exception as e:
        st.error(f"❌ Failed: {e}")

# ============================================================
# Tab 4 — Analytics (KPIs + Charts)
# ============================================================
with tab4:
    st.subheader("📈 Tenant Analytics Dashboard")
    st.info("Select a tenant to view KPIs and charts.")

    try:
        client = get_bq_client()
        tdf = fetch_tenants_df(client)
        if tdf is None or tdf.empty:
            st.warning("⚠ No tenants found.")
        else:
            tenant_choice = st.selectbox("🔍 Select Tenant for Analysis", tdf["tenantname"].tolist())
            tenant_id = int(tdf.loc[tdf["tenantname"] == tenant_choice, "tenantid"].iloc[0])
            top_n_products = st.slider("Top N products by revenue", min_value=5, max_value=50, value=10, step=5)

            if st.button("▶️ Run Analytics"):
                st.success(f"📊 Showing data for tenant: **{tenant_choice}**")

                # KPIs
                sql_kpi = f"""
                WITH orders AS (
                  SELECT po.saleprice AS saleprice, po.purchaseQuantity AS qty, po.mrp
                  FROM `{PROJECT_ID}.{DATASET_ID}.customerdata` c,
                  UNNEST(c.purchaseorders) AS po
                  WHERE c.tenantid = @tid
                )
                SELECT
                  (SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.customerdata` WHERE tenantid = @tid) AS total_customers,
                  COALESCE(SUM(qty), 0) AS total_orders,
                  COALESCE(SUM(saleprice * qty), 0) AS total_revenue,
                  COALESCE(AVG(1 - (saleprice / NULLIF(mrp, 0))) * 100, 0) AS avg_discount
                FROM orders
                """
                kpi_df = client.query(
                    sql_kpi, job_config=bigquery.QueryJobConfig(
                        query_parameters=[bigquery.ScalarQueryParameter("tid","INT64",tenant_id)]
                    )
                ).result().to_dataframe()

                c1, c2, c3, c4 = st.columns(4)
                c1.metric("👥 Customers", int(kpi_df["total_customers"][0] or 0))
                c2.metric("🛒 Orders", int(kpi_df["total_orders"][0] or 0))
                c3.metric("💰 Revenue", f"{(kpi_df['total_revenue'][0] or 0):,.2f}")
                c4.metric("🏷 Avg Discount (%)", f"{(kpi_df['avg_discount'][0] or 0):.2f}%")

                st.markdown("---")

                # Revenue by Category
                sql_category = f"""
                WITH orders AS (
                  SELECT po.saleprice AS saleprice, po.purchaseQuantity AS qty, po.productId AS productid
                  FROM `{PROJECT_ID}.{DATASET_ID}.customerdata` c,
                  UNNEST(c.purchaseorders) AS po
                  WHERE c.tenantid = @tid
                )
                SELECT pc.productcategory AS category, SUM(o.saleprice * o.qty) AS revenue
                FROM orders o
                LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.productcatalog` pc
                  ON o.productid = pc.productid
                GROUP BY category
                ORDER BY revenue DESC
                """
                cat_df = client.query(
                    sql_category, job_config=bigquery.QueryJobConfig(
                        query_parameters=[bigquery.ScalarQueryParameter("tid","INT64",tenant_id)]
                    )
                ).result().to_dataframe()

                # Trend
                sql_trend = f"""
                SELECT
                  DATE_TRUNC(po.purchaseDate, MONTH) AS month,
                  SUM(po.saleprice * po.purchaseQuantity) AS revenue
                FROM `{PROJECT_ID}.{DATASET_ID}.customerdata` c,
                UNNEST(c.purchaseorders) AS po
                WHERE c.tenantid = @tid
                GROUP BY month
                ORDER BY month
                """
                trend_df = client.query(
                    sql_trend, job_config=bigquery.QueryJobConfig(
                        query_parameters=[bigquery.ScalarQueryParameter("tid","INT64",tenant_id)]
                    )
                ).result().to_dataframe()

                # Avg Discount by Category
                sql_discount = f"""
                WITH orders AS (
                  SELECT po.productId, po.saleprice, po.mrp
                  FROM `{PROJECT_ID}.{DATASET_ID}.customerdata` c,
                  UNNEST(c.purchaseorders) AS po
                  WHERE c.tenantid = @tid
                )
                SELECT
                  pc.productcategory AS category,
                  AVG(1 - (o.saleprice / NULLIF(o.mrp, 0))) * 100 AS avg_discount
                FROM orders o
                LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.productcatalog` pc
                  ON o.productId = pc.productid
                GROUP BY category
                ORDER BY avg_discount DESC
                """
                disc_df = client.query(
                    sql_discount, job_config=bigquery.QueryJobConfig(
                        query_parameters=[bigquery.ScalarQueryParameter("tid","INT64",tenant_id)]
                    )
                ).result().to_dataframe()

                # Top Products
                sql_top_products = f"""
                SELECT
                  COALESCE(pc.productname, po.productId) AS productname,
                  SUM(po.saleprice * po.purchaseQuantity) AS revenue
                FROM `{PROJECT_ID}.{DATASET_ID}.customerdata` c,
                UNNEST(c.purchaseorders) AS po
                JOIN `{PROJECT_ID}.{DATASET_ID}.productcatalog` pc
                  ON po.productId = pc.productid
                WHERE c.tenantid = @tid
                GROUP BY productname
                ORDER BY revenue DESC
                LIMIT @topn
                """
                top_df = client.query(
                    sql_top_products,
                    job_config=bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter("tid","INT64",tenant_id),
                            bigquery.ScalarQueryParameter("topn","INT64",top_n_products),
                        ]
                    )
                ).result().to_dataframe()

                # Segmentation
                sql_segments = f"""
                WITH cust_revenue AS (
                  SELECT
                    c.customerid,
                    SUM(po.saleprice * po.purchaseQuantity) AS total_spend
                  FROM `{PROJECT_ID}.{DATASET_ID}.customerdata` c,
                  UNNEST(c.purchaseorders) AS po
                  WHERE c.tenantid = @tid
                  GROUP BY c.customerid
                ),
                thresholds AS (
                  SELECT
                    (SELECT APPROX_QUANTILES(total_spend, 100)[OFFSET(80)] FROM cust_revenue) AS p80,
                    (SELECT APPROX_QUANTILES(total_spend, 100)[OFFSET(30)] FROM cust_revenue) AS p30
                )
                SELECT
                  CASE
                    WHEN cr.total_spend >= t.p80 THEN 'High Spender'
                    WHEN cr.total_spend >= t.p30 THEN 'Medium Spender'
                    ELSE 'Low Spender'
                  END AS segment,
                  COUNT(*) AS customer_count
                FROM cust_revenue cr, thresholds t
                GROUP BY segment
                ORDER BY segment
                """
                seg_df = client.query(
                    sql_segments, job_config=bigquery.QueryJobConfig(
                        query_parameters=[bigquery.ScalarQueryParameter("tid","INT64",tenant_id)]
                    )
                ).result().to_dataframe()

                # ----- Charts -----
                c1, c2 = st.columns(2)
                with c1:
                    st.subheader("📦 Revenue by Category")
                    if not cat_df.empty:
                        fig_cat = px.bar(cat_df, x="category", y="revenue", text_auto=".2s", title="Revenue by Category")
                        st.plotly_chart(fig_cat, use_container_width=True)
                    else:
                        st.warning("No category data found.")

                with c2:
                    st.subheader("🏷 Avg Discount by Category")
                    if not disc_df.empty:
                        fig_disc = px.bar(disc_df, x="category", y="avg_discount", text_auto=".2s", title="Average Discount (%)")
                        st.plotly_chart(fig_disc, use_container_width=True)
                    else:
                        st.warning("No discount data found.")

                c3, c4 = st.columns(2)
                with c3:
                    st.subheader(f"🏆 Top {top_n_products} Products by Revenue")
                    if not top_df.empty:
                        fig_top = px.bar(top_df, x="revenue", y="productname", orientation="h",
                                         title="Top Products by Revenue", text_auto=".2s")
                        st.plotly_chart(fig_top, use_container_width=True)
                        st.dataframe(top_df, use_container_width=True)
                    else:
                        st.warning("No product revenue data available.")

                with c4:
                    st.subheader("🧩 Customer Segmentation")
                    if not seg_df.empty:
                        fig_seg = px.pie(seg_df, names="segment", values="customer_count",
                                         title="Customer Segments (High / Medium / Low)")
                        st.plotly_chart(fig_seg, use_container_width=True)
                        st.dataframe(seg_df, use_container_width=True)
                    else:
                        st.warning("No segmentation data available.")

                st.subheader("📆 Monthly Revenue Trend")
                if not trend_df.empty:
                    fig_trend = px.line(trend_df, x="month", y="revenue", markers=True,
                                        title="Revenue Trend Over Time")
                    st.plotly_chart(fig_trend, use_container_width=True)
                else:
                    st.warning("No trend data available.")
    except Exception as e:
        st.error(f"❌ Failed to load analytics: {str(e)}")