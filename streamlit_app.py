# streamlit_app.py

import json
import random
import uuid
from datetime import datetime, timedelta, timezone

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

st.set_page_config(page_title="BigQuery Data Generator", layout="wide")
st.title("🚀 BigQuery Data Generator (Streaming Inserts)")

faker = Faker()

# ---------- Helpers ----------
def iso_date(dt) -> str:
    return dt.isoformat()[:10]

def gen_tenants(n: int):
    out = []
    for tid in range(1, n + 1):
        onboard = (datetime.now(timezone.utc) - timedelta(days=random.randint(100, 1000))).date()
        out.append({
            "tenantid": tid,
            "tenantname": faker.company(),
            "onboardeddate": iso_date(onboard),
            "cataloglastupdated": iso_date(onboard + timedelta(days=30)),
            "lasttraineddate": iso_date(onboard + timedelta(days=60)),
            "active": True,
        })
    return out

def gen_festivals(tenantid: int, m: int = 5):
    cats = ["Sale", "Holiday", "Offer", "Cultural", "Seasonal"]
    events = []
    for _ in range(m):
        start = faker.date_between(datetime(2023, 1, 1), datetime(2025, 10, 15))
        days = random.randint(1, 7)
        end = start + timedelta(days=days)
        ts = datetime.now(timezone.utc).isoformat()
        events.append({
            "tenantid": tenantid,
            "festival_name": faker.word().capitalize(),
            "start_date": iso_date(start),
            "end_date": iso_date(end),
            "region": "All India",
            "category": random.choice(cats),
            "weight": round(random.uniform(0.5, 1.5), 2),
            "is_active": True,
            "created_at": ts,
            "updated_at": ts,
            "no_of_days": days,
        })
    return events

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
            "productimages": json.dumps(["https://example.com/img1.jpg"]),
            "productreviews": random.randint(0, 2000),
            "producttags": json.dumps(["eco","popular"]),
            "productquantity": random.randint(10, 200),
            "productbrand": random.choice(["BrandA","BrandB","BrandC","BrandD"]),
            "productvariants": json.dumps({"color": "Black"}),
        })
    return out

def gen_customers(tenantid: int, products, num_customers: int, avg_orders: int, sale_prob_pct: int, repeat_prob_pct: int, high_spender_prob_pct: int):
    def pick_age():
        r = random.random() * 100
        if r < 20: return random.randint(18, 25)
        if r < 60: return random.randint(26, 40)
        if r < 90: return random.randint(41, 60)
        return random.randint(61, 80)

    customers = []
    product_ids = [p["productid"] for p in products]

    for _ in range(num_customers):
        cid = str(uuid.uuid4())
        wishlist = random.sample(product_ids, k=min(5, len(product_ids)))
        orders = []
        num_purchases = random.randint(max(1, avg_orders - 2), avg_orders + 2)
        for __ in range(num_purchases):
            pid = random.choice(product_ids)
            mrp = float(next(p["productprice"] for p in products if p["productid"] == pid))
            sale = (random.random() * 100) < sale_prob_pct
            saleprice = round(max(0.01, mrp * (1 - random.uniform(0.05, 0.40))), 2) if sale else mrp
            qty = random.randint(1, 5) if (random.random() * 100) < high_spender_prob_pct else random.randint(1, 3)
            base_date = faker.date_between(start_date='-730d', end_date='today')

            orders.append({
                "productId": str(pid),
                "purchaseDate": iso_date(base_date),  # DATE
                "opuraProductID": f"OP_{tenantid}_{random.randint(1000, 9999)}",
                "purchaseQuantity": int(qty),
                "mrp": float(mrp),
                "saleprice": float(saleprice),
            })

            if (random.random() * 100) < repeat_prob_pct:
                repeat_date = base_date + timedelta(days=random.randint(15, 35))
                orders.append({
                    "productId": str(pid),
                    "purchaseDate": iso_date(repeat_date),
                    "opuraProductID": f"OP_{tenantid}_{random.randint(1000, 9999)}",
                    "purchaseQuantity": int(qty),
                    "mrp": float(mrp),
                    "saleprice": float(saleprice),
                })

        customers.append({
            "customerid": cid,
            "opuracustomerid": f"CUST_{tenantid}_{random.randint(10000, 99999)}",
            "tenantid": int(tenantid),
            "customername": faker.name(),
            "customerpersonaldetails": {"age": int(pick_age()), "gender": random.choice(["Male", "Female", "Other"])},
            "customergeolocations": {"city": faker.city()},
            "customerwishlist": [str(x) for x in wishlist],
            "purchaseorders": orders,
            "customertags": {"high_spender": bool(random.random() * 100 < high_spender_prob_pct)},
            "opuracustomertags": json.dumps({"tier": random.choice(["gold", "silver", "bronze"])}),  # JSON string
            "recommendedproducts": [str(x) for x in random.sample(product_ids, k=min(3, len(product_ids)))],
        })
    return customers

# ---------- Sidebar setup ----------
st.sidebar.header("⚙️ Setup")
if st.sidebar.button("Ensure Dataset & Tables (idempotent)"):
    try:
        client = get_bq_client()
        ensure_dataset(client)
        ensure_tables(client)
        st.sidebar.success("✅ Dataset & tables are ready.")
    except Exception as e:
        st.sidebar.error(f"❌ Setup failed: {e}")

st.sidebar.markdown(f"**Project:** `{PROJECT_ID}`  \n**Dataset:** `{DATASET_ID}`  \n**Location:** `asia-south1`")

# ---------- Tabs ----------
tab1, tab2, tab3, tab4 = st.tabs(["🏢 Tenants & Festivals", "📦 Products", "👥 Customers", "📈 Analytics"])

# ----- Tenant Creation -----
with tab1:
    st.subheader("🏢 Create Tenant (Manual Name Entry)")

    tenant_name_input = st.text_input(
        "Enter Tenant Name",
        placeholder="e.g. Titan Company, Reliance Retail, Tata Motors"
    )

    if st.button("➕ Insert Tenant"):
        if not tenant_name_input.strip():
            st.warning("⚠ Please enter a valid tenant name.")
        else:
            try:
                client = get_bq_client()
                ensure_dataset(client)
                ensure_tables(client)

                # Get next tenant ID automatically
                next_tenant_id = get_next_tenant_id(client)

                # Prepare row
                tenant_row = {
                    "tenantid": next_tenant_id,  # Internal ID (not shown to user)
                    "tenantname": tenant_name_input.strip(),
                    "onboardeddate": datetime.now(timezone.utc).date().isoformat(),
                    "cataloglastupdated": None,
                    "lasttraineddate": None,
                    "active": True
                }

                table_id = f"{PROJECT_ID}.{DATASET_ID}.tenantdata"
                ok, errs = insert_rows_streaming_detailed(client, table_id, [tenant_row])

                if errs:
                    st.error("❌ Failed to insert tenant.")
                    st.code(format_bq_errors(errs), language="text")
                else:
                    st.success(f"✅ Tenant '{tenant_name_input}' inserted successfully with internal ID {next_tenant_id}.")

            except Exception as e:
                st.error(f"❌ Error inserting tenant: {e}")

    st.markdown("---")


# ----- Tab 2: Products -----
with tab2:
    st.subheader("📦 Generate Products (Weighted Categories with Tenant Names)")

    try:
        client = get_bq_client()
        tdf = fetch_tenants_df(client)
        if tdf is None or tdf.empty:
            st.info("⚠ No tenants found. Please add a tenant first in the 'Tenants & Festivals' tab.")
        else:
            tenant_name = st.selectbox("Select Tenant Name for product insertion", tdf["tenantname"].tolist(), key="prod_tenant_name_dropdown")
            tenant_id = int(tdf.loc[tdf["tenantname"] == tenant_name, "tenantid"].iloc[0])

            num_products = st.slider("Number of products", 10, 10000, 200, step=10)

            if st.button("➕ Insert Products"):
                client = get_bq_client()
                st.info(f"📦 Generating {num_products} products for tenant **{tenant_name}** (ID: {tenant_id})...")

                # Generate products using weighted categories
                products = gen_products(tenant_id, num_products)

                # Insert into BigQuery
                table_id = f"{PROJECT_ID}.{DATASET_ID}.productcatalog"
                ok, errs = insert_rows_streaming_detailed(client, table_id, products)

                if errs:
                    st.error(f"❌ Errors while inserting products ({len(errs)} failures).")
                    st.code(format_bq_errors(errs), language="text")
                if ok:
                    st.success(f"✅ Successfully inserted {ok} products for **{tenant_name}**.")
    except Exception as e:
        st.error(f"❌ Failed to load products tab: {e}")

# ----- Tab 3: Customers -----
with tab3:
    st.subheader("👥 Generate Customers & Purchase Orders (Using Real Product Catalog)")
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
                client = get_bq_client()

                # 1️⃣ Fetch products from BigQuery
                product_query = f"""
                SELECT productid, productprice, productcategory
                FROM `{PROJECT_ID}.{DATASET_ID}.productcatalog`
                WHERE tenantid = {tid}
                """
                product_df = client.query(product_query).result().to_dataframe()

                if product_df.empty:
                    st.error(f"❌ No products found for tenant **{tname}**. Please add products first.")
                else:
                    # Convert products into list of dicts
                    products = product_df.to_dict(orient="records")

                    # Weighted random selection per category
                    category_weights = {c: PRODUCT_CATEGORY_CONFIG.get(c, {"weight": 1})["weight"]
                                        for c in product_df["productcategory"].unique()}


                    def weighted_random_product():
                        cats = list(category_weights.keys())
                        weights = [category_weights[c] for c in cats]
                        chosen_category = random.choices(cats, weights=weights, k=1)[0]
                        # Filter only products from the chosen category
                        eligible_products = [p for p in products if p["productcategory"] == chosen_category]
                        return random.choice(eligible_products)


                    # 2️⃣ Generate customers using REAL products
                    customers = []
                    for _ in range(num_customers):
                        cid = str(uuid.uuid4())
                        wishlist = random.sample([p["productid"] for p in products], min(5, len(products)))
                        orders = []
                        num_purchases = random.randint(max(1, avg_orders - 2), avg_orders + 2)

                        for __ in range(num_purchases):
                            p = weighted_random_product()
                            pid = p["productid"]
                            mrp = float(p["productprice"])

                            is_sale = (random.random() * 100) < sale_prob
                            saleprice = round(mrp * (1 - random.uniform(0.05, 0.40)), 2) if is_sale else mrp
                            qty = random.randint(1, 5) if (
                                                                      random.random() * 100) < high_spender_prob else random.randint(
                                1, 3)
                            base_date = faker.date_between(start_date='-730d', end_date='today')

                            orders.append({
                                "productId": pid,
                                "purchaseDate": base_date.isoformat()[:10],
                                "opuraProductID": f"OP_{tid}_{random.randint(1000, 9999)}",
                                "purchaseQuantity": qty,
                                "mrp": mrp,
                                "saleprice": saleprice
                            })

                            if (random.random() * 100) < repeat_prob:
                                repeat_date = base_date + timedelta(days=random.randint(15, 35))
                                orders.append({
                                    "productId": pid,
                                    "purchaseDate": repeat_date.isoformat()[:10],
                                    "opuraProductID": f"OP_{tid}_{random.randint(1000, 9999)}",
                                    "purchaseQuantity": qty,
                                    "mrp": mrp,
                                    "saleprice": saleprice
                                })

                        customers.append({
                            "customerid": cid,
                            "opuracustomerid": f"CUST_{tid}_{random.randint(10000, 99999)}",
                            "tenantid": tid,
                            "customername": faker.name(),
                            "customerpersonaldetails": {
                                "age": random.randint(18, 70),
                                "gender": random.choice(["Male", "Female", "Other"])
                            },
                            "customergeolocations": {"city": faker.city()},
                            "customerwishlist": wishlist,
                            "purchaseorders": orders,
                            "customertags": {"high_spender": bool(random.random() * 100 < high_spender_prob)},
                            "opuracustomertags": json.dumps({"tier": random.choice(["gold", "silver", "bronze"])}),
                            "recommendedproducts": random.sample([p["productid"] for p in products],
                                                                 min(3, len(products)))
                        })

                    # 3️⃣ Insert into BigQuery
                    cust_table = f"{PROJECT_ID}.{DATASET_ID}.customerdata"
                    ok, errs = insert_rows_streaming_detailed(client, cust_table, customers)

                    if errs:
                        st.error(f"❌ Inserted {ok} customers, but {len(errs)} errors occurred.")
                        st.code(format_bq_errors(errs), language="text")
                    if ok:
                        st.success(f"✅ Successfully inserted {ok} customers using REAL products for **{tname}**.")

    except Exception as e:
        st.error(f"❌ Failed: {e}")


# ----- Tab 4: Analytics (Dashboard per Tenant) -----
with tab4:
    st.subheader("📈 Tenant Analytics Dashboard")
    st.info("Select a tenant (by name) to view KPIs and charts.")

    try:
        client = get_bq_client()
        tdf = fetch_tenants_df(client)
        if tdf is None or tdf.empty:
            st.warning("⚠ No tenants found. Please insert tenants first.")
        else:
            tenant_choice = st.selectbox("🔍 Select Tenant for Analysis", tdf["tenantname"].tolist())
            tenant_id = int(tdf.loc[tdf["tenantname"] == tenant_choice, "tenantid"].iloc[0])

            # Let user control Top N products
            top_n_products = st.slider("Top N products by revenue", min_value=5, max_value=50, value=10, step=5)

            if st.button("▶️ Run Analytics"):
                st.success(f"📊 Showing data for tenant: **{tenant_choice}**")

                # ========== KPI Metrics ==========
                sql_kpi = f"""
                WITH orders AS (
                  SELECT po.saleprice AS saleprice, po.purchaseQuantity AS qty, po.mrp
                  FROM `{PROJECT_ID}.{DATASET_ID}.customerdata` c,
                  UNNEST(c.purchaseorders) AS po
                  WHERE c.tenantid = {tenant_id}
                )
                SELECT
                  (SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.customerdata` WHERE tenantid = {tenant_id}) AS total_customers,
                  SUM(qty) AS total_orders,
                  SUM(saleprice * qty) AS total_revenue,
                  AVG(1 - (saleprice / NULLIF(mrp, 0))) * 100 AS avg_discount
                FROM orders
                """
                kpi_df = client.query(sql_kpi).result().to_dataframe()

                col1, col2, col3, col4 = st.columns(4)
                if not kpi_df.empty:
                    col1.metric("👥 Total Customers", int(kpi_df["total_customers"][0] or 0))
                    col2.metric("🛒 Total Orders", int(kpi_df["total_orders"][0] or 0))
                    col3.metric("💰 Total Revenue", f"{(kpi_df['total_revenue'][0] or 0):,.2f}")
                    col4.metric("🏷 Avg Discount (%)", f"{(kpi_df['avg_discount'][0] or 0):.2f}%")

                st.markdown("---")

                # ========== Revenue by Category ==========
                sql_category = f"""
                WITH orders AS (
                  SELECT po.saleprice AS saleprice, po.purchaseQuantity AS qty, po.productId AS productid
                  FROM `{PROJECT_ID}.{DATASET_ID}.customerdata` c,
                  UNNEST(c.purchaseorders) AS po
                  WHERE c.tenantid = {tenant_id}
                )
                SELECT pc.productcategory AS category, SUM(o.saleprice * o.qty) AS revenue
                FROM orders o
                LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.productcatalog` pc
                ON o.productid = pc.productid
                GROUP BY category
                ORDER BY revenue DESC
                """
                cat_df = client.query(sql_category).result().to_dataframe()

                # ========== Monthly Revenue Trend ==========
                sql_trend = f"""
                SELECT
                  DATE_TRUNC(po.purchaseDate, MONTH) AS month,
                  SUM(po.saleprice * po.purchaseQuantity) AS revenue
                FROM `{PROJECT_ID}.{DATASET_ID}.customerdata` c,
                UNNEST(c.purchaseorders) AS po
                WHERE c.tenantid = {tenant_id}
                GROUP BY month
                ORDER BY month
                """
                trend_df = client.query(sql_trend).result().to_dataframe()

                # ========== Avg Discount by Category ==========
                sql_discount = f"""
                WITH orders AS (
                  SELECT po.productId, po.saleprice, po.mrp
                  FROM `{PROJECT_ID}.{DATASET_ID}.customerdata` c,
                  UNNEST(c.purchaseorders) AS po
                  WHERE c.tenantid = {tenant_id}
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
                disc_df = client.query(sql_discount).result().to_dataframe()

                # ========== Top Products by Revenue ==========
                sql_top_products = f"""
                SELECT
                  COALESCE(pc.productname, po.productId) AS productname,
                  SUM(po.saleprice * po.purchaseQuantity) AS revenue
                FROM `{PROJECT_ID}.{DATASET_ID}.customerdata` c,
                UNNEST(c.purchaseorders) AS po
                JOIN `{PROJECT_ID}.{DATASET_ID}.productcatalog` pc
                  ON po.productId = pc.productid
                WHERE c.tenantid = {tenant_id}
                GROUP BY productname
                ORDER BY revenue DESC
                LIMIT {top_n_products}
                """
                top_df = client.query(sql_top_products).result().to_dataframe()

                # ========== Customer Segmentation (High/Medium/Low spenders) ==========
                sql_segments = f"""
                WITH cust_revenue AS (
                  SELECT
                    c.customerid,
                    SUM(po.saleprice * po.purchaseQuantity) AS total_spend
                  FROM `{PROJECT_ID}.{DATASET_ID}.customerdata` c,
                  UNNEST(c.purchaseorders) AS po
                  WHERE c.tenantid = {tenant_id}
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
                seg_df = client.query(sql_segments).result().to_dataframe()

                # ========== Layout: 2 charts side-by-side (category & discount) ==========
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

                # ========== Layout: Top Products & Segmentation ==========
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

                # ========== Full Width: Revenue Trend ==========
                st.subheader("📆 Monthly Revenue Trend")
                if not trend_df.empty:
                    fig_trend = px.line(trend_df, x="month", y="revenue", markers=True,
                                        title="Revenue Trend Over Time")
                    st.plotly_chart(fig_trend, use_container_width=True)
                else:
                    st.warning("No trend data available.")

    except Exception as e:
        st.error(f"❌ Failed to load analytics: {str(e)}")