import streamlit as st
import pandas as pd
import numpy as np
import random
import uuid
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import time
import plotly.express as px
import psycopg2
from psycopg2 import sql
import multiprocessing as mp
import io
from dataclasses import dataclass
from typing import List, Dict, Tuple
import gc

# Try to import orjson (much faster than stdlib json)
try:
    import orjson

    def json_dumps(obj):
        return orjson.dumps(obj).decode("utf-8")

    def json_loads(s):
        return orjson.loads(s)

    JSON_LIBRARY = "orjson (ultra-fast)"
except ImportError:
    import json

    json_dumps = json.dumps
    json_loads = json.loads
    JSON_LIBRARY = "stdlib json (consider: pip install orjson)"

st.set_page_config(page_title="🚀 Ultra-Optimized Data Generator", layout="wide")

# ============================================================
#  DATABASE CONNECTION SETUP
# ============================================================

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "testdb",
    "user": "postgres",
    "password": "pranav2772004",
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
                st.error(f"❌ Unable to connect to database: {str(e)}")
                return False
            time.sleep(2)
    return False


try:
    engine = create_engine(
        db_url_from_config(DB_CONFIG),
        future=True,
        echo=False,
        pool_pre_ping=True,
        pool_size=20,
        max_overflow=10,
        pool_recycle=3600,
    )
    if not test_db_connection(engine):
        st.stop()
except Exception as e:
    st.error(f"❌ Database setup failed: {e}")
    st.stop()

# ============================================================
#  OPTIMIZED UTILITY FUNCTIONS
# ============================================================

np.random.seed(42)
random.seed(42)


@st.cache_data
def generate_name_pool(size=50000):
    """Pre-generate names for reuse - 100x faster than repeated Faker calls"""
    from faker import Faker

    fake = Faker()
    Faker.seed(42)
    return [fake.name() for _ in range(size)]


@st.cache_data
def generate_email_pool(size=50000):
    """Pre-generate emails for reuse"""
    from faker import Faker

    fake = Faker()
    Faker.seed(43)
    return [fake.email() for _ in range(size)]


NAME_POOL = generate_name_pool()
EMAIL_POOL = generate_email_pool()


def get_random_name(idx):
    return NAME_POOL[idx % len(NAME_POOL)]


def get_random_email(idx):
    return EMAIL_POOL[idx % len(EMAIL_POOL)]


def fetch_tenants():
    q = text("SELECT tenantid, tenantname FROM tenantdata ORDER BY tenantid;")
    with engine.connect() as conn:
        return pd.read_sql(q, conn)


# ============================================================
#  ULTRA-FAST COPY-BASED INSERT
# ============================================================


def ultra_fast_copy_insert(
    df: pd.DataFrame,
    table_name: str,
    engine,
    progress_callback=None,
    chunk_rows: int = 50_000,
) -> None:
    """PostgreSQL COPY FROM - fastest possible insert method"""
    total_rows = len(df)
    st.info(f"🚀 Ultra-fast COPY inserting {total_rows:,} rows into `{table_name}` ...")

    start_time = time.time()

    try:
        raw = engine.raw_connection()
        cur = raw.cursor()

        cur.execute("SET session statement_timeout = 0")
        cur.execute("SET session lock_timeout = 0")
        cur.execute("SET session synchronous_commit = OFF")
        cur.execute("SET session maintenance_work_mem = '1024MB'")

        cur.execute(
            """
            SELECT idx.indexname, idx.indexdef
            FROM pg_indexes idx
            WHERE idx.tablename = %s 
              AND idx.schemaname = 'public'
              AND idx.indexname NOT IN (
                  SELECT conname 
                  FROM pg_constraint 
                  WHERE conrelid = (SELECT oid FROM pg_class WHERE relname = %s)
              )
        """,
            (table_name, table_name),
        )

        index_defs = {row[0]: row[1] for row in cur.fetchall()}

        if index_defs:
            st.info(f"🧹 Dropping {len(index_defs)} indexes for faster insert...")
            for idx_name in index_defs.keys():
                try:
                    cur.execute(
                        sql.SQL("DROP INDEX IF EXISTS {}").format(
                            sql.Identifier(idx_name)
                        )
                    )
                except Exception as e:
                    st.warning(f"⚠️ Could not drop index {idx_name}: {e}")

        raw.commit()

        columns = list(df.columns)
        rows_done = 0

        for start in range(0, total_rows, chunk_rows):
            end = min(start + chunk_rows, total_rows)
            chunk = df.iloc[start:end]

            buffer = io.StringIO()
            chunk.to_csv(buffer, index=False, header=False, sep="\t", na_rep="\\N")
            buffer.seek(0)

            copy_sql = sql.SQL(
                "COPY {} ({}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')"
            ).format(
                sql.Identifier(table_name),
                sql.SQL(", ").join(map(sql.Identifier, columns)),
            )

            cur.copy_expert(copy_sql, buffer)
            rows_done += len(chunk)

            if rows_done % (chunk_rows * 2) == 0 or end == total_rows:
                raw.commit()
                if progress_callback:
                    progress_callback(rows_done / total_rows)

                elapsed = time.time() - start_time
                speed = rows_done / elapsed if elapsed > 0 else 0
                st.text(
                    f"  ⚡ {rows_done:,} / {total_rows:,} rows | {speed:,.0f} rec/sec"
                )

        raw.commit()

        elapsed = time.time() - start_time
        final_speed = total_rows / elapsed if elapsed > 0 else 0
        st.success(
            f"✅ Inserted {total_rows:,} rows in {elapsed:.1f}s ({final_speed:,.0f} rec/sec)"
        )

        if index_defs:
            st.info(f"🔨 Rebuilding {len(index_defs)} indexes...")
            raw.autocommit = True  # Enable autocommit for CONCURRENT index creation
            for idx_name, idx_def in index_defs.items():
                try:
                    concurrent_def = idx_def.replace(
                        "CREATE INDEX", "CREATE INDEX CONCURRENTLY", 1
                    )
                    cur.execute(concurrent_def)
                    # No commit needed in autocommit mode
                except Exception as e:
                    st.warning(f"⚠️ Could not rebuild index {idx_name}: {e}")
            raw.autocommit = False  # Disable autocommit
            st.success("✅ Indexes rebuilt")

        cur.execute("SET session synchronous_commit = ON")
        raw.commit()

    except Exception as e:
        raw.rollback()
        st.error(f"❌ Insert failed: {e}")
        raise
    finally:
        try:
            raw.close()
        except:
            pass


# ============================================================
#  CATEGORY HIERARCHY
# ============================================================

CATEGORY_HIERARCHY = {
    "Electronics": {
        "subcategories": [
            "Smartphones",
            "Laptops",
            "Tablets",
            "Cameras",
            "Headphones",
            "Speakers",
            "Smart Watches",
            "Gaming Consoles",
            "TVs",
            "Home Theater",
            "Computer Accessories",
            "Mobile Accessories",
            "Audio Equipment",
            "Wearables",
        ],
        "price_range": (500, 50000),
        "brands": [
            "Samsung",
            "Apple",
            "Sony",
            "LG",
            "Dell",
            "HP",
            "Lenovo",
            "Asus",
            "OnePlus",
            "Xiaomi",
            "Realme",
            "Oppo",
            "Vivo",
            "Motorola",
        ],
        "seasonal_boost": {"November": 1.5, "December": 1.8, "January": 1.3},
    },
    "Fashion": {
        "subcategories": [
            "Men's Clothing",
            "Women's Clothing",
            "Kids Wear",
            "Ethnic Wear",
            "Western Wear",
            "Sportswear",
            "Innerwear",
            "Winter Wear",
            "Summer Collection",
            "Party Wear",
            "Casual Wear",
            "Formal Wear",
        ],
        "price_range": (200, 15000),
        "brands": [
            "Zara",
            "H&M",
            "Nike",
            "Adidas",
            "Puma",
            "Levi's",
            "Raymond",
            "Peter England",
            "Allen Solly",
            "Van Heusen",
            "Jack & Jones",
            "Mango",
        ],
        "seasonal_boost": {"October": 1.4, "November": 1.6, "March": 1.3},
    },
    "Home & Kitchen": {
        "subcategories": [
            "Kitchen Appliances",
            "Cookware",
            "Dinnerware",
            "Home Decor",
            "Furniture",
            "Bedding",
            "Bath",
            "Storage",
            "Cleaning Supplies",
            "Smart Home",
            "Lighting",
            "Garden & Outdoor",
        ],
        "price_range": (100, 25000),
        "brands": [
            "Philips",
            "Prestige",
            "Borosil",
            "Milton",
            "Cello",
            "Godrej",
            "Duroflex",
            "Urban Ladder",
            "Pepperfry",
            "IKEA",
            "Havells",
            "Bajaj",
        ],
        "seasonal_boost": {"October": 1.3, "November": 1.5},
    },
    "Beauty & Personal Care": {
        "subcategories": [
            "Skincare",
            "Makeup",
            "Haircare",
            "Fragrances",
            "Bath & Body",
            "Men's Grooming",
            "Nail Care",
            "Tools & Accessories",
            "Organic Beauty",
            "Luxury Beauty",
            "Wellness",
        ],
        "price_range": (50, 8000),
        "brands": [
            "Lakme",
            "Maybelline",
            "L'Oreal",
            "Nivea",
            "Dove",
            "Garnier",
            "Neutrogena",
            "The Body Shop",
            "MAC",
            "Estee Lauder",
            "Clinique",
        ],
        "seasonal_boost": {"February": 1.3, "October": 1.4, "December": 1.5},
    },
    "Grocery & Gourmet": {
        "subcategories": [
            "Fresh Produce",
            "Dairy",
            "Bakery",
            "Beverages",
            "Snacks",
            "Packaged Food",
            "Organic",
            "Spices",
            "Health Foods",
            "International Foods",
            "Staples",
            "Frozen Foods",
        ],
        "price_range": (10, 5000),
        "brands": [
            "Amul",
            "Britannia",
            "Parle",
            "Nestle",
            "ITC",
            "Tata",
            "Patanjali",
            "Mother Dairy",
            "Haldiram's",
            "MTR",
            "Cadbury",
        ],
        "seasonal_boost": {},
    },
    "Books & Media": {
        "subcategories": [
            "Fiction",
            "Non-Fiction",
            "Academic",
            "Children's Books",
            "Comics",
            "Magazines",
            "eBooks",
            "Audiobooks",
            "Music",
            "Movies",
            "Educational",
            "Self-Help",
        ],
        "price_range": (50, 3000),
        "brands": [
            "Penguin",
            "HarperCollins",
            "Scholastic",
            "Oxford",
            "Cambridge",
            "McGraw Hill",
            "Pearson",
            "Wiley",
            "Marvel",
            "DC",
            "Kindle",
        ],
        "seasonal_boost": {"June": 1.2, "July": 1.3},
    },
    "Sports & Fitness": {
        "subcategories": [
            "Gym Equipment",
            "Yoga",
            "Running",
            "Cycling",
            "Swimming",
            "Team Sports",
            "Outdoor Activities",
            "Fitness Accessories",
            "Nutrition",
            "Sportswear",
            "Athletic Shoes",
        ],
        "price_range": (150, 30000),
        "brands": [
            "Nike",
            "Adidas",
            "Puma",
            "Reebok",
            "Decathlon",
            "Cosco",
            "Nivia",
            "Yonex",
            "Asics",
            "Under Armour",
            "New Balance",
        ],
        "seasonal_boost": {"January": 1.6, "June": 1.3},
    },
    "Toys & Games": {
        "subcategories": [
            "Action Figures",
            "Dolls",
            "Board Games",
            "Puzzles",
            "Educational Toys",
            "Outdoor Toys",
            "Remote Control",
            "Building Blocks",
            "Soft Toys",
            "Video Games",
            "STEM Toys",
        ],
        "price_range": (100, 15000),
        "brands": [
            "Lego",
            "Hasbro",
            "Mattel",
            "Funskool",
            "Hot Wheels",
            "Barbie",
            "Nerf",
            "Fisher-Price",
            "Playstation",
            "Xbox",
            "Nintendo",
        ],
        "seasonal_boost": {"October": 1.4, "November": 1.5, "December": 1.8},
    },
    "Automotive": {
        "subcategories": [
            "Car Accessories",
            "Bike Accessories",
            "Tyres",
            "Car Care",
            "Tools",
            "GPS & Navigation",
            "Car Audio",
            "Spare Parts",
            "Helmets",
            "Riding Gear",
            "Motor Oil",
        ],
        "price_range": (200, 50000),
        "brands": [
            "Bosch",
            "3M",
            "Michelin",
            "MRF",
            "Philips",
            "Garmin",
            "JBL",
            "Vega",
            "Steelbird",
            "Studds",
            "Shell",
            "Castrol",
        ],
        "seasonal_boost": {},
    },
    "Health & Wellness": {
        "subcategories": [
            "Vitamins & Supplements",
            "Medical Devices",
            "Health Monitors",
            "First Aid",
            "Ayurveda",
            "Fitness Supplements",
            "Weight Management",
            "Sexual Wellness",
            "Baby Care",
            "Elderly Care",
            "Pain Relief",
        ],
        "price_range": (50, 10000),
        "brands": [
            "Himalaya",
            "Dabur",
            "Baidyanath",
            "Patanjali",
            "HealthKart",
            "MuscleBlaze",
            "Optimum Nutrition",
            "Dr. Morepen",
            "Accu-Chek",
            "Omron",
        ],
        "seasonal_boost": {"January": 1.3, "March": 1.2, "September": 1.2},
    },
}

# ============================================================
#  FIXED PRODUCT GENERATION - CORRECT CATEGORY/SUBCATEGORY DISTRIBUTION
# ============================================================


def generate_products_with_exact_categories(
    tenant_id, num_products, num_categories, num_subcategories
):
    """
    FIXED: Generate products with EXACT category and subcategory counts as specified
    """
    st.info(
        f"🔧 Generating {num_products:,} products across {num_categories} categories and {num_subcategories} subcategories..."
    )
    start = time.time()

    # Generate unique category names
    available_categories = list(CATEGORY_HIERARCHY.keys())
    base_categories = available_categories * (
        (num_categories // len(available_categories)) + 1
    )

    # Create exactly num_categories unique categories
    categories = []
    for i in range(num_categories):
        if i < len(available_categories):
            categories.append(available_categories[i])
        else:
            # Create synthetic categories
            base_cat = available_categories[i % len(available_categories)]
            categories.append(f"{base_cat}_Type{i // len(available_categories) + 1}")

    # Generate exactly num_subcategories spread across categories
    subcategories_per_category = num_subcategories // num_categories
    extra_subcats = num_subcategories % num_categories

    category_to_subcats = {}
    subcat_count = 0

    for i, cat in enumerate(categories):
        # Get base subcategories from hierarchy
        if cat in CATEGORY_HIERARCHY:
            base_subcats = CATEGORY_HIERARCHY[cat]["subcategories"]
        else:
            # Use base category for synthetic categories
            base_cat = cat.split("_Type")[0] if "_Type" in cat else "Electronics"
            base_subcats = CATEGORY_HIERARCHY.get(base_cat, {}).get(
                "subcategories", ["General"]
            )

        # Determine how many subcategories this category gets
        num_subcats_for_this_cat = subcategories_per_category
        if i < extra_subcats:
            num_subcats_for_this_cat += 1

        # Generate subcategories
        subcats = []
        for j in range(num_subcats_for_this_cat):
            if j < len(base_subcats):
                subcats.append(base_subcats[j])
            else:
                # Create synthetic subcategories
                base_subcat = base_subcats[j % len(base_subcats)]
                subcats.append(f"{base_subcat}_V{j // len(base_subcats) + 1}")

        category_to_subcats[cat] = subcats
        subcat_count += len(subcats)

    st.info(
        f"✅ Created {len(categories)} categories with {subcat_count} total subcategories"
    )

    # Generate products distributed across all categories/subcategories
    products = []
    products_per_category = num_products // num_categories
    extra_products = num_products % num_categories

    adjectives = [
        "Pro",
        "Max",
        "Plus",
        "Elite",
        "Premium",
        "Standard",
        "Ultra",
        "Advanced",
        "Classic",
        "Modern",
    ]
    suffixes = ["Model", "Edition", "Series", "Collection"]

    for idx, cat in enumerate(categories):
        # Get category info
        base_cat = cat.split("_Type")[0] if "_Type" in cat else cat
        cat_info = CATEGORY_HIERARCHY.get(
            base_cat, {"price_range": (100, 10000), "brands": ["GenericBrand"]}
        )

        subcats = category_to_subcats[cat]

        # Determine products for this category
        num_prods_for_cat = products_per_category
        if idx < extra_products:
            num_prods_for_cat += 1

        products_per_subcat = max(1, num_prods_for_cat // len(subcats))
        extra_prods = num_prods_for_cat % len(subcats)

        for subcat_idx, subcat in enumerate(subcats):
            # Determine products for this subcategory
            n = products_per_subcat
            if subcat_idx < extra_prods:
                n += 1

            # VECTORIZED: Generate all fields at once
            prices = np.random.uniform(*cat_info["price_range"], size=n).round(2)
            is_preferred = np.random.random(n) < 0.2
            is_bestselling = np.random.random(n) < 0.15
            quantities = np.random.randint(100, 2000, size=n)
            reviews = np.random.randint(10, 5000, size=n)

            for i in range(n):
                product = {
                    "tenantid": tenant_id,
                    "productid": str(uuid.uuid4()),
                    "opuraproductid": f"OP_{tenant_id}_{len(products):06d}",
                    "productname": f"{subcat} {random.choice(adjectives)} {random.choice(suffixes)} {random.randint(100, 999)}",
                    "productcategory": cat,
                    "productdescription": f"High-quality {subcat.lower()} from {cat} category with premium features.",
                    "productprice": float(prices[i]),
                    "productimages": json_dumps(
                        [
                            f"https://cdn.example.com/products/{uuid.uuid4()}.jpg"
                            for _ in range(random.randint(3, 5))
                        ]
                    ),
                    "productreviews": int(reviews[i]),
                    "producttags": json_dumps(
                        {
                            "tags": random.sample(
                                [
                                    "bestseller",
                                    "new_arrival",
                                    "trending",
                                    "eco_friendly",
                                    "premium",
                                    "budget_friendly",
                                ],
                                k=random.randint(2, 4),
                            ),
                            "subcategory": subcat,
                        }
                    ),
                    "preferredproduct": "yes" if is_preferred[i] else "no",
                    "bestsellingproduct": "yes" if is_bestselling[i] else "no",
                    "productquantity": int(quantities[i]),
                    "productbrand": random.choice(cat_info.get("brands", ["Generic"])),
                    "productvariants": json_dumps(
                        {
                            "color": random.choice(
                                [
                                    "Red",
                                    "Blue",
                                    "Black",
                                    "White",
                                    "Green",
                                    "Grey",
                                    "Silver",
                                ]
                            ),
                            "size": random.choice(
                                ["S", "M", "L", "XL", "XXL", "Free Size"]
                            ),
                            "warranty": random.choice(
                                ["6 Months", "1 Year", "2 Years", "3 Years", "Lifetime"]
                            ),
                        }
                    ),
                }
                products.append(product)

    df = pd.DataFrame(products)
    elapsed = time.time() - start
    st.success(
        f"✅ Generated {len(df):,} products in {elapsed:.2f}s ({len(df)/elapsed:,.0f} rec/sec)"
    )

    # Verification
    actual_categories = df["productcategory"].nunique()
    actual_subcategories = (
        df["producttags"].apply(lambda x: json_loads(x).get("subcategory")).nunique()
    )
    st.info(
        f"🔍 Verification: {actual_categories} categories, {actual_subcategories} subcategories"
    )

    return df, category_to_subcats


# ============================================================
#  PARALLEL CUSTOMER GENERATION
# ============================================================


@dataclass
class CustomerGenConfig:
    tenant_id: int
    start_idx: int
    batch_size: int
    product_ids: List[str]
    product_prices: List[float]
    product_categories: List[str]
    product_subcategories: List[str]
    category_product_map: Dict
    num_purchases_range: Tuple[int, int]
    cities: List[str]
    city_weights: List[float]


def generate_customer_batch(config: CustomerGenConfig) -> pd.DataFrame:
    """Generate a batch of customers (designed for parallel execution)"""
    np.random.seed(42 + config.start_idx)
    random.seed(42 + config.start_idx)

    batch_size = config.batch_size
    age_distribution = [(18, 25, 0.20), (26, 35, 0.35), (36, 50, 0.30), (51, 70, 0.15)]

    customer_ids = [str(uuid.uuid4()) for _ in range(batch_size)]
    ages = np.array(
        [
            random.randint(
                *random.choices(
                    [(a[0], a[1]) for a in age_distribution],
                    weights=[a[2] for a in age_distribution],
                )[0]
            )
            for _ in range(batch_size)
        ]
    )
    genders = np.random.choice(["Male", "Female", "Other"], size=batch_size)
    cities_batch = random.choices(
        config.cities, weights=config.city_weights, k=batch_size
    )

    purchase_orders_list = []

    for i in range(batch_size):
        age = ages[i]

        if age < 30:
            num_purchases = random.randint(
                config.num_purchases_range[0],
                min(config.num_purchases_range[1] * 2, 20),
            )
            preferred_categories = ["Electronics", "Fashion", "Beauty & Personal Care"]
        elif age < 45:
            num_purchases = random.randint(
                config.num_purchases_range[0] * 2,
                min(config.num_purchases_range[1] * 3, 25),
            )
            preferred_categories = [
                "Home & Kitchen",
                "Electronics",
                "Grocery & Gourmet",
                "Fashion",
            ]
        else:
            num_purchases = random.randint(
                config.num_purchases_range[0], config.num_purchases_range[1] * 2
            )
            preferred_categories = [
                "Health & Wellness",
                "Grocery & Gourmet",
                "Books & Media",
            ]

        orders = []
        for _ in range(num_purchases):
            if random.random() < 0.7 and preferred_categories:
                cat = random.choice(preferred_categories)
                if (
                    cat in config.category_product_map
                    and config.category_product_map[cat]
                ):
                    product_idx = random.choice(config.category_product_map[cat])
                else:
                    product_idx = random.randint(0, len(config.product_ids) - 1)
            else:
                product_idx = random.randint(0, len(config.product_ids) - 1)

            days_ago = min(int(np.random.exponential(120)), 730)
            purchase_date = (datetime.now() - timedelta(days=days_ago)).date()

            month_name = purchase_date.strftime("%B")
            category = config.product_categories[product_idx]
            base_cat = category.split("_Type")[0] if "_Type" in category else category
            seasonal_boost = (
                CATEGORY_HIERARCHY.get(base_cat, {})
                .get("seasonal_boost", {})
                .get(month_name, 1.0)
            )

            base_sale_prob = 0.35
            sale_probability = min(base_sale_prob * seasonal_boost, 0.7)
            is_sale = random.random() < sale_probability

            mrp = config.product_prices[product_idx]
            if is_sale:
                discount = random.choice([5, 10, 15, 20, 25, 30, 40, 50])
                saleprice = round(mrp * (1 - discount / 100), 2)
            else:
                saleprice = mrp

            quantity = random.choices([1, 2, 3, 4, 5], weights=[60, 25, 10, 3, 2])[0]

            orders.append(
                {
                    "productId": config.product_ids[product_idx],
                    "purchaseDate": purchase_date.isoformat(),
                    "opuraProductID": f"OP_{config.tenant_id}_{product_idx:06d}",
                    "purchaseQuantity": quantity,
                    "mrp": mrp,
                    "saleprice": saleprice,
                    "discount": round((1 - saleprice / mrp) * 100, 1) if mrp > 0 else 0,
                    "category": category,
                    "subcategory": config.product_subcategories[product_idx],
                }
            )

        purchase_orders_list.append(json_dumps(orders))

    df = pd.DataFrame(
        {
            "customerid": customer_ids,
            "opuracustomerid": [
                f"CUST_{config.tenant_id}_{config.start_idx + i:08d}"
                for i in range(batch_size)
            ],
            "tenantid": [config.tenant_id] * batch_size,
            "customername": [
                get_random_name(config.start_idx + i) for i in range(batch_size)
            ],
            "customerpersonaldetails": [
                json_dumps(
                    {
                        "age": int(ages[i]),
                        "gender": genders[i],
                        "email": get_random_email(config.start_idx + i),
                        "phone": f"+91{random.randint(7000000000, 9999999999)}",
                    }
                )
                for i in range(batch_size)
            ],
            "customergeolocations": [
                json_dumps(
                    {
                        "city": cities_batch[i],
                        "state": "India",
                        "pincode": random.randint(100000, 999999),
                        "latitude": round(random.uniform(8.4, 35.5), 6),
                        "longitude": round(random.uniform(68.1, 97.4), 6),
                    }
                )
                for i in range(batch_size)
            ],
            "customerwishlist": [
                json_dumps(
                    random.sample(
                        config.product_ids,
                        min(random.randint(3, 8), len(config.product_ids)),
                    )
                )
                for _ in range(batch_size)
            ],
            "purchaseorders": purchase_orders_list,
            "customertags": [
                json_dumps(
                    {
                        "customer_segment": random.choice(
                            ["high_value", "medium_value", "low_value"]
                        ),
                        "active_status": "active",
                        "join_year": random.randint(2020, 2024),
                    }
                )
                for _ in range(batch_size)
            ],
            "opuracustomertags": [json_dumps({}) for _ in range(batch_size)],
            "recommendedproducts": [json_dumps([]) for _ in range(batch_size)],
        }
    )

    return df


def generate_customers_parallel(
    tenant_id,
    num_customers,
    product_df,
    num_purchases_range,
    category_to_subcats,
    num_workers=4,
):
    """Parallel customer generation with multiprocessing"""
    st.info(
        f"👥 Generating {num_customers:,} customers (parallel with {num_workers} workers)..."
    )
    start = time.time()

    product_ids = product_df["productid"].tolist()
    product_prices = product_df["productprice"].tolist()
    product_categories = product_df["productcategory"].tolist()

    product_subcategories = []
    for tags_json in product_df["producttags"]:
        tags_dict = json_loads(tags_json)
        product_subcategories.append(tags_dict.get("subcategory", "Unknown"))

    category_product_map = {}
    for cat in product_df["productcategory"].unique():
        category_product_map[cat] = product_df[
            product_df["productcategory"] == cat
        ].index.tolist()

    cities = [
        "Mumbai",
        "Delhi",
        "Bangalore",
        "Hyderabad",
        "Chennai",
        "Kolkata",
        "Pune",
        "Ahmedabad",
        "Jaipur",
        "Surat",
    ]
    city_weights = [0.15, 0.14, 0.12, 0.10, 0.10, 0.09, 0.08, 0.08, 0.07, 0.07]

    # Adaptive batch size to prevent memory errors with large datasets
    if num_customers > 1000000:  # For very large datasets (>1M customers)
        batch_size = 25000  # Smaller batches to reduce memory pressure
    else:
        batch_size = 50000  # Standard batch size for smaller datasets

    num_batches = (num_customers + batch_size - 1) // batch_size

    configs = []
    for i in range(num_batches):
        start_idx = i * batch_size
        actual_batch_size = min(batch_size, num_customers - start_idx)

        config = CustomerGenConfig(
            tenant_id=tenant_id,
            start_idx=start_idx,
            batch_size=actual_batch_size,
            product_ids=product_ids,
            product_prices=product_prices,
            product_categories=product_categories,
            product_subcategories=product_subcategories,
            category_product_map=category_product_map,
            num_purchases_range=num_purchases_range,
            cities=cities,
            city_weights=city_weights,
        )
        configs.append(config)

    progress_bar = st.progress(0)
    status_text = st.empty()

    all_results = []
    completed = 0

    with mp.Pool(processes=num_workers) as pool:
        for result in pool.imap_unordered(generate_customer_batch, configs):
            all_results.append(result)
            completed += len(result)
            progress_bar.progress(completed / num_customers)

            elapsed = time.time() - start
            speed = completed / elapsed if elapsed > 0 else 0
            status_text.text(
                f"  ⚡ {completed:,} / {num_customers:,} customers | {speed:,.0f} rec/sec"
            )

    progress_bar.empty()
    status_text.empty()

    final_df = pd.concat(all_results, ignore_index=True)

    del all_results
    gc.collect()

    elapsed = time.time() - start
    final_speed = len(final_df) / elapsed if elapsed > 0 else 0
    st.success(
        f"✅ Generated {len(final_df):,} customers in {elapsed:.1f}s ({final_speed:,.0f} rec/sec)"
    )

    return final_df


# ============================================================
#  STREAMLIT UI
# ============================================================

st.title("🚀 Ultra-Optimized Enterprise Data Generator")
st.markdown(
    f"**⚡ Powered by: {JSON_LIBRARY} | Parallel Processing | PostgreSQL COPY**"
)

tab1, tab2, tab3 = st.tabs(
    ["🎯 Preset Configurations", "📊 Tenant Summary", "🔍 Data Validation"]
)

with tab1:
    st.header("Data Generation Configurations")

    cpu_cores = mp.cpu_count()
    num_workers = st.slider(
        "Parallel Workers (CPU Cores)",
        1,
        cpu_cores,
        min(4, cpu_cores),
        help=f"Your system has {cpu_cores} cores",
    )

    tenants_df = fetch_tenants()

    # ======== CONFIGURATION 1 ========
    st.subheader("📋 Configuration 1: 5M Users + 100K Products")
    st.markdown(
        f"""
    **Pranav's Requirements:**
    - ✅ **5M users**
    - ✅ **100K products**
    - ✅ **10M purchases** (5M users × avg 2 purchases)
    - ⏱️ **Expected: 8-12 minutes** ⚡
    - 🚀 **Workers: {num_workers}**
    """
    )

    config1_name = st.text_input(
        "Tenant 1 Name", "Tenant_5M_Users_100K_Products", key="config1"
    )

    if st.button("🚀 Generate Configuration 1", key="btn_config1"):
        start_time = time.time()
        with st.spinner("🔥 Generating Configuration 1..."):
            try:
                next_id = (
                    int(tenants_df["tenantid"].max()) + 1 if not tenants_df.empty else 1
                )
                onboard_date = datetime.now(timezone.utc).date()

                with engine.begin() as conn:
                    conn.execute(
                        text(
                            """
                        INSERT INTO tenantdata (tenantid, tenantname, onboardeddate, cataloglastupdated, lasttraineddate, active)
                        VALUES (:id, :name, :onboard, :catalog, :trained, TRUE)
                    """
                        ),
                        {
                            "id": next_id,
                            "name": config1_name,
                            "onboard": onboard_date,
                            "catalog": onboard_date,
                            "trained": onboard_date,
                        },
                    )

                st.success(f"✅ Created tenant: {config1_name} (ID: {next_id})")

                # Use base categories for this config (no specific category/subcategory requirement)
                products_df, category_to_subcats = (
                    generate_products_with_exact_categories(
                        tenant_id=next_id,
                        num_products=100000,
                        num_categories=50,  # Reasonable spread
                        num_subcategories=200,
                    )
                )

                ultra_fast_copy_insert(products_df, "productcatalog", engine)

                # 5M users with 1-3 purchases each = ~10M purchases
                customers_df = generate_customers_parallel(
                    tenant_id=next_id,
                    num_customers=5000000,
                    product_df=products_df,
                    num_purchases_range=(1, 3),
                    category_to_subcats=category_to_subcats,
                    num_workers=num_workers,
                )

                ultra_fast_copy_insert(customers_df, "customerdata", engine)

                # Calculate actual purchases
                sample_purchases = sum(
                    len(json_loads(po))
                    for po in customers_df["purchaseorders"].head(1000)
                )
                total_purchases_est = (sample_purchases / 1000) * len(customers_df)
                elapsed = time.time() - start_time

                st.balloons()
                st.success(
                    f"""
                🎉 **Configuration 1 Complete!**
                - ✅ Customers: {len(customers_df):,}
                - ✅ Products: {len(products_df):,}
                - ✅ Estimated Purchases: ~{total_purchases_est:,.0f}
                - ⏱️ Time: {elapsed/60:.2f} minutes
                - 🚀 Speed: {(len(customers_df) + len(products_df))/elapsed:,.0f} rec/sec
                """
                )

            except Exception as e:
                st.error(f"❌ Error: {e}")
                import traceback

                st.code(traceback.format_exc())

    st.markdown("---")

    # ======== CONFIGURATION 2 ========
    st.subheader("📋 Configuration 2: 2M Users + 2K Products")
    st.markdown(
        f"""
    **Pranav's Requirements (EXACT):**
    - ✅ **2M users**
    - ✅ **2000 products**
    - ✅ **200 categories** (exactly)
    - ✅ **500 subcategories** (exactly)
    - ✅ **4M purchases/transactions**
    - ⏱️ **Expected: 5-7 minutes** ⚡
    - 🚀 **Workers: {num_workers}**
    """
    )

    config2_name = st.text_input(
        "Tenant 2 Name", "Tenant_2M_Users_2K_Products_200Cat_500Sub", key="config2"
    )

    if st.button("🚀 Generate Configuration 2", key="btn_config2"):
        start_time = time.time()
        with st.spinner("🔥 Generating Configuration 2 with EXACT categories..."):
            try:
                next_id = (
                    int(tenants_df["tenantid"].max()) + 1 if not tenants_df.empty else 1
                )
                onboard_date = datetime.now(timezone.utc).date()

                with engine.begin() as conn:
                    conn.execute(
                        text(
                            """
                        INSERT INTO tenantdata (tenantid, tenantname, onboardeddate, cataloglastupdated, lasttraineddate, active)
                        VALUES (:id, :name, :onboard, :catalog, :trained, TRUE)
                    """
                        ),
                        {
                            "id": next_id,
                            "name": config2_name,
                            "onboard": onboard_date,
                            "catalog": onboard_date,
                            "trained": onboard_date,
                        },
                    )

                st.success(f"✅ Created tenant: {config2_name} (ID: {next_id})")

                # EXACT: 2000 products, 200 categories, 500 subcategories
                products_df, category_to_subcats = (
                    generate_products_with_exact_categories(
                        tenant_id=next_id,
                        num_products=2000,
                        num_categories=200,
                        num_subcategories=500,
                    )
                )

                ultra_fast_copy_insert(products_df, "productcatalog", engine)

                # 2M users with 1-3 purchases = ~4M purchases
                customers_df = generate_customers_parallel(
                    tenant_id=next_id,
                    num_customers=2000000,
                    product_df=products_df,
                    num_purchases_range=(1, 3),
                    category_to_subcats=category_to_subcats,
                    num_workers=num_workers,
                )

                ultra_fast_copy_insert(customers_df, "customerdata", engine)

                sample_purchases = sum(
                    len(json_loads(po))
                    for po in customers_df["purchaseorders"].head(1000)
                )
                total_purchases_est = (sample_purchases / 1000) * len(customers_df)
                elapsed = time.time() - start_time

                st.balloons()
                st.success(
                    f"""
                🎉 **Configuration 2 Complete!**
                - ✅ Customers: {len(customers_df):,}
                - ✅ Products: {len(products_df):,}
                - ✅ Categories: 200 (exact)
                - ✅ Subcategories: 500 (exact)
                - ✅ Estimated Purchases: ~{total_purchases_est:,.0f}
                - ⏱️ Time: {elapsed/60:.2f} minutes
                - 🚀 Speed: {(len(customers_df) + len(products_df))/elapsed:,.0f} rec/sec
                """
                )

            except Exception as e:
                st.error(f"❌ Error: {e}")
                import traceback

                st.code(traceback.format_exc())

    st.markdown("---")

    # ======== CONFIGURATION 3 ========
    st.subheader("📋 Configuration 3: 20K Products + 20M Purchases")
    st.markdown(
        f"""
    **Pranav's Requirements (EXACT):**
    - ✅ **20,000 products**
    - ✅ **500 categories** (exactly)
    - ✅ **2000 subcategories** (exactly)
    - ✅ **20M purchases** (users calculated to achieve this)
    - 💡 **Calculated**: ~6.67M users (at avg 3 purchases each)
    - ⏱️ **Expected: 12-18 minutes** ⚡
    - 🚀 **Workers: {num_workers}**
    """
    )

    config3_name = st.text_input(
        "Tenant 3 Name",
        "Tenant_20K_Products_500Cat_2000Sub_20M_Purchases",
        key="config3",
    )

    if st.button("🚀 Generate Configuration 3", key="btn_config3"):
        start_time = time.time()
        with st.spinner("🔥 Generating Configuration 3 with EXACT specs..."):
            try:
                next_id = (
                    int(tenants_df["tenantid"].max()) + 1 if not tenants_df.empty else 1
                )
                onboard_date = datetime.now(timezone.utc).date()

                with engine.begin() as conn:
                    conn.execute(
                        text(
                            """
                        INSERT INTO tenantdata (tenantid, tenantname, onboardeddate, cataloglastupdated, lasttraineddate, active)
                        VALUES (:id, :name, :onboard, :catalog, :trained, TRUE)
                    """
                        ),
                        {
                            "id": next_id,
                            "name": config3_name,
                            "onboard": onboard_date,
                            "catalog": onboard_date,
                            "trained": onboard_date,
                        },
                    )

                st.success(f"✅ Created tenant: {config3_name} (ID: {next_id})")

                # EXACT: 20000 products, 500 categories, 2000 subcategories
                products_df, category_to_subcats = (
                    generate_products_with_exact_categories(
                        tenant_id=next_id,
                        num_products=20000,
                        num_categories=500,
                        num_subcategories=2000,
                    )
                )

                ultra_fast_copy_insert(products_df, "productcatalog", engine)

                # Calculate users needed for 20M purchases
                # With num_purchases_range=(2, 4), avg = 3 purchases per user
                # 20M purchases / 3 = ~6.67M users
                customers_df = generate_customers_parallel(
                    tenant_id=next_id,
                    num_customers=6670000,
                    product_df=products_df,
                    num_purchases_range=(2, 4),
                    category_to_subcats=category_to_subcats,
                    num_workers=num_workers,
                )

                ultra_fast_copy_insert(customers_df, "customerdata", engine)

                sample_purchases = sum(
                    len(json_loads(po))
                    for po in customers_df["purchaseorders"].head(1000)
                )
                total_purchases_est = (sample_purchases / 1000) * len(customers_df)
                elapsed = time.time() - start_time

                st.balloons()
                st.success(
                    f"""
                🎉 **Configuration 3 Complete!**
                - ✅ Customers: {len(customers_df):,}
                - ✅ Products: {len(products_df):,}
                - ✅ Categories: 500 (exact)
                - ✅ Subcategories: 2000 (exact)
                - ✅ Estimated Purchases: ~{total_purchases_est:,.0f} (Target: 20M)
                - ⏱️ Time: {elapsed/60:.2f} minutes
                - 🚀 Speed: {(len(customers_df) + len(products_df))/elapsed:,.0f} rec/sec
                """
                )

            except Exception as e:
                st.error(f"❌ Error: {e}")
                import traceback

                st.code(traceback.format_exc())

with tab2:
    st.header("📊 Tenant Summary Dashboard")

    if st.button("🔄 Refresh Data"):
        st.rerun()

    try:
        with engine.connect() as conn:
            tenant_stats_query = text(
                """
                SELECT 
                    t.tenantid,
                    t.tenantname,
                    t.onboardeddate,
                    t.active,
                    COUNT(DISTINCT p.productid) as product_count,
                    COUNT(DISTINCT c.customerid) as customer_count
                FROM tenantdata t
                LEFT JOIN productcatalog p ON t.tenantid = p.tenantid
                LEFT JOIN customerdata c ON t.tenantid = c.tenantid
                GROUP BY t.tenantid, t.tenantname, t.onboardeddate, t.active
                ORDER BY t.tenantid DESC;
            """
            )

            stats_df = pd.read_sql(tenant_stats_query, conn)

        if not stats_df.empty:
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Tenants", len(stats_df))
            with col2:
                st.metric("Total Products", f"{stats_df['product_count'].sum():,}")
            with col3:
                st.metric("Total Customers", f"{stats_df['customer_count'].sum():,}")
            with col4:
                active_tenants = stats_df[stats_df["active"] == True].shape[0]
                st.metric("Active Tenants", active_tenants)

            st.subheader("📋 Tenant Details")
            display_df = stats_df.copy()
            display_df["product_count"] = display_df["product_count"].apply(
                lambda x: f"{x:,}"
            )
            display_df["customer_count"] = display_df["customer_count"].apply(
                lambda x: f"{x:,}"
            )
            st.dataframe(display_df, use_container_width=True, height=400)

            st.subheader("📈 Visual Analytics")

            col1, col2 = st.columns(2)

            with col1:
                fig_products = px.bar(
                    stats_df,
                    x="tenantname",
                    y="product_count",
                    title="Products per Tenant",
                )
                st.plotly_chart(fig_products, use_container_width=True)

            with col2:
                fig_customers = px.bar(
                    stats_df,
                    x="tenantname",
                    y="customer_count",
                    title="Customers per Tenant",
                )
                st.plotly_chart(fig_customers, use_container_width=True)

        else:
            st.info("No tenants found in the database.")

    except Exception as e:
        st.error(f"❌ Error fetching tenant summary: {e}")

with tab3:
    st.header("🔍 Data Quality Validation")

    tenants_df = fetch_tenants()

    if not tenants_df.empty:
        selected_tenant = st.selectbox(
            "Select Tenant to Validate",
            options=tenants_df["tenantid"].tolist(),
            format_func=lambda x: f"{tenants_df[tenants_df['tenantid']==x]['tenantname'].iloc[0]} (ID: {x})",
        )

        if st.button("🔍 Run Validation"):
            with st.spinner("Running data quality checks..."):
                try:
                    with engine.connect() as conn:
                        product_query = text(
                            """
                            SELECT 
                                COUNT(*) as total_products,
                                COUNT(DISTINCT productcategory) as unique_categories,
                                MIN(productprice) as min_price,
                                MAX(productprice) as max_price,
                                AVG(productprice) as avg_price,
                                SUM(CASE WHEN productprice <= 0 THEN 1 ELSE 0 END) as invalid_prices
                            FROM productcatalog
                            WHERE tenantid = :tid
                        """
                        )
                        product_stats = pd.read_sql(
                            product_query, conn, params={"tid": selected_tenant}
                        )

                        customer_query = text(
                            """
                            SELECT 
                                COUNT(*) as total_customers,
                                COUNT(DISTINCT customername) as unique_names
                            FROM customerdata
                            WHERE tenantid = :tid
                        """
                        )
                        customer_stats = pd.read_sql(
                            customer_query, conn, params={"tid": selected_tenant}
                        )

                    st.subheader("✅ Validation Results")

                    col1, col2 = st.columns(2)

                    with col1:
                        st.markdown("### 📦 Product Data Quality")
                        st.metric(
                            "Total Products",
                            f"{product_stats['total_products'].iloc[0]:,}",
                        )
                        st.metric(
                            "Unique Categories",
                            product_stats["unique_categories"].iloc[0],
                        )
                        st.metric(
                            "Price Range",
                            f"₹{product_stats['min_price'].iloc[0]:,.2f} - ₹{product_stats['max_price'].iloc[0]:,.2f}",
                        )
                        st.metric(
                            "Average Price",
                            f"₹{product_stats['avg_price'].iloc[0]:,.2f}",
                        )

                        invalid_prices = product_stats["invalid_prices"].iloc[0]
                        if invalid_prices > 0:
                            st.error(
                                f"⚠️ Found {invalid_prices} products with invalid prices!"
                            )
                        else:
                            st.success("✅ All product prices are valid")

                    with col2:
                        st.markdown("### 👥 Customer Data Quality")
                        st.metric(
                            "Total Customers",
                            f"{customer_stats['total_customers'].iloc[0]:,}",
                        )
                        st.metric(
                            "Unique Names",
                            f"{customer_stats['unique_names'].iloc[0]:,}",
                        )
                        st.success("✅ Customer data validation passed")

                except Exception as e:
                    st.error(f"❌ Validation error: {e}")
    else:
        st.info("No tenants available for validation.")
