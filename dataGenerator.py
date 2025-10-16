import pandas as pd
import numpy as np
from faker import Faker
import random, uuid, psycopg2, json
from datetime import timedelta
from tqdm import tqdm
from decimal import Decimal

# ---------------------------------
# SAFE JSON SERIALIZER
# ---------------------------------
def safe_json_dumps(data):
    """Converts Decimals and other non-serializable types to serializable ones."""
    def convert(o):
        if isinstance(o, Decimal):
            return float(o)
        if isinstance(o, (set, np.ndarray)):
            return list(o)
        raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")
    return json.dumps(data, default=convert)

# ---------------------------------
# CONFIG
# ---------------------------------
fake = Faker()
Faker.seed(42)
np.random.seed(42)

DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": 55000
}

NUM_TENANTS = 5
NUM_PRODUCTS = 200
NUM_CUSTOMERS = 100_000
NUM_CATEGORIES = 20

CATEGORIES = [
    "Electronics", "Home Appliances", "Books", "Toys", "Fashion", "Footwear",
    "Beauty", "Health", "Sports", "Grocery", "Furniture", "Stationery",
    "Gaming", "Automotive", "Jewellery", "Pet Supplies", "Music",
    "Garden", "Kitchenware", "Accessories"
]

BRANDS = ["Sony", "Apple", "Samsung", "Nike", "Adidas", "LG", "Philips",
          "Dell", "HP", "Lenovo", "Asus", "Mi", "Boat", "Zara", "Puma"]

# ---------------------------------
# DB CONNECTION
# ---------------------------------
conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()

# ---------------------------------
# CREATE TABLES
# ---------------------------------
cursor.execute("""
CREATE TABLE IF NOT EXISTS TenantData (
    TenantId SERIAL PRIMARY KEY,
    TenantName VARCHAR(255),
    OnboardedDate DATE,
    CatalogLastUpdated DATE,
    LastTrainedDate DATE,
    Active BOOLEAN
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS ProductCatalog (
    TenantId INT REFERENCES TenantData(TenantId),
    ProductId UUID PRIMARY KEY,
    OpuraProductId VARCHAR(50),
    ProductName VARCHAR(255),
    ProductCategory VARCHAR(100),
    ProductDescription TEXT,
    ProductPrice FLOAT,
    ProductImages JSONB,
    ProductReviews INT,
    ProductTags JSONB,
    PreferredProduct VARCHAR(255),
    BestSellingProduct VARCHAR(255),
    ProductQuantity INT,
    ProductBrand VARCHAR(100),
    ProductVariants JSONB
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS CustomerData (
    CustomerId UUID PRIMARY KEY,
    OpuraCustomerId VARCHAR(50),
    TenantId INT REFERENCES TenantData(TenantId),
    CustomerName VARCHAR(255),
    CustomerPersonalDetails JSONB,
    CustomerGeolocations JSONB,
    CustomerWishlist JSONB,
    PurchaseOrders JSONB,
    CustomerTags JSONB,
    OpuraCustomerTags JSONB,
    RecommendedProducts JSONB
);
""")

conn.commit()

# ---------------------------------
# INSERT TENANT DATA
# ---------------------------------
tenant_data = []
for i in range(1, NUM_TENANTS + 1):
    onboard_date = fake.date_between(start_date='-3y', end_date='-1y')
    tenant_data.append({
        "TenantId": i,
        "TenantName": fake.company(),
        "OnboardedDate": str(onboard_date),
        "CatalogLastUpdated": str(onboard_date + timedelta(days=random.randint(30, 300))),
        "LastTrainedDate": str(onboard_date + timedelta(days=random.randint(60, 500))),
        "Active": random.choice([True, True, True, False])
    })

for tenant in tenant_data:
    cursor.execute("""
    INSERT INTO TenantData (TenantId, TenantName, OnboardedDate, CatalogLastUpdated, LastTrainedDate, Active)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (TenantId) DO NOTHING;
    """, (
        tenant["TenantId"], tenant["TenantName"], tenant["OnboardedDate"],
        tenant["CatalogLastUpdated"], tenant["LastTrainedDate"], tenant["Active"]
    ))

conn.commit()

# ---------------------------------
# INSERT PRODUCT CATALOG
# ---------------------------------
product_data = []
for _ in range(NUM_PRODUCTS):
    tenant_id = random.randint(1, NUM_TENANTS)
    category = random.choice(CATEGORIES)
    base_price = {
        "Electronics": (200, 2000),
        "Home Appliances": (150, 1500),
        "Fashion": (20, 300),
        "Books": (5, 80),
        "Toys": (10, 150),
        "Grocery": (2, 50),
        "Furniture": (100, 1000),
        "Beauty": (10, 100),
        "Sports": (30, 400),
        "Automotive": (50, 800)
    }.get(category, (10, 200))
    price = round(random.uniform(*base_price), 2)

    p = {
        "TenantId": tenant_id,
        "ProductId": str(uuid.uuid4()),
        "OpuraProductId": f"OP_{tenant_id}_{random.randint(1000,9999)}",
        "ProductName": fake.word().capitalize() + " " + random.choice(["Pro", "Max", "Plus", "Lite"]),
        "ProductCategory": category,
        "ProductDescription": fake.sentence(nb_words=10),
        "ProductPrice": price,
        "ProductImages": [fake.image_url() for _ in range(random.randint(1, 3))],
        "ProductReviews": random.randint(0, 5000),
        "ProductTags": random.sample(["eco", "popular", "discount", "premium", "new"], random.randint(1, 3)),
        "PreferredProduct": None,
        "BestSellingProduct": None,
        "ProductQuantity": random.randint(10, 500),
        "ProductBrand": random.choice(BRANDS),
        "ProductVariants": {
            "size": random.choice(["S", "M", "L", "XL", "NA"]),
            "watts": random.choice(["10W", "20W", "50W", "NA"]),
            "color": random.choice(["Red", "Blue", "Green", "Black", "White", "NA"])
        }
    }
    product_data.append(p)

for p in product_data:
    cursor.execute("""
    INSERT INTO ProductCatalog (
        TenantId, ProductId, OpuraProductId, ProductName, ProductCategory, ProductDescription,
        ProductPrice, ProductImages, ProductReviews, ProductTags,
        PreferredProduct, BestSellingProduct, ProductQuantity,
        ProductBrand, ProductVariants
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        p["TenantId"], p["ProductId"], p["OpuraProductId"], p["ProductName"], p["ProductCategory"],
        p["ProductDescription"], p["ProductPrice"], safe_json_dumps(p["ProductImages"]),
        p["ProductReviews"], safe_json_dumps(p["ProductTags"]),
        p["PreferredProduct"], p["BestSellingProduct"], p["ProductQuantity"],
        p["ProductBrand"], safe_json_dumps(p["ProductVariants"])
    ))

conn.commit()

# Cache product IDs by tenant
product_ids_by_tenant = {}
cursor.execute("SELECT TenantId, ProductId FROM ProductCatalog;")
for row in cursor.fetchall():
    product_ids_by_tenant.setdefault(row[0], []).append(row[1])

# ---------------------------------
# INSERT CUSTOMER DATA
# ---------------------------------
for _ in tqdm(range(NUM_CUSTOMERS), desc="Generating and inserting customers"):
    tenant_id = random.randint(1, NUM_TENANTS)
    age = random.randint(18, 75)
    gender = random.choice(["Male", "Female", "Other"])

    if age < 25:
        preferred_categories = ["Electronics", "Gaming", "Fashion"]
    elif age < 40:
        preferred_categories = ["Electronics", "Home Appliances", "Sports", "Fashion"]
    elif age < 55:
        preferred_categories = ["Home Appliances", "Furniture", "Books"]
    else:
        preferred_categories = ["Health", "Grocery", "Books", "Automotive"]

    purchase_orders = [{
        "productId": random.choice(product_ids_by_tenant[tenant_id]),
        "purchaseDate": str(fake.date_between(start_date='-2y', end_date='today')),
        "purchaseQuantity": random.randint(1, 5),
        "opuraProductID": f"OP_{tenant_id}_{random.randint(1000,9999)}"
    } for _ in range(random.randint(1, 12))]

    customer = {
        "CustomerId": str(uuid.uuid4()),
        "OpuraCustomerId": f"CUST_{tenant_id}_{random.randint(10000,99999)}",
        "TenantId": tenant_id,
        "CustomerName": fake.name(),
        "CustomerPersonalDetails": {
            "age": age, "gender": gender, "email": fake.email(), "phone": fake.phone_number()
        },
        "CustomerGeolocations": {
            "city": fake.city(),
            "country": fake.country(),
            "latitude": float(fake.latitude()),
            "longitude": float(fake.longitude())
        },
        "CustomerWishlist": random.sample(product_ids_by_tenant[tenant_id], random.randint(1, 5)),
        "PurchaseOrders": purchase_orders,
        "CustomerTags": {"PrimeCustomer": random.choice([True, False])},
        "OpuraCustomerTags": {
            "OrderFrequency": len(purchase_orders),
            "CategoryFrequency": len(set(preferred_categories)),
            "AvgPurchaseValue6M": round(random.uniform(50, 5000), 2),
            "AvgPurchaseValue3M": round(random.uniform(20, 3000), 2),
            "purchaseValuePerCat3M": round(random.uniform(20, 2000), 2),
            "purchaseValuePerCat6M": round(random.uniform(30, 2500), 2)
        },
        "RecommendedProducts": [{
            "ProductCategory": random.choice(preferred_categories),
            "OpuraProductId": f"OP_{tenant_id}_{random.randint(1000,9999)}",
            "RecommendedDate": str(fake.date_this_year()),
            "RecommendationAccepted": random.choice([True, False]),
            "RecommendationAcceptationRate": round(random.uniform(0, 1), 2)
        }]
    }

    cursor.execute("""
    INSERT INTO CustomerData (
        CustomerId, OpuraCustomerId, TenantId, CustomerName,
        CustomerPersonalDetails, CustomerGeolocations, CustomerWishlist,
        PurchaseOrders, CustomerTags, OpuraCustomerTags, RecommendedProducts
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        customer["CustomerId"], customer["OpuraCustomerId"], customer["TenantId"], customer["CustomerName"],
        safe_json_dumps(customer["CustomerPersonalDetails"]),
        safe_json_dumps(customer["CustomerGeolocations"]),
        safe_json_dumps(customer["CustomerWishlist"]),
        safe_json_dumps(customer["PurchaseOrders"]),
        safe_json_dumps(customer["CustomerTags"]),
        safe_json_dumps(customer["OpuraCustomerTags"]),
        safe_json_dumps(customer["RecommendedProducts"])
    ))

conn.commit()
cursor.close()
conn.close()

print("\n✅ All data generated and inserted into PostgreSQL successfully!")