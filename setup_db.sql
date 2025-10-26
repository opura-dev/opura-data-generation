-- Create database
CREATE DATABASE testdb;

-- Connect to the database
\c testdb

-- Create the three required tables

-- Table 1: tenantdata
CREATE TABLE tenantdata (
    tenantid SERIAL PRIMARY KEY,
    tenantname VARCHAR(255) NOT NULL,
    onboardeddate DATE,
    cataloglastupdated DATE,
    lasttraineddate DATE,
    active BOOLEAN DEFAULT TRUE
);

-- Table 2: productcatalog
CREATE TABLE productcatalog (
    productid VARCHAR(255) PRIMARY KEY,
    tenantid INTEGER REFERENCES tenantdata(tenantid),
    opuraproductid VARCHAR(255),
    productname VARCHAR(500),
    productcategory VARCHAR(255),
    productdescription TEXT,
    productprice NUMERIC(10, 2),
    productimages TEXT,
    productreviews INTEGER,
    producttags TEXT,
    preferredproduct VARCHAR(10),
    bestsellingproduct VARCHAR(10),
    productquantity INTEGER,
    productbrand VARCHAR(255),
    productvariants TEXT
);

-- Table 3: customerdata
CREATE TABLE customerdata (
    customerid VARCHAR(255) PRIMARY KEY,
    opuracustomerid VARCHAR(255),
    tenantid INTEGER REFERENCES tenantdata(tenantid),
    customername VARCHAR(255),
    customerpersonaldetails TEXT,
    customergeolocations TEXT,
    customerwishlist TEXT,
    purchaseorders TEXT,
    customertags TEXT,
    opuracustomertags TEXT,
    recommendedproducts TEXT
);

-- Create indexes for better performance
CREATE INDEX idx_product_tenant ON productcatalog(tenantid);
CREATE INDEX idx_customer_tenant ON customerdata(tenantid);
CREATE INDEX idx_product_category ON productcatalog(productcategory);
