-- E-commerce star schema: dimensions and fact
-- Run: psql -d ecommerce_dw -f sql/01_schema.sql

DROP TABLE IF EXISTS fact_sales CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;

-- Dimensions
CREATE TABLE dim_date (
    date_id DATE PRIMARY KEY,
    day INT NOT NULL,
    month INT NOT NULL,
    quarter INT NOT NULL,
    year INT NOT NULL
);

CREATE TABLE dim_customer (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(50),
    email VARCHAR(50),
    country VARCHAR(50),
    city VARCHAR(50)
);

CREATE TABLE dim_product (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(50),
    category VARCHAR(50),
    brand VARCHAR(50),
    price NUMERIC(12, 2)
);

-- Fact
CREATE TABLE fact_sales (
    order_id SERIAL PRIMARY KEY,
    order_date DATE NOT NULL REFERENCES dim_date(date_id),
    customer_id INT NOT NULL REFERENCES dim_customer(customer_id),
    product_id INT NOT NULL REFERENCES dim_product(product_id),
    quantity INT NOT NULL CHECK (quantity > 0),
    price NUMERIC(12, 2) NOT NULL,
    total_amount NUMERIC(12, 2) NOT NULL
);

CREATE INDEX idx_fact_sales_order_date ON fact_sales(order_date);
CREATE INDEX idx_fact_sales_customer_id ON fact_sales(customer_id);
CREATE INDEX idx_fact_sales_product_id ON fact_sales(product_id);
