-- Sample dimension and fact data
-- Run after 01_schema.sql: psql -d ecommerce_dw -f sql/02_seed_data.sql

-- Date dimension
INSERT INTO dim_date (date_id, day, month, quarter, year)
VALUES
    ('2026-05-01', 1, 5, 2, 2026),
    ('2026-05-02', 2, 5, 2, 2026);

-- Customers
INSERT INTO dim_customer (name, email, country, city)
VALUES
    ('Alice', 'alice@email.com', 'USA', 'New York'),
    ('Bob', 'bob@email.com', 'USA', 'Los Angeles');

-- Products
INSERT INTO dim_product (product_name, category, brand, price)
VALUES
    ('Laptop', 'Electronics', 'BrandA', 1000),
    ('Mouse', 'Electronics', 'BrandB', 25);

-- Fact table
INSERT INTO fact_sales (order_date, customer_id, product_id, quantity, price, total_amount)
VALUES
    ('2026-05-01', 1, 1, 1, 1000, 1000),
    ('2026-05-01', 2, 2, 2, 25, 50);
