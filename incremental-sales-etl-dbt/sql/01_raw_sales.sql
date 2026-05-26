-- Raw sales landing table (run before dbt)
-- psql -d sales_dw -f sql/01_raw_sales.sql

CREATE SCHEMA IF NOT EXISTS raw;

DROP TABLE IF EXISTS raw.sales_raw;

CREATE TABLE raw.sales_raw (
    order_id SERIAL PRIMARY KEY,
    order_date DATE,
    customer_id INT,
    product_id INT,
    quantity INT,
    price NUMERIC,
    created_at TIMESTAMP DEFAULT now()
);

INSERT INTO raw.sales_raw (order_date, customer_id, product_id, quantity, price)
VALUES
    ('2026-05-01', 101, 1, 2, 10.50),
    ('2026-05-01', 102, 2, 1, 25.00),
    ('2026-05-02', 103, 1, 3, 10.50);
