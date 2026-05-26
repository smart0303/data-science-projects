-- Optional: simulate a later batch load.
-- PREREQUISITE: run sql/01_raw_sales.sql first (creates raw.sales_raw + seed data).
-- Then run dbt run once before this script.
--
-- psql -h localhost -U postgres -d sales_dw -f sql/01_raw_sales.sql
-- dbt run
-- psql -h localhost -U postgres -d sales_dw -f sql/02_incremental_load.sql

INSERT INTO raw.sales_raw (order_date, customer_id, product_id, quantity, price)
VALUES
    ('2026-05-03', 104, 3, 1, 49.99),
    ('2026-05-03', 101, 2, 2, 25.00);
