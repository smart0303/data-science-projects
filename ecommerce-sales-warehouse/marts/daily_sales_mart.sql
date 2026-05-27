-- Daily Sales Mart
-- Daily revenue by product, category, customer, and region (country)

SELECT
    d.date_id AS order_date,
    d.year,
    d.quarter,
    d.month,
    c.customer_id,
    c.name AS customer_name,
    c.country AS region,
    c.city,
    p.product_id,
    p.product_name,
    p.category,
    p.brand,
    SUM(f.quantity) AS units_sold,
    SUM(f.total_amount) AS daily_revenue,
    COUNT(DISTINCT f.order_id) AS order_lines
FROM fact_sales f
JOIN dim_date d ON f.order_date = d.date_id
JOIN dim_customer c ON f.customer_id = c.customer_id
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY
    d.date_id,
    d.year,
    d.quarter,
    d.month,
    c.customer_id,
    c.name,
    c.country,
    c.city,
    p.product_id,
    p.product_name,
    p.category,
    p.brand
ORDER BY order_date, daily_revenue DESC;
