-- Sales by Product Category Mart
-- Category-level revenue and share of total sales

WITH category_totals AS (
    SELECT
        p.category,
        SUM(f.quantity) AS units_sold,
        SUM(f.total_amount) AS category_revenue,
        COUNT(DISTINCT f.order_id) AS order_lines,
        COUNT(DISTINCT f.customer_id) AS unique_customers
    FROM fact_sales f
    JOIN dim_product p ON f.product_id = p.product_id
    GROUP BY p.category
),
grand_total AS (
    SELECT SUM(category_revenue) AS total_revenue
    FROM category_totals
)
SELECT
    ct.category,
    ct.units_sold,
    ct.category_revenue,
    ROUND(100.0 * ct.category_revenue / gt.total_revenue, 2) AS revenue_pct,
    ct.order_lines,
    ct.unique_customers
FROM category_totals ct
CROSS JOIN grand_total gt
ORDER BY ct.category_revenue DESC;

-- Total sales by region (customer country)
SELECT
    c.country AS region,
    SUM(f.total_amount) AS total_sales,
    SUM(f.quantity) AS units_sold,
    COUNT(DISTINCT f.customer_id) AS unique_customers
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.country
ORDER BY total_sales DESC;
