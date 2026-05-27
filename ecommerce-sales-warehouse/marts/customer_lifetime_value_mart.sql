-- Customer Lifetime Value Mart
-- Per-customer revenue, order count, and average order value (AOV)

WITH order_totals AS (
    SELECT
        f.order_id,
        f.customer_id,
        SUM(f.total_amount) AS order_value
    FROM fact_sales f
    GROUP BY f.order_id, f.customer_id
),
customer_metrics AS (
    SELECT
        customer_id,
        COUNT(*) AS order_count,
        SUM(order_value) AS lifetime_revenue,
        AVG(order_value) AS avg_order_value
    FROM order_totals
    GROUP BY customer_id
)
SELECT
    c.customer_id,
    c.name,
    c.email,
    c.country,
    c.city,
    cm.order_count,
    cm.lifetime_revenue,
    ROUND(cm.avg_order_value, 2) AS avg_order_value
FROM customer_metrics cm
JOIN dim_customer c ON cm.customer_id = c.customer_id
ORDER BY cm.lifetime_revenue DESC;
