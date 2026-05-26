select
    order_date,
    count(distinct order_id) as order_count,
    count(distinct customer_id) as customer_count,
    sum(quantity) as total_quantity,
    sum(line_total) as total_revenue,
    round(avg(line_total), 2) as avg_line_total
from {{ ref('stg_sales') }}
group by order_date
order by order_date
