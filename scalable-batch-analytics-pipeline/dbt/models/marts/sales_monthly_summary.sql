select
    order_month,
    sum(line_total) as total_sales,
    count(distinct order_id) as order_count,
    sum(quantity) as total_units
from {{ ref('stg_orders') }}
group by order_month
