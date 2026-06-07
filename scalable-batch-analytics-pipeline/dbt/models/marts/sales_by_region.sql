select
    region,
    sum(line_total) as total_sales,
    sum(quantity) as total_units,
    count(distinct order_id) as order_count,
    round(avg(line_total), 2) as avg_line_total
from {{ ref('stg_orders') }}
group by region
