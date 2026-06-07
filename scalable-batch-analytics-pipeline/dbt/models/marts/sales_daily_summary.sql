{{
    config(
        unique_key='order_date',
        incremental_strategy='merge'
    )
}}

select
    order_date,
    sum(line_total) as total_sales,
    count(distinct order_id) as order_count,
    sum(quantity) as total_units
from {{ ref('stg_orders') }}

{% if is_incremental() %}
    where order_date > (select coalesce(max(order_date), '1970-01-01'::date) from {{ this }})
{% endif %}

group by order_date
