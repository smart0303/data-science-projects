{{
    config(
        unique_key='order_id',
        incremental_strategy='merge'
    )
}}

with source_sales as (
    select
        order_id,
        order_date,
        customer_id,
        product_id,
        quantity,
        price,
        created_at
    from {{ source('raw', 'sales_raw') }}

    {% if is_incremental() %}
        where created_at > (select coalesce(max(created_at), '1970-01-01'::timestamp) from {{ this }})
    {% endif %}
)

select
    order_id,
    order_date,
    customer_id,
    product_id,
    quantity,
    price,
    quantity * price as line_total,
    created_at
from source_sales
