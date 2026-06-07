{{
    config(
        unique_key='order_id',
        incremental_strategy='merge'
    )
}}

with source_orders as (
    select
        cast(order_id as bigint) as order_id,
        cast(order_date as date) as order_date,
        region,
        product_category,
        product,
        cast(quantity as integer) as quantity,
        cast(unit_price as double) as unit_price,
        cast(line_total as double) as line_total,
        order_month,
        cast(ingested_at as timestamp) as ingested_at
    from read_parquet('{{ var("silver_orders_glob") }}')

    {% if is_incremental() %}
        where ingested_at > (select coalesce(max(ingested_at), '1970-01-01'::timestamp) from {{ this }})
    {% endif %}
)

select * from source_orders
