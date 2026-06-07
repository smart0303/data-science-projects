-- Fails if any staged order has a non-positive line total
select order_id, line_total
from {{ ref('stg_orders') }}
where line_total <= 0
