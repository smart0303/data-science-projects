select *
from {{ ref('stg_sales') }}
where line_total <= 0
