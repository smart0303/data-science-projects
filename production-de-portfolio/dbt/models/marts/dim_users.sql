select
    user_id,
    name,
    username,
    email,
    phone,
    website,
    city,
    company_name,
    extracted_at as last_extracted_at
from {{ ref('stg_users') }}
