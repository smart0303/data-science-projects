select
    user_id,
    name,
    username,
    lower(trim(email)) as email,
    phone,
    website,
    city,
    company_name,
    extracted_at
from {{ source('raw', 'users') }}
