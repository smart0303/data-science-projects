select
    post_id,
    user_id,
    trim(title) as title,
    trim(body) as body,
    length(trim(title)) as title_length,
    length(trim(body)) as body_length,
    extracted_at
from {{ source('raw', 'posts') }}
