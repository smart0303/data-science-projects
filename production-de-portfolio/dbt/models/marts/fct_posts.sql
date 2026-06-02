select
    p.post_id,
    p.user_id,
    p.title,
    p.body,
    p.title_length,
    p.body_length,
    p.extracted_at as post_extracted_at,
    u.name as author_name,
    u.email as author_email,
    u.city as author_city
from {{ ref('stg_posts') }} as p
inner join {{ ref('stg_users') }} as u on p.user_id = u.user_id
