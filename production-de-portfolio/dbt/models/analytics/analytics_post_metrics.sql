select
    u.user_id,
    u.name as author_name,
    u.city,
    count(p.post_id) as post_count,
    round(avg(p.title_length)::numeric, 2) as avg_title_length,
    round(avg(p.body_length)::numeric, 2) as avg_body_length,
    max(p.post_extracted_at) as latest_post_extracted_at
from {{ ref('dim_users') }} as u
left join {{ ref('fct_posts') }} as p on u.user_id = p.user_id
group by u.user_id, u.name, u.city
