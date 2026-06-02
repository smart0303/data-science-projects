select user_id, post_count
from {{ ref('analytics_post_metrics') }}
where post_count < 0
