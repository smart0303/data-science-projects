-- Top 3 longest posts per user (by body length)
WITH ranked AS (
    SELECT
        user_id,
        author_name,
        post_id,
        title,
        body_length,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY body_length DESC, post_id
        ) AS post_rank
    FROM marts.fct_posts
)
SELECT
    user_id,
    author_name,
    post_id,
    title,
    body_length,
    post_rank
FROM ranked
WHERE post_rank <= 3
ORDER BY user_id, post_rank;
