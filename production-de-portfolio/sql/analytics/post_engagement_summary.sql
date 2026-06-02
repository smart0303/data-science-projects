-- Posts per author with average content length (analytics layer)
SELECT
    author_name,
    author_city,
    COUNT(*) AS total_posts,
    ROUND(AVG(title_length)::numeric, 1) AS avg_title_length,
    ROUND(AVG(body_length)::numeric, 1) AS avg_body_length,
    MAX(post_extracted_at) AS latest_post_at
FROM marts.fct_posts
GROUP BY author_name, author_city
ORDER BY total_posts DESC, author_name;
