-- User activity rollup from analytics mart
SELECT
    user_id,
    author_name,
    city,
    post_count,
    avg_title_length,
    avg_body_length,
    latest_post_extracted_at,
    CASE
        WHEN post_count >= 10 THEN 'high'
        WHEN post_count >= 5 THEN 'medium'
        ELSE 'low'
    END AS activity_tier
FROM analytics.analytics_post_metrics
ORDER BY post_count DESC, author_name;
