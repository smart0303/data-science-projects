from __future__ import annotations

import logging
from typing import Any

import psycopg2
from psycopg2.extras import execute_values

from etl.config import Settings

logger = logging.getLogger("de_portfolio.load")

USER_UPSERT = """
INSERT INTO raw.users (
    user_id, name, username, email, phone, website, city, company_name, extracted_at
) VALUES %s
ON CONFLICT (user_id) DO UPDATE SET
    name = EXCLUDED.name,
    username = EXCLUDED.username,
    email = EXCLUDED.email,
    phone = EXCLUDED.phone,
    website = EXCLUDED.website,
    city = EXCLUDED.city,
    company_name = EXCLUDED.company_name,
    extracted_at = EXCLUDED.extracted_at
"""

POST_UPSERT = """
INSERT INTO raw.posts (post_id, user_id, title, body, extracted_at)
VALUES %s
ON CONFLICT (post_id) DO UPDATE SET
    user_id = EXCLUDED.user_id,
    title = EXCLUDED.title,
    body = EXCLUDED.body,
    extracted_at = EXCLUDED.extracted_at
"""


def _upsert(
    settings: Settings,
    sql_template: str,
    rows: list[dict[str, Any]],
    columns: list[str],
    label: str,
) -> int:
    if not rows:
        logger.warning("No %s rows to load", label)
        return 0

    tuples = [tuple(row[col] for col in columns) for row in rows]
    with psycopg2.connect(settings.postgres_dsn) as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql_template, tuples, page_size=500)
        conn.commit()

    logger.info("Loaded %d %s rows into PostgreSQL", len(rows), label)
    return len(rows)


def load_users(settings: Settings, rows: list[dict[str, Any]]) -> int:
    columns = [
        "user_id",
        "name",
        "username",
        "email",
        "phone",
        "website",
        "city",
        "company_name",
        "extracted_at",
    ]
    return _upsert(settings, USER_UPSERT, rows, columns, "user")


def load_posts(settings: Settings, rows: list[dict[str, Any]]) -> int:
    columns = ["post_id", "user_id", "title", "body", "extracted_at"]
    return _upsert(settings, POST_UPSERT, rows, columns, "post")
