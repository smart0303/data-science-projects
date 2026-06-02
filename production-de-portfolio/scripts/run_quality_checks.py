#!/usr/bin/env python3
"""Post-pipeline data quality checks against PostgreSQL."""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timezone

import psycopg2
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("quality_checks")


def _dsn() -> str:
    return (
        f"host={os.getenv('POSTGRES_HOST', 'localhost')} "
        f"port={os.getenv('POSTGRES_PORT', '5432')} "
        f"dbname={os.getenv('POSTGRES_DB', 'de_portfolio')} "
        f"user={os.getenv('POSTGRES_USER', 'de_user')} "
        f"password={os.getenv('POSTGRES_PASSWORD', 'de_password')}"
    )


def run_checks() -> None:
    failures: list[str] = []

    with psycopg2.connect(_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM raw.users")
            user_count = cur.fetchone()[0]
            if user_count < 1:
                failures.append(f"raw.users row count too low: {user_count}")

            cur.execute("SELECT COUNT(*) FROM raw.posts")
            post_count = cur.fetchone()[0]
            if post_count < 1:
                failures.append(f"raw.posts row count too low: {post_count}")

            cur.execute(
                """
                SELECT COUNT(*)
                FROM raw.posts p
                LEFT JOIN raw.users u ON p.user_id = u.user_id
                WHERE u.user_id IS NULL
                """
            )
            orphan_posts = cur.fetchone()[0]
            if orphan_posts > 0:
                failures.append(f"orphan posts in raw layer: {orphan_posts}")

            cur.execute(
                """
                SELECT MAX(extracted_at) FROM raw.posts
                """
            )
            latest_extract = cur.fetchone()[0]
            if latest_extract is None:
                failures.append("raw.posts has no extracted_at timestamps")
            else:
                age_hours = (
                    datetime.now(timezone.utc) - latest_extract.astimezone(timezone.utc)
                ).total_seconds() / 3600
                if age_hours > 168:
                    failures.append(
                        f"raw.posts freshness stale ({age_hours:.1f} hours old)"
                    )

            cur.execute(
                """
                SELECT COUNT(*)
                FROM analytics.analytics_post_metrics
                """
            )
            analytics_count = cur.fetchone()[0]
            if analytics_count < 1:
                failures.append("analytics.analytics_post_metrics is empty")

    if failures:
        for item in failures:
            logger.error("QUALITY CHECK FAILED: %s", item)
        raise SystemExit(1)

    logger.info(
        "All quality checks passed (users=%s, posts=%s, analytics_rows=%s)",
        user_count,
        post_count,
        analytics_count,
    )


if __name__ == "__main__":
    run_checks()
