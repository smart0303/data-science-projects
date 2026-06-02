#!/usr/bin/env python3
"""Run the JSONPlaceholder → PostgreSQL ETL pipeline."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

from etl.config import load_settings
from etl.extract import fetch_posts, fetch_users
from etl.load import load_posts, load_users
from etl.logging_setup import configure_logging
from etl.transform import transform_posts, transform_users

load_dotenv()


def run_pipeline() -> dict[str, int]:
    logger = configure_logging()
    settings = load_settings()
    logger.info("Starting ETL pipeline (source=%s)", settings.api_base_url)

    raw_users = fetch_users(settings)
    raw_posts = fetch_posts(settings)

    user_rows = transform_users(raw_users)
    post_rows = transform_posts(raw_posts)

    users_loaded = load_users(settings, user_rows)
    posts_loaded = load_posts(settings, post_rows)

    summary = {
        "users_extracted": len(raw_users),
        "posts_extracted": len(raw_posts),
        "users_loaded": users_loaded,
        "posts_loaded": posts_loaded,
    }
    logger.info("ETL complete: %s", summary)
    return summary


if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception:
        logging.getLogger("de_portfolio").exception("ETL pipeline failed")
        sys.exit(1)
