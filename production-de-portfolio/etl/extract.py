from __future__ import annotations

import logging
from typing import Any

import requests

from etl.config import Settings
from etl.retry import API_RETRY

logger = logging.getLogger("de_portfolio.extract")


@API_RETRY
def _get_json(url: str) -> list[dict[str, Any]]:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        raise ValueError(f"Expected list from {url}, got {type(payload).__name__}")
    return payload


def fetch_users(settings: Settings) -> list[dict[str, Any]]:
    url = f"{settings.api_base_url}/users"
    logger.info("Fetching users from %s", url)
    users = _get_json(url)
    logger.info("Fetched %d users", len(users))
    return users


def fetch_posts(settings: Settings) -> list[dict[str, Any]]:
    url = f"{settings.api_base_url}/posts"
    logger.info("Fetching posts from %s", url)
    posts = _get_json(url)
    logger.info("Fetched %d posts", len(posts))
    return posts
