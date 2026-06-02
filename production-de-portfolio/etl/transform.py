from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger("de_portfolio.transform")


def _clean_text(value: str | None, max_len: int | None = None) -> str:
    text = (value or "").strip()
    if max_len and len(text) > max_len:
        return text[:max_len]
    return text


def transform_users(raw_users: list[dict[str, Any]]) -> list[dict[str, Any]]:
    extracted_at = datetime.now(timezone.utc)
    rows: list[dict[str, Any]] = []

    for user in raw_users:
        address = user.get("address") or {}
        company = user.get("company") or {}
        rows.append(
            {
                "user_id": int(user["id"]),
                "name": _clean_text(user.get("name"), 255),
                "username": _clean_text(user.get("username"), 100),
                "email": _clean_text(user.get("email"), 255),
                "phone": _clean_text(user.get("phone"), 50),
                "website": _clean_text(user.get("website"), 255),
                "city": _clean_text(address.get("city"), 100),
                "company_name": _clean_text(company.get("name"), 255),
                "extracted_at": extracted_at,
            }
        )

    logger.info("Transformed %d user records", len(rows))
    return rows


def transform_posts(raw_posts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    extracted_at = datetime.now(timezone.utc)
    rows: list[dict[str, Any]] = []

    for post in raw_posts:
        rows.append(
            {
                "post_id": int(post["id"]),
                "user_id": int(post["userId"]),
                "title": _clean_text(post.get("title")),
                "body": _clean_text(post.get("body")),
                "extracted_at": extracted_at,
            }
        )

    logger.info("Transformed %d post records", len(rows))
    return rows
