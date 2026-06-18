"""Event schemas and helpers for structured Kafka messages."""

import json
import uuid
from datetime import datetime, timezone


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def create_signup_event(user: dict) -> dict:
    """Build a user_signup event from a user record."""
    return {
        "event_type": "user_signup",
        "event_id": f"evt-{uuid.uuid4().hex[:8]}",
        "user_id": user["user_id"],
        "name": user["name"],
        "email": user["email"],
        "plan": user.get("plan", "free"),
        "timestamp": utc_now(),
    }


def parse_signup_event(raw: str) -> dict | None:
    """Parse and validate a user_signup event from a JSON string."""
    try:
        event = json.loads(raw)
    except json.JSONDecodeError:
        return None

    return validate_signup_event(event)


def validate_signup_event(event: dict) -> dict | None:
    """Validate a user_signup event dict. Returns None for invalid input."""
    if not isinstance(event, dict) or event.get("event_type") != "user_signup":
        return None

    required = ("user_id", "name", "email", "event_id")
    if not all(field in event for field in required):
        return None

    return event


SAMPLE_USERS = [
    {"user_id": "u-2001", "name": "Alice Smith", "email": "alice@example.com", "plan": "free"},
    {"user_id": "u-2002", "name": "Bob Jones", "email": "bob@example.com", "plan": "pro"},
    {"user_id": "u-2003", "name": "Carol Lee", "email": "carol@example.com", "plan": "free"},
    {"user_id": "u-2004", "name": "David Kim", "email": "david@example.com", "plan": "team"},
    {"user_id": "u-2005", "name": "Eva Martinez", "email": "eva@example.com", "plan": "free"},
]
