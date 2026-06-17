"""
Topic 09: User Signup Event Producer

Publishes user_signup events to Kafka when new users register.
Run: python signup_producer.py
"""

import json
import logging
import sys
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError, NoBrokersAvailable

BOOTSTRAP_SERVERS = ["localhost:9092"]
TOPIC = "user-events"
MAX_CONNECT_RETRIES = 5
MAX_SEND_RETRIES = 3
RETRY_DELAY_SECONDS = 2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

NEW_USERS = [
    {"user_id": "u-2001", "name": "Alice Smith", "email": "alice@example.com", "plan": "free"},
    {"user_id": "u-2002", "name": "Bob Jones", "email": "bob@example.com", "plan": "pro"},
    {"user_id": "u-2003", "name": "Carol Lee", "email": "carol@example.com", "plan": "free"},
    {"user_id": "u-2004", "name": "David Kim", "email": "david@example.com", "plan": "team"},
    {"user_id": "u-2005", "name": "Eva Martinez", "email": "eva@example.com", "plan": "free"},
]


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def create_signup_event(user: dict) -> dict:
    return {
        "event_type": "user_signup",
        "event_id": f"evt-{uuid.uuid4().hex[:8]}",
        "user_id": user["user_id"],
        "name": user["name"],
        "email": user["email"],
        "plan": user["plan"],
        "timestamp": utc_now(),
    }


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        request_timeout_ms=10000,
        retries=0,
    )


def connect_with_retry() -> KafkaProducer:
    delay = RETRY_DELAY_SECONDS
    for attempt in range(1, MAX_CONNECT_RETRIES + 1):
        try:
            logger.info("Connecting to Kafka (attempt %d/%d)", attempt, MAX_CONNECT_RETRIES)
            producer = create_producer()
            producer.partitions_for(TOPIC)
            logger.info("Connected to Kafka")
            return producer
        except (NoBrokersAvailable, KafkaError) as exc:
            logger.error("Connection failed: %s", exc)
            if attempt < MAX_CONNECT_RETRIES:
                logger.info("Retrying in %d seconds...", delay)
                time.sleep(delay)
                delay *= 2
    raise NoBrokersAvailable("Kafka unavailable after retries")


def publish_signup(producer: KafkaProducer, event: dict) -> bool:
    delay = RETRY_DELAY_SECONDS
    for attempt in range(1, MAX_SEND_RETRIES + 1):
        try:
            future = producer.send(TOPIC, value=event, key=event["user_id"].encode("utf-8"))
            meta = future.get(timeout=10)
            logger.info(
                "Signup published | user=%s | email=%s | event_id=%s | offset=%d",
                event["name"],
                event["email"],
                event["event_id"],
                meta.offset,
            )
            return True
        except (KafkaError, KafkaTimeoutError) as exc:
            logger.error("Publish failed (attempt %d/%d): %s", attempt, MAX_SEND_RETRIES, exc)
            if attempt < MAX_SEND_RETRIES:
                time.sleep(delay)
                delay *= 2
    return False


def main() -> None:
    print("=" * 50)
    print("  USER SIGNUP SERVICE — Event Producer")
    print("=" * 50)

    try:
        producer = connect_with_retry()
    except NoBrokersAvailable:
        logger.error("Start Kafka: cd kafka-producer-consumer && docker compose up -d")
        sys.exit(1)

    sent = 0
    for user in NEW_USERS:
        event = create_signup_event(user)
        print(f"\nRegistering: {user['name']} ({user['email']})")
        if publish_signup(producer, event):
            sent += 1
        time.sleep(0.3)

    producer.flush()
    producer.close()

    print(f"\n{'=' * 50}")
    print(f"  {sent}/{len(NEW_USERS)} signup events published to '{TOPIC}'")
    print("=" * 50)


if __name__ == "__main__":
    main()
