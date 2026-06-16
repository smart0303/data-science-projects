"""
Topic 08: Kafka Producer with Error Handling

Sends JSON events with connection retries, send retries, and structured logging.
Run: python producer.py
"""

import json
import logging
import sys
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError, NoBrokersAvailable

BOOTSTRAP_SERVERS = ["localhost:9092"]
USER_EVENTS_TOPIC = "user-events"
ORDER_EVENTS_TOPIC = "order-events"

MAX_CONNECT_RETRIES = 5
MAX_SEND_RETRIES = 3
RETRY_DELAY_SECONDS = 2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_events() -> list[tuple[str, dict]]:
    return [
        (
            USER_EVENTS_TOPIC,
            {
                "event_type": "user_signup",
                "user_id": "u-1001",
                "email": "alice@example.com",
                "name": "Alice Smith",
                "timestamp": utc_now(),
            },
        ),
        (
            USER_EVENTS_TOPIC,
            {
                "event_type": "user_login",
                "user_id": "u-1001",
                "ip_address": "192.168.1.10",
                "device": "web",
                "timestamp": utc_now(),
            },
        ),
        (
            ORDER_EVENTS_TOPIC,
            {
                "event_type": "order_created",
                "order_id": "ord-5001",
                "user_id": "u-1001",
                "amount": 49.99,
                "currency": "USD",
                "items": 2,
                "timestamp": utc_now(),
            },
        ),
    ]


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        request_timeout_ms=10000,
        retries=0,
    )


def connect_with_retry() -> KafkaProducer:
    """Connect to Kafka with retries — handles broker unavailable at startup."""
    delay = RETRY_DELAY_SECONDS

    for attempt in range(1, MAX_CONNECT_RETRIES + 1):
        try:
            logger.info("Connecting to Kafka at %s (attempt %d/%d)", BOOTSTRAP_SERVERS, attempt, MAX_CONNECT_RETRIES)
            producer = create_producer()
            producer.partitions_for(USER_EVENTS_TOPIC)
            logger.info("Connected to Kafka successfully")
            return producer
        except NoBrokersAvailable:
            logger.error("No brokers available — is Kafka running?")
        except KafkaError as exc:
            logger.error("Kafka connection error: %s", exc)

        if attempt < MAX_CONNECT_RETRIES:
            logger.info("Retrying in %d seconds...", delay)
            time.sleep(delay)
            delay *= 2

    raise NoBrokersAvailable("Failed to connect after %d attempts" % MAX_CONNECT_RETRIES)


def send_event_with_retry(producer: KafkaProducer, topic: str, event: dict) -> bool:
    """Send one event with retries — returns True on success, False on failure."""
    delay = RETRY_DELAY_SECONDS

    for attempt in range(1, MAX_SEND_RETRIES + 1):
        try:
            future = producer.send(topic, value=event)
            record_metadata = future.get(timeout=10)
            logger.info(
                "Sent %s -> topic=%s, partition=%d, offset=%d",
                event["event_type"],
                record_metadata.topic,
                record_metadata.partition,
                record_metadata.offset,
            )
            return True
        except (KafkaError, KafkaTimeoutError) as exc:
            logger.error(
                "Send failed for %s (attempt %d/%d): %s",
                event.get("event_type", "unknown"),
                attempt,
                MAX_SEND_RETRIES,
                exc,
            )
            if attempt < MAX_SEND_RETRIES:
                logger.info("Retrying send in %d seconds...", delay)
                time.sleep(delay)
                delay *= 2

    logger.error("Giving up on event: %s", event.get("event_type", "unknown"))
    return False


def main() -> None:
    try:
        producer = connect_with_retry()
    except NoBrokersAvailable:
        logger.error(
            "Could not connect to Kafka. Start the broker: "
            "cd kafka-producer-consumer && docker compose up -d"
        )
        sys.exit(1)

    events = build_events()
    sent = 0
    failed = 0

    for topic, event in events:
        if send_event_with_retry(producer, topic, event):
            sent += 1
        else:
            failed += 1

    try:
        producer.flush()
    except KafkaError as exc:
        logger.error("Flush failed: %s", exc)
    finally:
        producer.close()

    logger.info("Done — sent: %d, failed: %d", sent, failed)
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
