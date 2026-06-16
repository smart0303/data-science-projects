"""
Topic 08: Kafka Consumer with Error Handling

Reads JSON events with connection retries, invalid JSON handling, and logging.
Run: python consumer.py
Stop: Ctrl+C
"""

import json
import logging
import signal
import sys
import time

from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable

BOOTSTRAP_SERVERS = ["localhost:9092"]
TOPICS = ["user-events", "order-events"]
GROUP_ID = "error-handling-consumer-group"

MAX_CONNECT_RETRIES = 5
RETRY_DELAY_SECONDS = 2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def create_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        *TOPICS,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: value.decode("utf-8"),
        request_timeout_ms=10000,
    )


def connect_with_retry() -> KafkaConsumer:
    """Connect to Kafka with retries — handles broker unavailable at startup."""
    delay = RETRY_DELAY_SECONDS

    for attempt in range(1, MAX_CONNECT_RETRIES + 1):
        try:
            logger.info("Connecting to Kafka at %s (attempt %d/%d)", BOOTSTRAP_SERVERS, attempt, MAX_CONNECT_RETRIES)
            consumer = create_consumer()
            consumer.topics()
            logger.info("Connected. Subscribed to: %s", ", ".join(TOPICS))
            return consumer
        except NoBrokersAvailable:
            logger.error("No brokers available — is Kafka running?")
        except KafkaError as exc:
            logger.error("Kafka connection error: %s", exc)

        if attempt < MAX_CONNECT_RETRIES:
            logger.info("Retrying in %d seconds...", delay)
            time.sleep(delay)
            delay *= 2

    raise NoBrokersAvailable("Failed to connect after %d attempts" % MAX_CONNECT_RETRIES)


def parse_event(raw_value: str) -> dict | None:
    """Parse JSON safely — returns None for invalid messages."""
    try:
        event = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        logger.warning("Invalid JSON — skipping message: %s (error: %s)", raw_value[:80], exc)
        return None

    if not isinstance(event, dict):
        logger.warning("Expected JSON object — skipping: %r", raw_value[:80])
        return None

    if "event_type" not in event:
        logger.warning("Missing event_type field — skipping: %s", raw_value[:80])
        return None

    return event


def format_event(event: dict) -> str:
    event_type = event["event_type"]

    if event_type == "user_signup":
        return (
            f"USER SIGNUP | user={event['user_id']} | "
            f"name={event['name']} | email={event['email']}"
        )
    if event_type == "user_login":
        return (
            f"USER LOGIN | user={event['user_id']} | "
            f"ip={event['ip_address']} | device={event['device']}"
        )
    if event_type == "order_created":
        return (
            f"ORDER CREATED | order={event['order_id']} | user={event['user_id']} | "
            f"amount={event['currency']} {event['amount']:.2f}"
        )

    return f"UNKNOWN EVENT | type={event_type} | payload={json.dumps(event)}"


def main() -> None:
    try:
        consumer = connect_with_retry()
    except NoBrokersAvailable:
        logger.error(
            "Could not connect to Kafka. Start the broker: "
            "cd kafka-producer-consumer && docker compose up -d"
        )
        sys.exit(1)

    logger.info("Reading messages... Press Ctrl+C to stop.")

    running = True

    def handle_stop(signum, frame):
        nonlocal running
        logger.info("Shutdown signal received")
        running = False

    signal.signal(signal.SIGINT, handle_stop)

    processed = 0
    skipped = 0

    try:
        while running:
            try:
                records = consumer.poll(timeout_ms=1000)
            except KafkaError as exc:
                logger.error("Poll error: %s — will retry on next iteration", exc)
                time.sleep(RETRY_DELAY_SECONDS)
                continue

            for messages in records.values():
                for message in messages:
                    event = parse_event(message.value)
                    if event is None:
                        skipped += 1
                        continue

                    logger.info(
                        "%s [topic=%s, partition=%d, offset=%d]",
                        format_event(event),
                        message.topic,
                        message.partition,
                        message.offset,
                    )
                    processed += 1
    finally:
        consumer.close()
        logger.info("Consumer closed — processed: %d, skipped: %d", processed, skipped)


if __name__ == "__main__":
    main()
