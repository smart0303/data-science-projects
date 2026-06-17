"""
Topic 09: Welcome Email Consumer

Listens for user_signup events and simulates welcome email delivery.
Run: python signup_consumer.py
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
TOPIC = "user-events"
GROUP_ID = "welcome-email-service"
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
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: value.decode("utf-8"),
    )


def connect_with_retry() -> KafkaConsumer:
    delay = RETRY_DELAY_SECONDS
    for attempt in range(1, MAX_CONNECT_RETRIES + 1):
        try:
            logger.info("Connecting to Kafka (attempt %d/%d)", attempt, MAX_CONNECT_RETRIES)
            consumer = create_consumer()
            consumer.topics()
            logger.info("Welcome Email Service listening on '%s'", TOPIC)
            return consumer
        except (NoBrokersAvailable, KafkaError) as exc:
            logger.error("Connection failed: %s", exc)
            if attempt < MAX_CONNECT_RETRIES:
                logger.info("Retrying in %d seconds...", delay)
                time.sleep(delay)
                delay *= 2
    raise NoBrokersAvailable("Kafka unavailable after retries")


def parse_signup_event(raw: str) -> dict | None:
    try:
        event = json.loads(raw)
    except json.JSONDecodeError:
        return None

    if not isinstance(event, dict) or event.get("event_type") != "user_signup":
        return None

    required = ("user_id", "name", "email", "event_id")
    if not all(field in event for field in required):
        logger.warning("Incomplete signup event — skipping")
        return None

    return event


def display_user_info(event: dict) -> None:
    print("\n" + "─" * 50)
    print("  NEW USER SIGNUP RECEIVED")
    print("─" * 50)
    print(f"  Event ID  : {event['event_id']}")
    print(f"  User ID   : {event['user_id']}")
    print(f"  Name      : {event['name']}")
    print(f"  Email     : {event['email']}")
    print(f"  Plan      : {event.get('plan', 'free')}")
    print(f"  Timestamp : {event.get('timestamp', 'n/a')}")
    print("─" * 50)


def send_welcome_email(event: dict) -> None:
    """Simulate welcome email processing."""
    name = event["name"].split()[0]
    email = event["email"]
    plan = event.get("plan", "free")

    print(f"  [EMAIL] Composing welcome email for {name}...")
    time.sleep(0.5)

    subject = f"Welcome to our platform, {name}!"
    body_preview = (
        f"Hi {name}, thanks for signing up! Your {plan} plan is now active."
    )

    print(f"  [EMAIL] To      : {email}")
    print(f"  [EMAIL] Subject : {subject}")
    print(f"  [EMAIL] Body    : {body_preview}")
    print(f"  [EMAIL] Status  : SENT")
    logger.info("Welcome email sent to %s (event_id=%s)", email, event["event_id"])


def process_signup(event: dict) -> None:
    display_user_info(event)
    send_welcome_email(event)
    print()


def main() -> None:
    print("=" * 50)
    print("  WELCOME EMAIL SERVICE — Event Consumer")
    print("=" * 50)

    try:
        consumer = connect_with_retry()
    except NoBrokersAvailable:
        logger.error("Start Kafka: cd kafka-producer-consumer && docker compose up -d")
        sys.exit(1)

    print("\nWaiting for signup events... Press Ctrl+C to stop.\n")

    running = True

    def handle_stop(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, handle_stop)

    signups_processed = 0

    try:
        while running:
            try:
                records = consumer.poll(timeout_ms=1000)
            except KafkaError as exc:
                logger.error("Poll error: %s", exc)
                time.sleep(RETRY_DELAY_SECONDS)
                continue

            for messages in records.values():
                for message in messages:
                    event = parse_signup_event(message.value)
                    if event is None:
                        continue

                    process_signup(event)
                    signups_processed += 1
    finally:
        consumer.close()
        print(f"\nService stopped. Welcome emails sent: {signups_processed}")


if __name__ == "__main__":
    main()
