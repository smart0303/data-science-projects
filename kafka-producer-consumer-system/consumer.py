"""
Welcome Email Service — consumes user_signup events and sends welcome emails.

Run from project root:
    python consumer.py
Stop: Ctrl+C
"""

import logging
import signal
import sys
import time

from kafka.errors import NoBrokersAvailable

from src.config import USER_EVENTS_TOPIC, WELCOME_EMAIL_GROUP
from src.consumer import KafkaEventConsumer
from src.events import validate_signup_event

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

_running = True


def _stop_handler(signum, frame):
    global _running
    _running = False


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
    first_name = event["name"].split()[0]
    email = event["email"]
    plan = event.get("plan", "free")

    print(f"  [EMAIL] Composing welcome email for {first_name}...")
    time.sleep(0.5)

    print(f"  [EMAIL] To      : {email}")
    print(f"  [EMAIL] Subject : Welcome to our platform, {first_name}!")
    print(f"  [EMAIL] Body    : Hi {first_name}, thanks for signing up! Your {plan} plan is now active.")
    print("  [EMAIL] Status  : SENT")
    logger.info("Welcome email sent to %s (event_id=%s)", email, event["event_id"])


def handle_event(event: dict) -> None:
    signup = validate_signup_event(event)
    if signup is None:
        return

    display_user_info(signup)
    send_welcome_email(signup)
    print()


def main() -> None:
    global _running

    print("=" * 50)
    print("  WELCOME EMAIL SERVICE — Event Consumer")
    print("=" * 50)

    consumer = KafkaEventConsumer(
        topics=[USER_EVENTS_TOPIC],
        group_id=WELCOME_EMAIL_GROUP,
    )

    try:
        consumer.connect()
    except NoBrokersAvailable:
        logger.error("Start Kafka: docker compose up -d")
        sys.exit(1)

    signal.signal(signal.SIGINT, _stop_handler)
    print("\nWaiting for signup events... Press Ctrl+C to stop.\n")

    try:
        count = consumer.run(
            handler=handle_event,
            should_stop=lambda: not _running,
        )
    finally:
        consumer.close()

    print(f"\nService stopped. Welcome emails sent: {count}")


if __name__ == "__main__":
    main()
