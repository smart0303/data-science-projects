"""
User Signup Service — publishes user_signup events to Kafka.

Run from project root:
    python producer.py
"""

import logging
import sys
import time

from kafka.errors import NoBrokersAvailable

from src.config import USER_EVENTS_TOPIC
from src.events import SAMPLE_USERS, create_signup_event
from src.producer import KafkaEventProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def main() -> None:
    print("=" * 50)
    print("  USER SIGNUP SERVICE — Event Producer")
    print("=" * 50)

    producer = KafkaEventProducer(topic=USER_EVENTS_TOPIC)

    try:
        producer.connect()
    except NoBrokersAvailable:
        logger.error("Start Kafka: docker compose up -d")
        sys.exit(1)

    sent = 0
    try:
        for user in SAMPLE_USERS:
            event = create_signup_event(user)
            print(f"\nRegistering: {user['name']} ({user['email']})")
            if producer.send(event, key=event["user_id"]):
                sent += 1
            time.sleep(0.3)
    finally:
        producer.close()

    print(f"\n{'=' * 50}")
    print(f"  {sent}/{len(SAMPLE_USERS)} signup events published to '{USER_EVENTS_TOPIC}'")
    print("=" * 50)


if __name__ == "__main__":
    main()
