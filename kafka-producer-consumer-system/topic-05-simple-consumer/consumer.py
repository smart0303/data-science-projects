"""
Topic 05: Simple Kafka Consumer

Subscribes to user-events and prints incoming messages to the terminal.
Run: python consumer.py
Stop: Ctrl+C
"""

import signal
import sys

from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable

BOOTSTRAP_SERVERS = ["localhost:9092"]
TOPIC = "user-events"
GROUP_ID = "user-events-consumer-group"


def create_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: value.decode("utf-8"),
    )


def main() -> None:
    print(f"Connecting to Kafka at {BOOTSTRAP_SERVERS} ...")

    try:
        consumer = create_consumer()
    except NoBrokersAvailable:
        print(
            "Could not connect to Kafka. "
            "Start the broker first: cd kafka-producer-consumer && docker compose up -d"
        )
        raise SystemExit(1) from None

    print(f"Subscribed to topic '{TOPIC}' (group: {GROUP_ID}).")
    print("Reading messages... Press Ctrl+C to stop.\n")

    running = True

    def handle_stop(signum, frame):
        nonlocal running
        print("\nStopping consumer...")
        running = False

    signal.signal(signal.SIGINT, handle_stop)

    try:
        while running:
            records = consumer.poll(timeout_ms=1000)
            for messages in records.values():
                for message in messages:
                    print(
                        f"Received: {message.value!r} "
                        f"[topic={message.topic}, "
                        f"partition={message.partition}, "
                        f"offset={message.offset}]"
                    )
    finally:
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    try:
        main()
    except KafkaError as exc:
        print(f"Kafka error: {exc}")
        sys.exit(1)
