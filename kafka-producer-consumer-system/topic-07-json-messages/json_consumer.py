"""
Topic 07: JSON Event Consumer

Subscribes to user-events and order-events, parses JSON, and displays formatted output.
Run: python json_consumer.py
Stop: Ctrl+C
"""

import json
import signal
import sys

from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable

BOOTSTRAP_SERVERS = ["localhost:9092"]
TOPICS = ["user-events", "order-events"]
GROUP_ID = "json-events-consumer-group"


def create_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        *TOPICS,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: value.decode("utf-8"),
    )


def format_event(event: dict) -> str:
    event_type = event.get("event_type", "unknown")

    handlers = {
        "user_signup": lambda e: (
            f"USER SIGNUP\n"
            f"  User ID : {e['user_id']}\n"
            f"  Name    : {e['name']}\n"
            f"  Email   : {e['email']}\n"
            f"  Time    : {e['timestamp']}"
        ),
        "user_login": lambda e: (
            f"USER LOGIN\n"
            f"  User ID : {e['user_id']}\n"
            f"  IP      : {e['ip_address']}\n"
            f"  Device  : {e['device']}\n"
            f"  Time    : {e['timestamp']}"
        ),
        "order_created": lambda e: (
            f"ORDER CREATED\n"
            f"  Order ID : {e['order_id']}\n"
            f"  User ID  : {e['user_id']}\n"
            f"  Amount   : {e['currency']} {e['amount']:.2f}\n"
            f"  Items    : {e['items']}\n"
            f"  Time     : {e['timestamp']}"
        ),
    }

    formatter = handlers.get(event_type)
    if formatter:
        return formatter(event)

    return f"UNKNOWN EVENT ({event_type})\n{json.dumps(event, indent=2)}"


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

    print(f"Subscribed to topics: {', '.join(TOPICS)} (group: {GROUP_ID}).")
    print("Reading JSON events... Press Ctrl+C to stop.\n")

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
                    try:
                        event = json.loads(message.value)
                    except json.JSONDecodeError:
                        print(f"Skipping non-JSON message: {message.value!r}\n")
                        continue

                    print("─" * 40)
                    print(format_event(event))
                    print(
                        f"  [topic={message.topic}, "
                        f"partition={message.partition}, "
                        f"offset={message.offset}]"
                    )
                    print()
    finally:
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    try:
        main()
    except KafkaError as exc:
        print(f"Kafka error: {exc}")
        sys.exit(1)
