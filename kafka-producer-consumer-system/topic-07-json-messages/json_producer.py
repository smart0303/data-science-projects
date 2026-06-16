"""
Topic 07: JSON Event Producer

Sends structured JSON events to Kafka topics.
Run: python json_producer.py
"""

import json
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

BOOTSTRAP_SERVERS = ["localhost:9092"]
USER_EVENTS_TOPIC = "user-events"
ORDER_EVENTS_TOPIC = "order-events"


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_events() -> list[tuple[str, dict]]:
    """Return (topic, event_dict) pairs for each sample event type."""
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
        (
            USER_EVENTS_TOPIC,
            {
                "event_type": "user_signup",
                "user_id": "u-1002",
                "email": "bob@example.com",
                "name": "Bob Jones",
                "timestamp": utc_now(),
            },
        ),
        (
            USER_EVENTS_TOPIC,
            {
                "event_type": "user_login",
                "user_id": "u-1002",
                "ip_address": "10.0.0.55",
                "device": "mobile",
                "timestamp": utc_now(),
            },
        ),
        (
            ORDER_EVENTS_TOPIC,
            {
                "event_type": "order_created",
                "order_id": "ord-5002",
                "user_id": "u-1002",
                "amount": 129.50,
                "currency": "USD",
                "items": 5,
                "timestamp": utc_now(),
            },
        ),
    ]


def send_event(producer: KafkaProducer, topic: str, event: dict) -> None:
    future = producer.send(topic, value=event)
    record_metadata = future.get(timeout=10)
    print(f"Sent {event['event_type']} -> topic={record_metadata.topic}, offset={record_metadata.offset}")
    print(json.dumps(event, indent=2))
    print()


def main() -> None:
    print(f"Connecting to Kafka at {BOOTSTRAP_SERVERS} ...")

    try:
        producer = create_producer()
    except NoBrokersAvailable:
        print(
            "Could not connect to Kafka. "
            "Start the broker first: cd kafka-producer-consumer && docker compose up -d"
        )
        raise SystemExit(1) from None

    print("Connected. Publishing JSON events.\n")

    events = build_events()
    for topic, event in events:
        send_event(producer, topic, event)

    producer.flush()
    producer.close()
    print(f"All {len(events)} JSON events sent successfully.")


if __name__ == "__main__":
    try:
        main()
    except KafkaError as exc:
        print(f"Kafka error: {exc}")
        raise SystemExit(1) from exc
