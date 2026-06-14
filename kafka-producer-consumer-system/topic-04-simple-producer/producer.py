"""
Topic 04: Simple Kafka Producer

Connects to local Kafka and publishes messages to the user-events topic.
Run: python producer.py
"""

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

BOOTSTRAP_SERVERS = ["localhost:9092"]
TOPIC = "user-events"


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: value.encode("utf-8"),
    )


def send_message(producer: KafkaProducer, message: str) -> None:
    future = producer.send(TOPIC, value=message)
    record_metadata = future.get(timeout=10)
    print(
        f"Sent: {message!r} -> "
        f"topic={record_metadata.topic}, "
        f"partition={record_metadata.partition}, "
        f"offset={record_metadata.offset}"
    )


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

    print(f"Connected. Publishing to topic '{TOPIC}'.\n")

    print("--- Simple text message ---")
    send_message(producer, "Hello from Kafka producer!")

    print("\n--- 10 test messages ---")
    for i in range(1, 11):
        send_message(producer, f"Test message {i}/10")

    producer.flush()
    producer.close()
    print("\nAll messages sent successfully.")


if __name__ == "__main__":
    try:
        main()
    except KafkaError as exc:
        print(f"Kafka error: {exc}")
        raise SystemExit(1) from exc
