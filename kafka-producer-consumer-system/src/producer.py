"""Refactored Kafka producer with connection retries and JSON serialization."""

import json
import logging
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError, NoBrokersAvailable

from src.config import (
    BOOTSTRAP_SERVERS,
    MAX_CONNECT_RETRIES,
    MAX_SEND_RETRIES,
    RETRY_DELAY_SECONDS,
    SEND_TIMEOUT_SECONDS,
    USER_EVENTS_TOPIC,
)

logger = logging.getLogger(__name__)


class KafkaEventProducer:
    """Publishes JSON events to Kafka with retry support."""

    def __init__(self, topic: str = USER_EVENTS_TOPIC):
        self.topic = topic
        self._producer: KafkaProducer | None = None

    def _create_client(self) -> KafkaProducer:
        return KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            request_timeout_ms=10000,
            retries=0,
        )

    def connect(self) -> None:
        delay = RETRY_DELAY_SECONDS
        for attempt in range(1, MAX_CONNECT_RETRIES + 1):
            try:
                logger.info("Connecting to Kafka (attempt %d/%d)", attempt, MAX_CONNECT_RETRIES)
                self._producer = self._create_client()
                self._producer.partitions_for(self.topic)
                logger.info("Producer connected to topic '%s'", self.topic)
                return
            except (NoBrokersAvailable, KafkaError) as exc:
                logger.error("Connection failed: %s", exc)
                if attempt < MAX_CONNECT_RETRIES:
                    logger.info("Retrying in %d seconds...", delay)
                    time.sleep(delay)
                    delay *= 2
        raise NoBrokersAvailable("Kafka unavailable after retries")

    def send(self, event: dict, key: str | None = None) -> bool:
        if self._producer is None:
            raise RuntimeError("Producer not connected. Call connect() first.")

        delay = RETRY_DELAY_SECONDS
        key_bytes = key.encode("utf-8") if key else None

        for attempt in range(1, MAX_SEND_RETRIES + 1):
            try:
                future = self._producer.send(self.topic, value=event, key=key_bytes)
                meta = future.get(timeout=SEND_TIMEOUT_SECONDS)
                logger.info(
                    "Event published | type=%s | topic=%s | offset=%d",
                    event.get("event_type", "unknown"),
                    meta.topic,
                    meta.offset,
                )
                return True
            except (KafkaError, KafkaTimeoutError) as exc:
                logger.error("Send failed (attempt %d/%d): %s", attempt, MAX_SEND_RETRIES, exc)
                if attempt < MAX_SEND_RETRIES:
                    time.sleep(delay)
                    delay *= 2
        return False

    def close(self) -> None:
        if self._producer:
            self._producer.flush()
            self._producer.close()
            self._producer = None
