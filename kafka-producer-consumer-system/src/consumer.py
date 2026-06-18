"""Refactored Kafka consumer with retry, JSON parsing, and graceful shutdown."""

import json
import logging
import time
from collections.abc import Callable

from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable

from src.config import (
    BOOTSTRAP_SERVERS,
    MAX_CONNECT_RETRIES,
    POLL_TIMEOUT_MS,
    RETRY_DELAY_SECONDS,
)

logger = logging.getLogger(__name__)

EventHandler = Callable[[dict], None]


class KafkaEventConsumer:
    """Subscribes to Kafka topics and dispatches events to a handler."""

    def __init__(self, topics: list[str], group_id: str):
        self.topics = topics
        self.group_id = group_id
        self._consumer: KafkaConsumer | None = None
        self._running = False

    def _create_client(self) -> KafkaConsumer:
        return KafkaConsumer(
            *self.topics,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=self.group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda value: value.decode("utf-8"),
            request_timeout_ms=10000,
        )

    def connect(self) -> None:
        delay = RETRY_DELAY_SECONDS
        for attempt in range(1, MAX_CONNECT_RETRIES + 1):
            try:
                logger.info("Connecting to Kafka (attempt %d/%d)", attempt, MAX_CONNECT_RETRIES)
                self._consumer = self._create_client()
                self._consumer.topics()
                logger.info("Consumer subscribed to: %s", ", ".join(self.topics))
                return
            except (NoBrokersAvailable, KafkaError) as exc:
                logger.error("Connection failed: %s", exc)
                if attempt < MAX_CONNECT_RETRIES:
                    logger.info("Retrying in %d seconds...", delay)
                    time.sleep(delay)
                    delay *= 2
        raise NoBrokersAvailable("Kafka unavailable after retries")

    def run(self, handler: EventHandler, should_stop: Callable[[], bool]) -> int:
        if self._consumer is None:
            raise RuntimeError("Consumer not connected. Call connect() first.")

        processed = 0
        while not should_stop():
            try:
                records = self._consumer.poll(timeout_ms=POLL_TIMEOUT_MS)
            except KafkaError as exc:
                logger.error("Poll error: %s", exc)
                time.sleep(RETRY_DELAY_SECONDS)
                continue

            for messages in records.values():
                for message in messages:
                    try:
                        event = json.loads(message.value)
                    except json.JSONDecodeError:
                        logger.warning("Invalid JSON — skipping message")
                        continue

                    if not isinstance(event, dict):
                        continue

                    handler(event)
                    processed += 1

        return processed

    def close(self) -> None:
        if self._consumer:
            self._consumer.close()
            self._consumer = None
