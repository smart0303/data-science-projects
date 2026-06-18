"""Shared configuration for the Kafka Producer & Consumer System."""

BOOTSTRAP_SERVERS = ["localhost:9092"]

USER_EVENTS_TOPIC = "user-events"
ORDER_EVENTS_TOPIC = "order-events"

WELCOME_EMAIL_GROUP = "welcome-email-service"

MAX_CONNECT_RETRIES = 5
MAX_SEND_RETRIES = 3
RETRY_DELAY_SECONDS = 2
POLL_TIMEOUT_MS = 1000
SEND_TIMEOUT_SECONDS = 10
