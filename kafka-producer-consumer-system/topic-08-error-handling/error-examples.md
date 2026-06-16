# Topic 08 — Error Handling Examples

Quick reference for common Kafka errors and how this project handles them.

---

## Common errors

| Error | When it happens | Handler |
|-------|-----------------|---------|
| `NoBrokersAvailable` | Kafka is down or wrong bootstrap address | Retry connect with backoff; exit after max attempts |
| `KafkaTimeoutError` | Broker slow or network issue during send | Retry send up to 3 times |
| `KafkaError` (general) | Topic missing, leader election, etc. | Log error; retry or skip depending on context |
| `json.JSONDecodeError` | Non-JSON or malformed message in topic | Log warning; skip message; continue consuming |
| Poll failure | Broker unavailable during `consumer.poll()` | Log error; sleep; retry on next iteration |

---

## Producer examples

### Successful send

```
12:00:01 [INFO] Connecting to Kafka at ['localhost:9092'] (attempt 1/5)
12:00:01 [INFO] Connected to Kafka successfully
12:00:01 [INFO] Sent user_signup -> topic=user-events, partition=0, offset=15
12:00:01 [INFO] Done — sent: 3, failed: 0
```

### Connection failure (Kafka stopped)

```
12:00:01 [INFO] Connecting to Kafka at ['localhost:9092'] (attempt 1/5)
12:00:03 [ERROR] No brokers available — is Kafka running?
12:00:03 [INFO] Retrying in 2 seconds...
12:00:05 [INFO] Connecting to Kafka at ['localhost:9092'] (attempt 2/5)
12:00:07 [ERROR] No brokers available — is Kafka running?
12:00:07 [INFO] Retrying in 4 seconds...
...
12:00:25 [ERROR] Could not connect to Kafka. Start the broker: cd kafka-producer-consumer && docker compose up -d
```

### Send failure with retry

```
12:00:10 [ERROR] Send failed for user_login (attempt 1/3): KafkaTimeoutError(...)
12:00:10 [INFO] Retrying send in 2 seconds...
12:00:12 [INFO] Sent user_login -> topic=user-events, partition=1, offset=8
```

---

## Consumer examples

### Invalid JSON skipped

```
12:00:05 [WARNING] Invalid JSON — skipping message: this is not json (error: Expecting value: line 1 column 1 (char 0))
12:00:05 [INFO] USER SIGNUP | user=u-1001 | name=Alice Smith | email=alice@example.com [topic=user-events, partition=0, offset=16]
```

### Missing event_type field

```
12:00:06 [WARNING] Missing event_type field — skipping: {"user_id": "u-9999"}
```

### Poll error during shutdown

```
12:00:20 [ERROR] Poll error: NoBrokersAvailable — will retry on next iteration
12:00:22 [ERROR] Poll error: NoBrokersAvailable — will retry on next iteration
```

After Kafka restarts, polling resumes and new messages are processed.

---

## Simulation commands

```powershell
# Stop Kafka (simulate shutdown)
cd d:\Work\data-science-projects\kafka-producer-consumer
docker compose stop

# Start Kafka (recover)
docker compose start

# Inject invalid JSON into topic
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh `
  --bootstrap-server localhost:9092 `
  --topic user-events
# Type: not valid json {broken

# Run producer with error handling
cd d:\Work\data-science-projects\kafka-producer-consumer-system\topic-08-error-handling
python producer.py

# Run consumer with error handling
python consumer.py
```

---

## Retry configuration

Defined at the top of `producer.py` and `consumer.py`:

```python
MAX_CONNECT_RETRIES = 5
MAX_SEND_RETRIES = 3
RETRY_DELAY_SECONDS = 2
# delay doubles each retry: 2s → 4s → 8s → ...
```

Adjust these values to tune how long the client waits before giving up.

---

## Fault tolerance principles

1. **Fail gracefully** — log the error, don't crash silently
2. **Retry transient failures** — network blips and broker restarts are often temporary
3. **Skip poison messages** — one bad JSON record should not stop the entire consumer
4. **Use timeouts** — `future.get(timeout=10)` prevents hanging forever
5. **Clean up resources** — `producer.close()` / `consumer.close()` in `finally` blocks

---

## Related reading

- [error-handling-notes.md](error-handling-notes.md) — full exercise walkthrough
- [Topic 07 JSON consumer](../topic-07-json-messages/json_consumer.py) — basic invalid JSON skip (upgraded in Topic 08)
