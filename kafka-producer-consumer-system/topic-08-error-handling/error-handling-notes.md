# Topic 08 — Error Handling Basics

## Goal

Handle common Kafka failures gracefully using try/except blocks, logging, retries, and defensive parsing.

## Files

| File | Purpose |
|------|---------|
| `producer.py` | Connect/send retries, structured error logging |
| `consumer.py` | Connect retries, invalid JSON handling, poll error recovery |
| `error-examples.md` | Hands-on failure simulation exercises |

---

## Error handling patterns used

### 1. Connection retries (producer and consumer)

```python
for attempt in range(1, MAX_CONNECT_RETRIES + 1):
    try:
        client = create_client()
        return client
    except NoBrokersAvailable:
        logger.error("No brokers available")
        time.sleep(delay)
        delay *= 2
```

Retries with **exponential backoff** give Kafka time to restart after a shutdown.

### 2. Send retries (producer)

```python
try:
    future = producer.send(topic, value=event)
    future.get(timeout=10)
except KafkaError as exc:
    logger.error("Send failed: %s", exc)
    # retry...
```

### 3. Invalid JSON (consumer)

```python
try:
    event = json.loads(raw_value)
except json.JSONDecodeError as exc:
    logger.warning("Invalid JSON — skipping: %s", exc)
    return None
```

Bad messages are **logged and skipped** — the consumer keeps running.

### 4. Structured logging

```python
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger.error("Send failed for user_login: %s", exc)
```

Logs include timestamps and severity for debugging production issues.

---

## Setup

```powershell
cd d:\Work\data-science-projects\kafka-producer-consumer-system\topic-08-error-handling
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

---

## Exercise 1 — Producer connection failure

**Simulate:** Kafka is stopped.

```powershell
cd d:\Work\data-science-projects\kafka-producer-consumer
docker compose stop
```

**Test producer:**

```powershell
cd ..\kafka-producer-consumer-system\topic-08-error-handling
python producer.py
```

**Expected logs:**

```
[INFO] Connecting to Kafka at ['localhost:9092'] (attempt 1/5)
[ERROR] No brokers available — is Kafka running?
[INFO] Retrying in 2 seconds...
...
[ERROR] Could not connect to Kafka.
```

**Recover:**

```powershell
cd d:\Work\data-science-projects\kafka-producer-consumer
docker compose start
python ..\kafka-producer-consumer-system\topic-08-error-handling\producer.py
```

Producer should connect and send events successfully.

---

## Exercise 2 — Consumer connection failure

**Simulate:** Kafka stopped while setting up consumer.

```powershell
docker compose stop
python consumer.py
```

**Expected:** Same retry pattern as producer, then exit with error after max attempts.

**Recover:** Start Kafka and run consumer again.

---

## Exercise 3 — Kafka shutdown during operation

**Terminal 1 — consumer running:**

```powershell
python consumer.py
```

**Terminal 2 — stop Kafka mid-run:**

```powershell
cd d:\Work\data-science-projects\kafka-producer-consumer
docker compose stop
```

**Expected:** Consumer logs poll errors and retries. Restart Kafka — consumer recovers on next poll.

**Terminal 2 — restart Kafka:**

```powershell
docker compose start
```

---

## Exercise 4 — Invalid JSON messages

**Inject a bad message** using the Kafka console producer:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh `
  --bootstrap-server localhost:9092 `
  --topic user-events
```

Type: `this is not json` and press Enter. Type `Ctrl+C` to exit.

**Run consumer:**

```powershell
python consumer.py
```

**Expected log:**

```
[WARNING] Invalid JSON — skipping message: this is not json (error: ...)
```

Valid JSON events are still processed normally.

---

## Exercise 5 — Send retry on transient failure

**Simulate:** Stop Kafka, start producer (will fail connect), OR stop Kafka between producer connect and send.

1. Start Kafka and run `python producer.py` successfully.
2. Stop Kafka: `docker compose stop`
3. Run producer again — observe connection retries.
4. Start Kafka during retries — producer may succeed on a later attempt.

---

## Reset consumer group

```powershell
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh `
  --bootstrap-server localhost:9092 `
  --group error-handling-consumer-group `
  --delete
```

---

## Deliverables

| Deliverable | File |
|-------------|------|
| Updated producer with error handling | `producer.py` |
| Updated consumer with error handling | `consumer.py` |
| Error handling examples | `error-examples.md` |
| Screenshot | `screenshots/error-handling.png` |

Capture logs showing retry attempts, invalid JSON skip, or recovery after Kafka restart.

---

## Expected outcome

You understand basic fault tolerance: retry transient failures, log errors clearly, skip bad data without crashing, and recover when Kafka comes back online.

See [error-examples.md](error-examples.md) for a quick reference of all error scenarios.
