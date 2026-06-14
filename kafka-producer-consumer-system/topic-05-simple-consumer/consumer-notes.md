# Topic 05 — Build a Simple Consumer

## Goal

Read messages from Kafka using a Python consumer application.

## Prerequisites

1. **Kafka running** — `docker compose up -d` in `kafka-producer-consumer/`
2. **Topic exists** — `user-events` (Topic 03)
3. **Messages in topic** — run the Topic 04 producer, or publish while the consumer is running
4. **Python 3.10+**

## Install dependencies

```powershell
cd d:\Work\data-science-projects\kafka-producer-consumer-system\topic-05-simple-consumer
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Run the consumer

```powershell
python consumer.py
```

The consumer:

1. **Connects** to Kafka at `localhost:9092`
2. **Subscribes** to the `user-events` topic
3. **Reads** messages in a poll loop
4. **Displays** each message with topic, partition, and offset
5. **Stops** cleanly on `Ctrl+C`

## Test with the producer

Use two terminals.

**Terminal 1 — start the consumer:**

```powershell
cd d:\Work\data-science-projects\kafka-producer-consumer-system\topic-05-simple-consumer
.\.venv\Scripts\Activate.ps1
python consumer.py
```

**Terminal 2 — publish messages:**

```powershell
cd d:\Work\data-science-projects\kafka-producer-consumer-system\topic-04-simple-producer
.\.venv\Scripts\Activate.ps1
python producer.py
```

Terminal 1 should print the 11 messages from the producer as they arrive.

## Stop and restart the consumer

This demonstrates **offset tracking** with a consumer group.

### First run

```powershell
python consumer.py
```

- With a new consumer group, `auto_offset_reset="earliest"` reads from the start of the topic.
- Messages appear in the terminal.
- Press **Ctrl+C** to stop. Offsets are committed automatically.

### Restart

```powershell
python consumer.py
```

- The consumer **resumes from the last committed offset**.
- Already-read messages are **not** shown again.
- Run the producer again in another terminal — only **new** messages appear.

### Reset offsets (optional)

To re-read all messages from the beginning, delete the consumer group:

```powershell
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh `
  --bootstrap-server localhost:9092 `
  --group user-events-consumer-group `
  --delete
```

Then start the consumer again.

## How consumer.py works

| Step | Code concept |
|------|----------------|
| Connect | `KafkaConsumer(bootstrap_servers=["localhost:9092"])` |
| Subscribe | Pass topic name `user-events` to the constructor |
| Consumer group | `group_id` tracks read progress across restarts |
| Deserialize | `value_deserializer` converts UTF-8 bytes to Python strings |
| Read | `consumer.poll()` fetches new records in batches |
| Display | Print `message.value`, partition, and offset |
| Stop | `Ctrl+C` closes the consumer and commits offsets |

## Deliverables

| Deliverable | Location |
|-------------|----------|
| Working `consumer.py` | This folder |
| Screenshot of received messages | `screenshots/consumer-output.png` |

Capture Terminal 1 showing `Received:` lines after running the producer.

## Troubleshooting

| Issue | Fix |
|-------|-----|
| `NoBrokersAvailable` | Start Kafka with `docker compose up -d` |
| No messages appear | Run Topic 04 producer, or reset consumer group (see above) |
| `ModuleNotFoundError: kafka` | Activate venv and `pip install -r requirements.txt` |

## Expected outcome

You can subscribe to a Kafka topic, read messages in real time, display them in the terminal, and understand how stop/restart behavior works with consumer group offsets.
