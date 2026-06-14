# Topic 06 — End-to-End Kafka Pipeline Demo

## Goal

Connect the producer and consumer into a complete pipeline and verify messages flow from one service to another through Kafka — without direct coupling.

## What you are building

```
Terminal 2 (Producer)  →  Kafka (user-events)  →  Terminal 1 (Consumer)
```

This mirrors how real microservices communicate: the producer does not know which consumer reads the data, and the consumer does not call the producer directly.

---

## Prerequisites

| Requirement | Location |
|-------------|----------|
| Docker Desktop running | Topic 02 |
| `user-events` topic created | Topic 03 |
| Producer ready | `topic-04-simple-producer/producer.py` |
| Consumer ready | `topic-05-simple-consumer/consumer.py` |

---

## Step-by-step demo

### Step 1 — Start Kafka

```powershell
cd d:\Work\data-science-projects\kafka-producer-consumer
docker compose up -d
docker compose ps
```

Wait until the `kafka` container shows **running** (and **healthy** after ~30 seconds).

Optional — ensure the topic exists:

```powershell
cd d:\Work\data-science-projects\kafka-producer-consumer-system\topic-03-create-kafka-topics
.\manage-topics.ps1
```

---

### Step 2 — Reset consumer group (fresh demo)

For a clean end-to-end test, delete the consumer group so the consumer reads from the beginning:

```powershell
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh `
  --bootstrap-server localhost:9092 `
  --group user-events-consumer-group `
  --delete
```

Skip this step if you only want to see **new** messages from this demo run.

---

### Step 3 — Terminal 1: Start the consumer

```powershell
cd d:\Work\data-science-projects\kafka-producer-consumer-system\topic-05-simple-consumer
.\.venv\Scripts\Activate.ps1
python consumer.py
```

Leave this terminal open. You should see:

```
Subscribed to topic 'user-events' (group: user-events-consumer-group).
Reading messages... Press Ctrl+C to stop.
```

The consumer is now **waiting** for messages.

---

### Step 4 — Terminal 2: Run the producer (first batch)

Open a **second terminal**:

```powershell
cd d:\Work\data-science-projects\kafka-producer-consumer-system\topic-04-simple-producer
.\.venv\Scripts\Activate.ps1
python producer.py
```

The producer sends **11 messages** (1 hello + 10 test messages).

---

### Step 5 — Verify messages arrive at the consumer

Watch **Terminal 1**. You should see 11 lines like:

```
Received: 'Hello from Kafka producer!' [topic=user-events, partition=0, offset=0]
Received: 'Test message 1/10' [topic=user-events, partition=1, offset=0]
...
Received: 'Test message 10/10' [topic=user-events, partition=2, offset=3]
```

**Verification checklist:**

| Check | Expected |
|-------|----------|
| Message count | 11 `Received:` lines per producer run |
| Topic name | `user-events` on every line |
| Producer confirmation | Terminal 2 shows 11 `Sent:` lines + `All messages sent successfully.` |
| Partitions | Messages may appear on partitions 0, 1, or 2 (round-robin without keys) |

---

### Step 6 — Send multiple message batches

With the consumer **still running** in Terminal 1, run the producer again in Terminal 2:

```powershell
python producer.py
python producer.py
python producer.py
```

Each run adds 11 more messages. Terminal 1 should show **33 additional** `Received:` lines (3 runs × 11 messages).

This demonstrates Kafka as a **durable log** — the consumer receives new events as they are published.

---

### Step 7 — Stop and restart the consumer

1. In Terminal 1, press **Ctrl+C** to stop the consumer.
2. Run the producer once more in Terminal 2 (`python producer.py`).
3. Restart the consumer in Terminal 1 (`python consumer.py`).

**Expected behavior:**

- On restart, the consumer **resumes from the last committed offset**.
- It picks up the 11 messages sent while it was stopped.
- Messages already read before Ctrl+C are **not** repeated.

This shows how consumer groups track progress with offsets.

---

## Quick start script

Run setup steps automatically (Kafka + topic check + instructions):

```powershell
cd d:\Work\data-science-projects\kafka-producer-consumer-system\topic-06-connect-producer-consumer
.\run-pipeline-demo.ps1
```

Then follow the on-screen instructions for Terminal 1 and Terminal 2.

---

## Why this matters

| Without Kafka | With Kafka |
|---------------|------------|
| Service A calls Service B directly (tight coupling) | Service A publishes to a topic; Service B subscribes independently |
| If B is down, A fails or must retry | Messages are stored; B catches up when it restarts |
| Adding a third service requires changing A | New consumers subscribe to the same topic without changing the producer |

Kafka decouples **who sends** from **who reads**, which is the foundation of event-driven architecture.

---

## Deliverables

| Deliverable | Location |
|-------------|----------|
| End-to-end pipeline demo | Steps above |
| Architecture diagram | [pipeline-architecture.md](pipeline-architecture.md) |
| Screenshot | `screenshots/pipeline-demo.png` — both terminals showing sent and received messages |

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| Consumer shows nothing | Reset consumer group (Step 2) or run producer while consumer is active |
| `NoBrokersAvailable` | `docker compose up -d` and wait for healthy status |
| Duplicate messages on restart | Normal if offsets were not committed — stop with Ctrl+C and wait a second before restart |
| Message count mismatch | Count `Sent:` in Terminal 2 vs `Received:` in Terminal 1 — should match per run |

---

## Expected outcome

You can start Kafka, run a consumer and producer in separate terminals, send multiple message batches, verify end-to-end delivery, and explain how services communicate asynchronously through Kafka topics.

See [pipeline-architecture.md](pipeline-architecture.md) for the complete message flow diagram.
