# Project Summary

## Kafka Producer & Consumer System

A hands-on event-driven application demonstrating **Apache Kafka** fundamentals through a real-world **user signup pipeline**.

---

## What it does

When a new user signs up, the **Signup Service** publishes a structured `user_signup` event to Kafka. The **Welcome Email Service** consumes that event independently and processes a welcome email — without any direct API call between services.

```
Signup Service  →  user-events (Kafka)  →  Welcome Email Service
```

---

## Skills demonstrated

- **Apache Kafka** — topics, partitions, offsets, consumer groups
- **Producer pattern** — publish JSON events with partition keys
- **Consumer pattern** — subscribe, poll, deserialize, process
- **Event-driven architecture** — decoupled microservices via message bus
- **Error handling** — connection retries, invalid message skipping, structured logging
- **Docker** — local Kafka deployment with Docker Compose
- **Python** — clean module structure with shared `src/` library

---

## Tech stack

| Component | Technology |
|-----------|------------|
| Message broker | Apache Kafka 3.9 (KRaft) |
| Language | Python 3.10+ |
| Kafka client | kafka-python |
| Infrastructure | Docker Compose |
| Data format | JSON |

---

## Highlights for portfolio

1. **End-to-end pipeline** — working producer and consumer communicating through Kafka
2. **Structured events** — JSON schema with `event_type`, `event_id`, timestamps
3. **Production patterns** — retries, logging, consumer groups, graceful shutdown
4. **Clean codebase** — refactored `src/` library with config, events, producer, and consumer modules
5. **Full curriculum** — 10-topic learning path from Kafka basics to portfolio deployment

---

## Quick demo

```powershell
docker compose up -d
pip install -r requirements.txt
python consumer.py    # Terminal 1
python producer.py    # Terminal 2
```

5 users register; 5 welcome emails are simulated in real time.

---

## Repository structure

| Path | Purpose |
|------|---------|
| `producer.py` / `consumer.py` | Portfolio entry points |
| `src/` | Shared Kafka client library |
| `docker-compose.yml` | Local Kafka broker |
| `ARCHITECTURE.md` | System diagrams |
| `topic-*/` | Progressive learning exercises |

---

## Learning outcomes

After completing this project, you can:

- Explain how **Producer → Topic → Consumer** data flow works
- Run Kafka locally and manage topics
- Build Python producers and consumers with kafka-python
- Exchange structured JSON events between services
- Handle common failures with retries and defensive parsing
- Design a simple event-driven application

---

*Built as a portfolio project demonstrating event streaming and message-driven architecture.*
