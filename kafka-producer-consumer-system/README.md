# Kafka Producer & Consumer System

A hands-on learning project for Apache Kafka — from core concepts to building producers and consumers.

## Topics

| Topic | Focus | Status |
|-------|-------|--------|
| **01 — Kafka Basics** | Producer, Topic, Consumer, architecture | Complete |
| **02 — Local Kafka Setup** | Docker Desktop, Docker Compose, run Kafka locally | Complete |
| **03 — Create Kafka Topics** | Create, list, describe, delete topics | Complete |
| 04 — Producer | Writing messages to Kafka | Planned |
| 05 — Consumer | Reading messages from Kafka | Planned |

## Topic 01 deliverables

- [Kafka concepts (one-page note)](topic-01-kafka-basics/kafka-concepts.md)
- [Kafka architecture diagram](topic-01-kafka-basics/kafka-architecture.md)

## Topic 02 deliverables

- [Local Kafka setup notes](topic-02-local-kafka-setup/setup-notes.md)
- [Docker Compose config](../kafka-producer-consumer/docker-compose.yml) in `kafka-producer-consumer/`

## Topic 03 deliverables

- [Topic concepts and purpose](topic-03-create-kafka-topics/topic-notes.md)
- [Kafka topic commands reference](topic-03-create-kafka-topics/topic-commands.md)
- [Automated topic setup script](topic-03-create-kafka-topics/manage-topics.ps1)
- Screenshot: `topic-03-create-kafka-topics/screenshots/topics-list.png` (capture after running the script)

## Expected outcome

After Topic 01, you should be able to explain the core data flow:

**Producer → Kafka Topic → Consumer**
