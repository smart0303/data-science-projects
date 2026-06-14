# Kafka Producer & Consumer System

A hands-on learning project for Apache Kafka — from core concepts to building producers and consumers.

## Topics

| Topic | Focus | Status |
|-------|-------|--------|
| **01 — Kafka Basics** | Producer, Topic, Consumer, architecture | Complete |
| **02 — Local Kafka Setup** | Docker Desktop, Docker Compose, run Kafka locally | Complete |
| **03 — Create Kafka Topics** | Create, list, describe, delete topics | Complete |
| **04 — Simple Producer** | Python producer, publish messages to topics | Complete |
| **05 — Simple Consumer** | Python consumer, read messages from topics | Complete |
| **06 — Connect Pipeline** | End-to-end producer → Kafka → consumer demo | Complete |

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

## Topic 04 deliverables

- [Working producer.py](topic-04-simple-producer/producer.py)
- Screenshot: `topic-04-simple-producer/screenshots/producer-output.png`

## Topic 05 deliverables

- [Consumer setup and usage notes](topic-05-simple-consumer/consumer-notes.md)
- [Working consumer.py](topic-05-simple-consumer/consumer.py)
- [Run script](topic-05-simple-consumer/run-consumer.ps1)
- Screenshot: `topic-05-simple-consumer/screenshots/consumer-output.png`

## Topic 06 deliverables

- [End-to-end pipeline demo guide](topic-06-connect-producer-consumer/pipeline-demo.md)
- [Pipeline architecture diagram](topic-06-connect-producer-consumer/pipeline-architecture.md)
- [Demo setup script](topic-06-connect-producer-consumer/run-pipeline-demo.ps1)
- Screenshot: `topic-06-connect-producer-consumer/screenshots/pipeline-demo.png`

## Expected outcome

After completing Topics 01–06, you should understand and run the full data flow:

**Producer → Kafka Topic → Consumer**

- Explain core Kafka concepts (Topic 01)
- Run Kafka locally with Docker (Topic 02)
- Create and manage topics (Topic 03)
- Publish messages with Python (Topic 04)
- Consume messages with Python (Topic 05)
- Connect producer and consumer in an end-to-end pipeline (Topic 06)
