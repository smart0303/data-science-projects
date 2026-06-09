# Kafka Basics — One-Page Notes

## What is Apache Kafka?

Apache Kafka is a **distributed event streaming platform**. It lets applications publish (produce) streams of records and subscribe to (consume) those streams in real time. Kafka stores messages durably on disk, replicates them across servers for fault tolerance, and scales horizontally to handle millions of events per second.

Think of Kafka as a **high-throughput, fault-tolerant message bus** that decouples systems: producers write data without knowing who will read it, and consumers read data without knowing who wrote it.

---

## Core concepts

| Concept | Role |
|---------|------|
| **Producer** | An application that **publishes** messages (records) to one or more Kafka topics. |
| **Topic** | A **named category** or feed where messages are stored. Topics are split into **partitions** for parallelism and ordering within a partition. |
| **Consumer** | An application that **subscribes** to topics and processes the messages it receives. |
| **Broker** | A Kafka server that stores topic data and serves producers and consumers. A cluster has multiple brokers. |
| **Consumer group** | A set of consumers that **cooperate** to read a topic — each partition is assigned to one consumer in the group. |

### The fundamental flow

```
Producer  →  Topic (partitions)  →  Consumer
```

1. A **producer** sends a record (key, value, timestamp) to a **topic**.
2. Kafka appends the record to a **partition** (chosen by key or round-robin).
3. A **consumer** polls the topic, reads new records, and processes them.
4. The consumer commits its **offset** (position in the partition) so it can resume after restarts.

---

## How Kafka works (short explanation)

Kafka runs as a **cluster of brokers**. Each topic is divided into **partitions**, and each partition is an ordered, append-only log. Producers write to the end of a partition; consumers read sequentially and track their position via **offsets**.

Replication ensures durability: each partition has one **leader** broker (handles reads/writes) and zero or more **followers** that copy the leader’s data. If a broker fails, a follower can be promoted.

Because data is persisted and consumers control their read pace, Kafka supports both **real-time streaming** and **replay** — consumers can re-read historical data from any offset.

---

## Why Kafka is used in modern systems

- **Decoupling** — Services communicate through topics instead of direct API calls, reducing tight coupling and cascading failures.
- **Scalability** — Partitions and consumer groups let you scale reads and writes independently across many machines.
- **Durability & replay** — Messages are retained (hours to forever), so new consumers can catch up or reprocess history for analytics and recovery.
- **High throughput** — Sequential disk I/O and batching handle very large event volumes at low latency.
- **Real-time pipelines** — Kafka connects operational systems (databases, apps) to stream processors, warehouses, and monitoring in near real time.

---

## Three real-world use cases

1. **Activity / event tracking** — User clicks, page views, and app events are produced to Kafka; analytics, recommendation engines, and data lakes consume them for dashboards and ML features (e.g., LinkedIn, Netflix-style pipelines).

2. **Log aggregation** — Microservices ship logs and metrics to Kafka; centralized systems (Elasticsearch, Splunk, observability stacks) consume and index them for search and alerting.

3. **Change data capture (CDC) & integration** — Database changes are streamed through Kafka so downstream services (search indexes, caches, warehouses) stay in sync without polling source systems — common in e-commerce inventory and payment flows.

---

## Quick glossary

| Term | Meaning |
|------|---------|
| **Record / message** | A single event (key + value + metadata). |
| **Partition** | Ordered sub-stream of a topic; unit of parallelism. |
| **Offset** | Integer position of a record within a partition. |
| **ZooKeeper / KRaft** | Cluster coordination (metadata, leader election); modern Kafka uses KRaft mode. |

---

## Self-check

Can you explain this chain without looking at your notes?

**Producer → Kafka Topic → Consumer**

- What does each component do?
- Why are topics split into partitions?
- What happens if a consumer crashes and restarts?

See [kafka-architecture.md](kafka-architecture.md) for the full architecture diagram.
