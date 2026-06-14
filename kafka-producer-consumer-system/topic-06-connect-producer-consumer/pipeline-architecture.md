# Topic 06 — Pipeline Architecture Diagram

Complete message flow for the **user-events** end-to-end demo (Topics 04–06).

---

## System overview

```mermaid
flowchart LR
    subgraph T2["Terminal 2 — Producer Service"]
        PROD["producer.py<br/>(Python KafkaProducer)"]
    end

    subgraph Docker["Docker — localhost:9092"]
        subgraph Broker["Kafka Broker (KRaft)"]
            subgraph Topic["Topic: user-events"]
                P0["Partition 0"]
                P1["Partition 1"]
                P2["Partition 2"]
            end
        end
    end

    subgraph T1["Terminal 1 — Consumer Service"]
        CONS["consumer.py<br/>(Python KafkaConsumer)"]
    end

    PROD -->|"11 messages<br/>per run"| P0
    PROD --> P1
    PROD --> P2

    P0 -->|"poll & read"| CONS
    P1 --> CONS
    P2 --> CONS
```

**Producer → Kafka Topic → Consumer**

The producer and consumer never talk to each other directly. Kafka sits in the middle as a durable message bus.

---

## Message flow (sequence)

What happens when you run the demo:

```mermaid
sequenceDiagram
    participant P as producer.py<br/>(Terminal 2)
    participant K as Kafka Broker<br/>(user-events)
    participant C as consumer.py<br/>(Terminal 1)

    Note over C: Step 1 — Consumer starts first
    C->>K: Subscribe to user-events<br/>(group: user-events-consumer-group)
    K-->>C: Subscription confirmed

    Note over P: Step 2 — Producer publishes
    P->>K: send("Hello from Kafka producer!")
    K-->>P: Ack (partition, offset)
    P->>K: send("Test message 1/10")
    K-->>P: Ack
    P->>K: send("Test message 2/10") ... x10
    K-->>P: Ack (each message)

    Note over K,C: Step 3 — Consumer receives
    C->>K: poll()
    K-->>C: Batch of records
    C->>C: Print "Received: ..." to terminal

    Note over C: Step 4 — Offsets committed
    C->>K: Commit offset (auto)

    Note over P,C: Step 5 — Multiple runs
    P->>K: Another batch of 11 messages
    K-->>C: New records via poll()
```

---

## Component map

| Component | File | Role |
|-----------|------|------|
| **Kafka broker** | `kafka-producer-consumer/docker-compose.yml` | Stores and serves messages |
| **Topic** | `user-events` (3 partitions) | Named channel for user activity events |
| **Producer** | `topic-04-simple-producer/producer.py` | Publishes text messages to the topic |
| **Consumer** | `topic-05-simple-consumer/consumer.py` | Subscribes and displays messages |
| **Consumer group** | `user-events-consumer-group` | Tracks read progress (offsets) |

---

## Data path detail

```mermaid
flowchart TB
    subgraph ProducerApp["Producer Application"]
        direction TB
        A1["Create KafkaProducer"]
        A2["Serialize string → UTF-8 bytes"]
        A3["send(topic, value)"]
        A4["future.get() — wait for broker ack"]
        A1 --> A2 --> A3 --> A4
    end

    subgraph Kafka["Apache Kafka"]
        direction TB
        B1["Receive record"]
        B2["Assign partition<br/>(round-robin, no key)"]
        B3["Append to partition log"]
        B4["Replicate (factor 1 locally)"]
        B1 --> B2 --> B3 --> B4
    end

    subgraph ConsumerApp["Consumer Application"]
        direction TB
        C1["Create KafkaConsumer"]
        C2["Subscribe to user-events"]
        C3["poll() — fetch records"]
        C4["Deserialize bytes → string"]
        C5["Print to terminal"]
        C6["Commit offset"]
        C1 --> C2 --> C3 --> C4 --> C5 --> C6
    end

    A4 --> B1
    B4 --> C3
```

---

## ASCII diagram (portable reference)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         END-TO-END KAFKA PIPELINE                           │
└─────────────────────────────────────────────────────────────────────────────┘

  TERMINAL 2                         DOCKER                         TERMINAL 1
  ──────────                    ───────────────                    ──────────

┌──────────────┐              ┌─────────────────┐              ┌──────────────┐
│  producer.py │              │  Kafka Broker   │              │ consumer.py  │
│              │   publish    │  localhost:9092 │    poll      │              │
│ KafkaProducer│─────────────▶│                 │◀─────────────│ KafkaConsumer│
│              │              │  Topic:         │              │              │
│ 11 msgs/run  │              │  user-events    │              │ print msgs   │
└──────────────┘              │  ┌────┬────┬────┐ │              └──────────────┘
       │                      │  │ P0 │ P1 │ P2 │ │                     │
       │                      │  └──┬─┴──┬─┴──┬─┘ │                     │
       │                      └─────┼────┼────┼────┘                     │
       │                            │    │    │                          │
       └────────────────────────────┴────┴────┴──────────────────────────┘
                    Messages flow through topic — not direct service calls

  Sent: 'Test message 3/10'  →  stored at partition 1, offset N  →  Received: 'Test message 3/10'
```

---

## How services communicate through Kafka

```mermaid
flowchart TB
    subgraph Today["This demo (2 Python scripts)"]
        PA[producer.py]
        KB[(Kafka)]
        CA[consumer.py]
        PA --> KB --> CA
    end

    subgraph Production["Real-world (same pattern)"]
        WEB[Web App]
        MOB[Mobile App]
        K2[(Kafka Cluster)]
        ANA[Analytics Service]
        REC[Recommendation Engine]
        WH[Data Warehouse]

        WEB --> K2
        MOB --> K2
        K2 --> ANA
        K2 --> REC
        K2 --> WH
    end
```

The demo uses two terminal scripts, but the pattern is identical at scale: **many producers, one topic, many independent consumers**.

---

## Key takeaways

1. **Decoupling** — Producer and consumer only know the topic name and broker address, not each other.
2. **Durability** — Messages persist in the topic log until retention expires.
3. **Scalability** — Partitions allow parallel reads and writes.
4. **Replay** — New consumer groups can re-read history from any offset.
5. **Resilience** — If the consumer stops, messages accumulate; it catches up on restart.

---

## Related reading

- [pipeline-demo.md](pipeline-demo.md) — step-by-step demo instructions
- [Topic 01 architecture](../topic-01-kafka-basics/kafka-architecture.md) — general Kafka cluster diagrams
- [Topic 04 producer](../topic-04-simple-producer/producer.py)
- [Topic 05 consumer](../topic-05-simple-consumer/consumer.py)
