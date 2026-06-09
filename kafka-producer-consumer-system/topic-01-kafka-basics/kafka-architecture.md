# Kafka Architecture Diagram

## High-level data flow

The core pattern every Kafka system follows:

```mermaid
flowchart LR
    subgraph Producers
        P1[Producer A]
        P2[Producer B]
    end

    subgraph KafkaCluster["Kafka Cluster"]
        subgraph TopicOrders["Topic: orders"]
            PO0[Partition 0]
            PO1[Partition 1]
            PO2[Partition 2]
        end
        B1[(Broker 1)]
        B2[(Broker 2)]
        B3[(Broker 3)]
    end

    subgraph Consumers
        C1[Consumer 1]
        C2[Consumer 2]
        C3[Consumer 3]
    end

    P1 --> PO0
    P2 --> PO1
    P2 --> PO2

    PO0 --- B1
    PO1 --- B2
    PO2 --- B3

    PO0 --> C1
    PO1 --> C2
    PO2 --> C3
```

**Producer → Kafka Topic → Consumer**

| Step | What happens |
|------|----------------|
| 1 | Producers publish records to a topic. |
| 2 | Kafka routes each record to a partition (by key hash or round-robin). |
| 3 | Brokers persist records in partition logs and replicate for fault tolerance. |
| 4 | Consumers in a group read assigned partitions and commit offsets. |

---

## Cluster architecture (brokers, topics, replication)

```mermaid
flowchart TB
    subgraph Clients
        PR[Producers]
        CO[Consumers]
    end

    subgraph Cluster["Kafka Cluster"]
        direction TB
        ZK["KRaft / ZooKeeper<br/>(cluster metadata)"]

        subgraph Broker1["Broker 1"]
            L0["Partition 0 — Leader"]
            F1["Partition 1 — Follower"]
        end

        subgraph Broker2["Broker 2"]
            F0["Partition 0 — Follower"]
            L1["Partition 1 — Leader"]
        end

        subgraph Broker3["Broker 3"]
            L2["Partition 2 — Leader"]
            F2["Partition 2 — Follower"]
        end
    end

    PR --> L0
    PR --> L1
    PR --> L2

    L0 --> F0
    L1 --> F1
    L2 --> F2

    L0 --> CO
    L1 --> CO
    L2 --> CO

    ZK -.-> Broker1
    ZK -.-> Broker2
    ZK -.-> Broker3
```

Each **partition** has one leader (handles reads/writes) and followers that replicate data. If a broker fails, a follower is elected leader so the topic stays available.

---

## Consumer groups

```mermaid
flowchart LR
    subgraph Topic["Topic: events (3 partitions)"]
        P0[P0]
        P1[P1]
        P2[P2]
    end

    subgraph GroupA["Consumer Group A"]
        CA1[Consumer A1]
        CA2[Consumer A2]
        CA3[Consumer A3]
    end

    subgraph GroupB["Consumer Group B"]
        CB1[Consumer B1]
    end

    P0 --> CA1
    P1 --> CA2
    P2 --> CA3

    P0 --> CB1
    P1 --> CB1
    P2 --> CB1
```

- **Within one consumer group**, each partition is read by **at most one** consumer (load balancing).
- **Different consumer groups** each get **all** messages independently (pub/sub broadcast).

---

## ASCII overview (portable reference)

```
┌─────────────┐     ┌──────────────────────────────────────────┐     ┌─────────────┐
│  Producer   │────▶│  Topic: "user-events"                    │────▶│  Consumer   │
│  (app/API)  │     │  ┌──────────┐ ┌──────────┐ ┌──────────┐ │     │  (service)  │
└─────────────┘     │  │ Part. 0  │ │ Part. 1  │ │ Part. 2  │ │     └─────────────┘
                    │  │ offset:  │ │ offset:  │ │ offset:  │ │
┌─────────────┐     │  │ 0,1,2,…  │ │ 0,1,2,…  │ │ 0,1,2,…  │ │     ┌─────────────┐
│  Producer   │────▶│  └────┬─────┘ └────┬─────┘ └────┬─────┘ │────▶│  Consumer   │
└─────────────┘     │       │            │            │       │     │  (analytics)│
                    │       ▼            ▼            ▼       │     └─────────────┘
                    │   Broker 1    Broker 2    Broker 3      │
                    └──────────────────────────────────────────┘
```

---

## Related reading

- [kafka-concepts.md](kafka-concepts.md) — one-page concept summary and use cases
