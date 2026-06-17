# Topic 09 — User Signup Pipeline Architecture

## System diagram

```mermaid
flowchart TB
    subgraph Users["New Users"]
        U1[Alice]
        U2[Bob]
        U3[Carol]
        U4[David]
        U5[Eva]
    end

    subgraph SignupApp["Signup Service"]
        API["signup_producer.py"]
        EVT["Create user_signup event"]
        API --> EVT
    end

    subgraph KafkaCluster["Kafka — localhost:9092"]
        subgraph Topic["user-events"]
            P0["Partition 0"]
            P1["Partition 1"]
            P2["Partition 2"]
        end
    end

    subgraph EmailApp["Welcome Email Service"]
        CON["signup_consumer.py"]
        PARSE["Parse JSON event"]
        DISPLAY["Display user info"]
        EMAIL["Simulate welcome email"]
        CON --> PARSE --> DISPLAY --> EMAIL
    end

    U1 & U2 & U3 & U4 & U5 --> API
    EVT -->|"publish"| P0 & P1 & P2
    P0 & P1 & P2 -->|"subscribe + poll"| CON
```

---

## Message flow sequence

```mermaid
sequenceDiagram
    participant User
    participant Signup as Signup Service
    participant Kafka as Kafka (user-events)
    participant Email as Welcome Email Service

    Note over Email: Service starts first
    Email->>Kafka: Subscribe (group: welcome-email-service)

    User->>Signup: Register account
    Signup->>Signup: Build user_signup event
    Signup->>Kafka: Publish event (key=user_id)
    Kafka-->>Signup: Ack (partition, offset)

    Email->>Kafka: poll()
    Kafka-->>Email: user_signup event
    Email->>Email: Display user information
    Email->>Email: Simulate welcome email
    Email->>Kafka: Commit offset

    Note over User,Email: Repeat for each of 5 users
```

---

## Event envelope

Every signup message follows the same structure:

```
┌─────────────────────────────────────────┐
│ event_type  : "user_signup"  (router)   │
│ event_id    : unique id per event       │
│ user_id     : partition key             │
│ name        : display name              │
│ email       : welcome email recipient   │
│ plan        : subscription tier         │
│ timestamp   : ISO-8601 UTC              │
└─────────────────────────────────────────┘
```

The consumer filters on `event_type == "user_signup"` and ignores other event types on the same topic.

---

## ASCII overview

```
  NEW USERS                SIGNUP SERVICE              KAFKA                EMAIL SERVICE
  ─────────                ──────────────              ─────                ─────────────

  Alice  ──┐
  Bob    ──┤
  Carol  ──┼──▶  signup_producer.py  ──▶  user-events  ──▶  signup_consumer.py
  David  ──┤         │                      topic              │
  Eva    ──┘         │                      │                  ├─ Display user info
                     │                      │                  └─ Send welcome email
                     └─ user_signup JSON events (5 per run)
```

---

## Scaling path (real world)

This demo uses one producer and one consumer. In production:

| Component | Scale approach |
|-----------|----------------|
| Signup Service | Multiple instances behind a load balancer |
| Kafka topic | More partitions for higher throughput |
| Email Service | Consumer group with N instances (one partition each) |
| New features | Add consumers for analytics, CRM sync, fraud checks — same topic |

---

## Related files

- [demo-guide.md](demo-guide.md) — run instructions and demo recording
- [signup_producer.py](signup_producer.py)
- [signup_consumer.py](signup_consumer.py)
