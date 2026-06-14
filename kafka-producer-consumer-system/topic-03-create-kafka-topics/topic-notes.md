# Topic 03 — Create Your First Kafka Topic

## What is a Kafka Topic?

A **Kafka topic** is a named, append-only log where producers write records and consumers read them. Topics are the central unit of organization in Kafka — think of them as **channels** or **feeds** for a specific kind of event.

Key properties:

| Property | Description |
|----------|-------------|
| **Name** | A unique identifier (e.g. `user-events`). Producers and consumers reference topics by name. |
| **Partitions** | Topics are split into partitions for parallelism. Records within one partition are strictly ordered. |
| **Replication** | Each partition can be replicated across brokers for fault tolerance (factor of 1 in local dev). |
| **Retention** | Messages are kept for a configurable time or size limit, allowing replay. |

### How topics fit in the flow

```
Producer  →  user-events (topic)  →  Consumer group A
          →  order-events (topic)  →  Consumer group B
```

Producers publish to a topic by name. Consumers subscribe to one or more topics. Multiple consumer groups can read the same topic independently — each maintains its own offset.

---

## Topics created in this project

### `user-events`

**Purpose:** Stream of user activity and behavioral events from web and mobile applications.

| Aspect | Detail |
|--------|--------|
| **Producers** | Web app, mobile app, authentication service |
| **Example events** | `page_view`, `button_click`, `signup`, `login`, `profile_update` |
| **Consumers** | Analytics pipeline, recommendation engine, real-time dashboards |
| **Why a separate topic** | User events are high-volume and often processed by analytics systems that do not need order/payment data. Isolating them keeps schemas and retention policies independent. |

### `order-events`

**Purpose:** Stream of order lifecycle events from the e-commerce or checkout system.

| Aspect | Detail |
|--------|--------|
| **Producers** | Order service, payment gateway, fulfillment service |
| **Example events** | `order_created`, `payment_confirmed`, `order_shipped`, `order_cancelled` |
| **Consumers** | Inventory service, shipping notifications, data warehouse (CDC), fraud detection |
| **Why a separate topic** | Order events have stricter ordering and durability requirements. Downstream systems (inventory, billing) subscribe only to commerce events without noise from user clicks. |

### `test-topic` (practice only)

**Purpose:** Temporary topic used to practice **delete** and **recreate** operations. Not used in the production pipeline. Deleted and recreated during Topic 03 exercises to confirm topic lifecycle commands work.

---

## Topic design principles (quick reference)

1. **One domain per topic** — `user-events` vs `order-events` keeps event types and consumers separated.
2. **Choose partition count for throughput** — more partitions = more parallel consumers (3 partitions is a common starting point for learning).
3. **Replication factor** — use `1` for local single-broker setups; use `3` in production clusters.
4. **Explicit creation** — prefer `kafka-topics.sh --create` over relying on auto-create so partition count and replication are intentional.

---

## Self-check

- What is the difference between a topic and a partition?
- Why would you use separate topics for user activity vs orders?
- What command lists all topics on the broker?
- How do you delete a topic and create it again with different settings?

See [topic-commands.md](topic-commands.md) for the full command reference.
