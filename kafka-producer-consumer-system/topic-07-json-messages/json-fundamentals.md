# Topic 07 — JSON Fundamentals for Kafka Events

## What is JSON?

**JSON** (JavaScript Object Notation) is a text format for structured data. It is the most common serialization format for Kafka events because it is human-readable, language-agnostic, and easy to debug.

```json
{
  "event_type": "user_login",
  "user_id": "u-1001",
  "timestamp": "2026-06-10T12:00:00Z"
}
```

---

## JSON building blocks

| Type | Example | Python equivalent |
|------|---------|-------------------|
| **Object** | `{"key": "value"}` | `dict` |
| **Array** | `[1, 2, 3]` | `list` |
| **String** | `"hello"` | `str` |
| **Number** | `42`, `3.14` | `int`, `float` |
| **Boolean** | `true`, `false` | `True`, `False` |
| **Null** | `null` | `None` |

Rules:

- Keys must be **double-quoted strings**
- Strings use double quotes (`"`), not single quotes
- No trailing commas after the last item
- Nested objects and arrays are allowed

---

## Python dictionaries → JSON

Kafka producers work with Python objects; the broker stores **bytes**. JSON sits in the middle.

```python
import json

# 1. Create a Python dictionary
event = {
    "event_type": "user_signup",
    "user_id": "u-1001",
    "email": "alice@example.com",
}

# 2. Convert dict → JSON string
json_string = json.dumps(event)
# '{"event_type": "user_signup", "user_id": "u-1001", "email": "alice@example.com"}'

# 3. Convert JSON string → bytes (for Kafka)
payload = json_string.encode("utf-8")
```

Reverse in the consumer:

```python
# bytes → JSON string → dict
event = json.loads(message.value.decode("utf-8"))
print(event["event_type"])  # user_signup
```

---

## Sample events for this project

### User Signup → `user-events` topic

```json
{
  "event_type": "user_signup",
  "user_id": "u-1001",
  "email": "alice@example.com",
  "name": "Alice Smith",
  "timestamp": "2026-06-10T10:00:00Z"
}
```

### User Login → `user-events` topic

```json
{
  "event_type": "user_login",
  "user_id": "u-1001",
  "ip_address": "192.168.1.10",
  "device": "web",
  "timestamp": "2026-06-10T10:05:00Z"
}
```

### Order Created → `order-events` topic

```json
{
  "event_type": "order_created",
  "order_id": "ord-5001",
  "user_id": "u-1001",
  "amount": 49.99,
  "currency": "USD",
  "items": 2,
  "timestamp": "2026-06-10T10:10:00Z"
}
```

---

## Why JSON for Kafka events?

| Benefit | Explanation |
|---------|-------------|
| **Readable** | Inspect messages in logs and console consumers without special tools |
| **Flexible** | Add fields without breaking all consumers (schema evolution) |
| **Universal** | Every language has a JSON library |
| **Debuggable** | Easy to print and validate during development |

Production systems often use **Avro** or **Protobuf** with a schema registry for stricter contracts. JSON is the best starting point for learning.

---

## Event envelope pattern

Each message includes an `event_type` field so consumers can route or handle different events:

```
┌─────────────────────────────────────┐
│  event_type  (discriminator)        │
│  timestamp   (when it happened)       │
│  ...fields specific to event...     │
└─────────────────────────────────────┘
```

The consumer reads `event_type` and formats output accordingly.

---

## Self-check

- How do you convert a Python dict to a JSON string?
- How do you parse a JSON string back to a dict?
- Why include `event_type` in every message?
- Which topic should `order_created` events go to?

See [json-events-notes.md](json-events-notes.md) for the hands-on demo.
