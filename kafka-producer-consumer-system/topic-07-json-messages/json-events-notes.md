# Topic 07 — Send JSON Messages

## Goal

Exchange structured event data through Kafka using JSON serialization.

## Prerequisites

- Kafka running (Topic 02)
- Topics `user-events` and `order-events` created (Topic 03)
- Python 3.10+

## Install dependencies

```powershell
cd d:\Work\data-science-projects\kafka-producer-consumer-system\topic-07-json-messages
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## JSON concepts

See [json-fundamentals.md](json-fundamentals.md) for:

- JSON syntax and types
- Python `dict` ↔ JSON conversion with `json.dumps()` / `json.loads()`
- Sample event schemas

## Run the demo

Use two terminals.

### Terminal 1 — JSON consumer

```powershell
cd d:\Work\data-science-projects\kafka-producer-consumer-system\topic-07-json-messages
.\.venv\Scripts\Activate.ps1
python json_consumer.py
```

### Terminal 2 — JSON producer

```powershell
cd d:\Work\data-science-projects\kafka-producer-consumer-system\topic-07-json-messages
.\.venv\Scripts\Activate.ps1
python json_producer.py
```

Or use the setup script:

```powershell
.\run-json-demo.ps1
```

Then start consumer and producer in separate terminals as shown above.

## Event types sent

| Event | Topic | Key fields |
|-------|-------|------------|
| **User Signup** | `user-events` | `user_id`, `email`, `name` |
| **User Login** | `user-events` | `user_id`, `ip_address`, `device` |
| **Order Created** | `order-events` | `order_id`, `user_id`, `amount`, `currency` |

Each producer run sends **6 events** (2 of each type for two users).

## Expected consumer output

```
────────────────────────────────────────
USER SIGNUP
  User ID : u-1001
  Name    : Alice Smith
  Email   : alice@example.com
  Time    : 2026-06-10T10:00:00Z
  [topic=user-events, partition=0, offset=12]

────────────────────────────────────────
USER LOGIN
  User ID : u-1001
  IP      : 192.168.1.10
  Device  : web
  Time    : 2026-06-10T10:05:00Z
  [topic=user-events, partition=1, offset=8]

────────────────────────────────────────
ORDER CREATED
  Order ID : ord-5001
  User ID  : u-1001
  Amount   : USD 49.99
  Items    : 2
  Time     : 2026-06-10T10:10:00Z
  [topic=order-events, partition=0, offset=3]
```

## How serialization works

**Producer** (`json_producer.py`):

```python
# Python dict
event = {"event_type": "user_login", "user_id": "u-1001", ...}

# Serializer: dict → JSON string → UTF-8 bytes
value_serializer=lambda value: json.dumps(value).encode("utf-8")
```

**Consumer** (`json_consumer.py`):

```python
# Deserializer: bytes → JSON string → Python dict
value_deserializer=lambda value: json.loads(value.decode("utf-8"))

# Access fields
event["event_type"]  # "user_login"
```

## Reset consumer group (optional)

To re-read all JSON events from the beginning:

```powershell
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh `
  --bootstrap-server localhost:9092 `
  --group json-events-consumer-group `
  --delete
```

## Deliverables

| Deliverable | File |
|-------------|------|
| Producer sending JSON | `json_producer.py` |
| Consumer processing JSON | `json_consumer.py` |
| JSON fundamentals | `json-fundamentals.md` |
| Screenshot | `screenshots/json-pipeline.png` |

## Troubleshooting

| Issue | Fix |
|-------|-----|
| `JSONDecodeError` | Consumer received non-JSON text from earlier Topic 04/05 runs — reset group or use a new topic |
| `UnknownTopicOrPartitionException` | Create topics with Topic 03 `manage-topics.ps1` |
| Wrong event format | Ensure producer and consumer both use `json.dumps` / `json.loads` |

## Expected outcome

You can create Python dictionaries, serialize them to JSON, publish structured events to Kafka, parse JSON in the consumer, and display formatted output for multiple event types.
