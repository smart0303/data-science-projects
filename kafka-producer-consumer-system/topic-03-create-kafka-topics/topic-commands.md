# Topic 03 — Kafka Topic Commands Reference

All commands run against the local broker at `localhost:9092` (Docker Compose setup from Topic 02).

**Prerequisite:** Kafka must be running.

```powershell
cd d:\Work\data-science-projects\kafka-producer-consumer
docker compose up -d
```

Commands below use `docker exec` so you do not need Kafka installed on the host — the CLI runs inside the `kafka` container.

---

## 1. Connect to Kafka (verify broker)

List existing topics — confirms connectivity:

```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server localhost:9092 `
  --list
```

---

## 2. Create `user-events` topic

```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server localhost:9092 `
  --create `
  --topic user-events `
  --partitions 3 `
  --replication-factor 1
```

| Flag | Value | Meaning |
|------|-------|---------|
| `--partitions` | 3 | Three parallel sub-streams; up to 3 consumers in one group can read in parallel |
| `--replication-factor` | 1 | Single copy (appropriate for local single-broker setup) |

---

## 3. Create `order-events` topic

```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server localhost:9092 `
  --create `
  --topic order-events `
  --partitions 3 `
  --replication-factor 1
```

---

## 4. List all topics

```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server localhost:9092 `
  --list
```

Expected output (order may vary):

```
order-events
user-events
```

---

## 5. Describe a topic (optional — inspect configuration)

```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server localhost:9092 `
  --describe `
  --topic user-events
```

Shows partition count, leader broker, and replication status.

---

## 6. Delete and recreate a test topic

### Create test topic

```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server localhost:9092 `
  --create `
  --topic test-topic `
  --partitions 1 `
  --replication-factor 1
```

### Delete test topic

```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server localhost:9092 `
  --delete `
  --topic test-topic
```

### Recreate test topic (with different partition count)

```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server localhost:9092 `
  --create `
  --topic test-topic `
  --partitions 2 `
  --replication-factor 1
```

### Verify recreation

```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server localhost:9092 `
  --describe `
  --topic test-topic
```

Confirm `PartitionCount: 2` in the output.

### Clean up test topic (optional)

```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server localhost:9092 `
  --delete `
  --topic test-topic
```

---

## Command summary

| Action | Command flags |
|--------|----------------|
| List topics | `--list` |
| Create topic | `--create --topic <name> --partitions <n> --replication-factor <n>` |
| Describe topic | `--describe --topic <name>` |
| Delete topic | `--delete --topic <name>` |

All commands require `--bootstrap-server localhost:9092` (or `kafka:29092` from inside the Docker network).

---

## Screenshot deliverable

Run the list command and capture the terminal:

```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server localhost:9092 `
  --list
```

Save the screenshot as `topic-03-create-kafka-topics/screenshots/topics-list.png`.

You should see at least:

```
order-events
user-events
```

---

## Automated script

Run all Topic 03 steps in one go:

```powershell
cd d:\Work\data-science-projects\kafka-producer-consumer-system\topic-03-create-kafka-topics
.\manage-topics.ps1
```
