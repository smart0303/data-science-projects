# Topic 10 — Portfolio Project & GitHub Review

## Goal

Prepare the Kafka project for your GitHub portfolio: refactored code, clean structure, documentation, and public repository.

---

## What changed in Topic 10

| Before | After |
|--------|-------|
| Code spread across topic folders | Shared `src/` library + root entry points |
| Docker in separate folder | `docker-compose.yml` at project root |
| Multiple `requirements.txt` | Single `requirements.txt` |
| Learning README | Portfolio-ready `README.md` |
| Topic-specific scripts | `scripts/setup.ps1` and `scripts/run-demo.ps1` |

---

## Portfolio project structure

```
kafka-producer-consumer-system/
├── README.md                 # Portfolio README (start here)
├── PROJECT_SUMMARY.md        # One-page summary for recruiters
├── ARCHITECTURE.md           # System diagrams
├── producer.py               # Signup Service (refactored)
├── consumer.py               # Welcome Email Service (refactored)
├── docker-compose.yml        # Local Kafka
├── requirements.txt
├── src/                      # Shared library
│   ├── config.py
│   ├── events.py
│   ├── producer.py           # KafkaEventProducer
│   └── consumer.py           # KafkaEventConsumer
├── scripts/
│   ├── setup.ps1
│   └── run-demo.ps1
├── docs/
│   └── CURRICULUM.md         # Topics 01–09 index
└── topic-01 … topic-09/      # Learning exercises
```

---

## Refactored code

### Shared library (`src/`)

| Module | Responsibility |
|--------|----------------|
| `config.py` | Broker address, topics, retry settings |
| `events.py` | Event schemas, validation, sample users |
| `producer.py` | `KafkaEventProducer` — connect, send with retries |
| `consumer.py` | `KafkaEventConsumer` — subscribe, poll, dispatch |

### Entry points

| File | Service |
|------|---------|
| `producer.py` | Registers users, publishes `user_signup` events |
| `consumer.py` | Receives signups, displays user info, sends welcome emails |

---

## Push to GitHub

### Option A — Standalone repository (recommended for portfolio)

Create a new public repo with only this project:

```powershell
cd d:\Work\data-science-projects\kafka-producer-consumer-system

git init
git add .
git commit -m "Kafka Producer & Consumer System — event-driven signup pipeline"

gh repo create kafka-producer-consumer-system --public --source=. --push
```

### Option B — Subfolder in existing monorepo

If keeping `data-science-projects` as one repo, ensure the Kafka README is discoverable:

1. Add a link in the root `README.md` to this project
2. Use the project summary in your portfolio as a direct link:
   `https://github.com/YOUR_USER/data-science-projects/tree/main/kafka-producer-consumer-system`

### Pre-push checklist

- [ ] Remove any `.env` files or secrets (none should exist)
- [ ] Add demo screenshots to `topic-09-mini-final-demo/screenshots/` or root `screenshots/`
- [ ] Verify `docker compose up -d` works
- [ ] Verify `python producer.py` and `python consumer.py` work
- [ ] Review `README.md` — update GitHub username if needed
- [ ] Set repository description on GitHub: *Event-driven user signup pipeline with Apache Kafka and Python*

---

## Portfolio presentation tips

### README badges (optional)

Add to the top of `README.md`:

```markdown
![Python](https://img.shields.io/badge/Python-3.10+-blue)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.9-black)
![Docker](https://img.shields.io/badge/Docker-Compose-blue)
```

### What to highlight on your resume / LinkedIn

> Built an event-driven user signup pipeline with Apache Kafka and Python. Implemented producer and consumer services exchanging JSON events through Kafka topics, with connection retries, consumer groups, and Docker-based local deployment.

### Screenshots to include

| Screenshot | Shows |
|------------|-------|
| Architecture diagram | From `ARCHITECTURE.md` (render on GitHub) |
| Consumer + Producer terminals | End-to-end demo |
| Topic list | `docker exec kafka ... --list` |

---

## Deliverables

| Deliverable | Location |
|-------------|----------|
| Refactored producer | `producer.py` + `src/producer.py` |
| Refactored consumer | `consumer.py` + `src/consumer.py` |
| Portfolio README | `README.md` |
| Architecture diagram | `ARCHITECTURE.md` |
| Project summary | `PROJECT_SUMMARY.md` |
| Setup instructions | `README.md` + `scripts/setup.ps1` |
| GitHub guide | This file |

---

## Expected outcome

You have a complete, portfolio-ready Kafka project that demonstrates **Producer**, **Topic**, and **Consumer** concepts in a real event-driven application — ready to share on GitHub.
