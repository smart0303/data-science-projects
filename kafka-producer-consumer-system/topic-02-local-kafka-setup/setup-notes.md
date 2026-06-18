# Topic 02 — Local Kafka Setup

## Goal

Install and run Apache Kafka locally using Docker Desktop and Docker Compose.

## Project folder

```
kafka-producer-consumer-system/
├── docker-compose.yml      # Kafka broker (project root)
└── scripts/setup.ps1       # Setup helper
```

> **Note:** Docker Compose was consolidated to the project root in Topic 10. The legacy `kafka-producer-consumer/` folder is kept for reference.

## Tasks checklist

### 1. Install Docker Desktop

Download from [https://www.docker.com/products/docker-desktop/](https://www.docker.com/products/docker-desktop/) or run:

```powershell
winget install Docker.DockerDesktop --accept-package-agreements --accept-source-agreements
```

After installation, **start Docker Desktop** from the Start menu and wait until it shows "Docker Desktop is running".

### 2. Verify Docker installation

```powershell
docker --version
docker compose version
```

Expected output (versions may differ):

```
Docker version 27.x.x, build ...
Docker Compose version v2.x.x
```

### 3. Run Kafka

```powershell
cd d:\Work\data-science-projects\kafka-producer-consumer-system
docker compose up -d
```

### 4. Verify Kafka containers are running

```powershell
docker compose ps
```

Expected: `kafka` container with status `running` (and `healthy` after ~30s).

### 5. Stop and restart Kafka

```powershell
# Stop
docker compose stop

# Restart
docker compose start

# Or full down/up cycle
docker compose down
docker compose up -d
```

### 6. Optional: run all verification steps

```powershell
.\verify-setup.ps1
```

## Deliverables

| Deliverable | How to capture |
|-------------|----------------|
| Screenshot of running Kafka containers | Run `docker compose ps` and screenshot the terminal |
| Working Docker Compose configuration | `docker-compose.yml` in this project folder |

## Architecture

This setup uses **Apache Kafka in KRaft mode** (no Zookeeper):

- **Image:** `apache/kafka:3.9.0`
- **Broker port:** `localhost:9092`
- **Single broker** suitable for local development and learning

## Troubleshooting

| Issue | Fix |
|-------|-----|
| `docker` not recognized | Restart terminal after installing Docker Desktop; ensure Docker Desktop is running |
| Port 9092 already in use | Stop other Kafka instances or change the host port in `docker-compose.yml` |
| Container exits immediately | Run `docker compose logs kafka` to inspect errors |
| WSL 2 required (Windows) | Enable WSL 2 in Docker Desktop settings during first launch |
