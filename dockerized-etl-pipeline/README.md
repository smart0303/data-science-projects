# Dockerized ETL Pipeline

A containerized Python ETL pipeline that reads raw sales CSV data, cleans and aggregates it, and writes summary files to an output directory.

## Pipeline

| Stage | Description |
|-------|-------------|
| **Extract** | Read `data/sales_raw.csv` |
| **Transform** | Compute line revenue, aggregate by category and order date |
| **Load** | Write cleaned rows and summaries to `output/` |

## Project layout

```
dockerized-etl-pipeline/
├── data/
│   └── sales_raw.csv
├── etl/
│   └── pipeline.py
├── output/                 # created at runtime (gitignored)
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

## Step 1: ETL Python script

The pipeline lives in `etl/pipeline.py`. Run it locally:

```powershell
cd dockerized-etl-pipeline
python etl/pipeline.py
```

By default it reads `data/sales_raw.csv` and writes timestamped CSV files under `output/`.

## Step 2: Dockerfile

The `Dockerfile` uses `python:3.12-slim`, copies the ETL script and seed data, and sets the default command to run the pipeline.

## Step 3: Build Docker image

```powershell
cd dockerized-etl-pipeline
docker build -t dockerized-etl-pipeline .
```

Or build via Compose:

```powershell
docker compose build
```

## Step 4: docker-compose.yml

`docker-compose.yml` defines an `etl` service that:

- Builds the image from the local `Dockerfile`
- Mounts `./data` read-only into the container
- Mounts `./output` so results appear on the host

## Step 5: Run ETL inside container

```powershell
docker compose up --build
```

One-off run (container exits after ETL completes):

```powershell
docker compose run --rm etl
```

Or with plain Docker:

```powershell
docker run --rm `
  -v "${PWD}/data:/app/data:ro" `
  -v "${PWD}/output:/app/output" `
  dockerized-etl-pipeline
```

## Expected output

After a successful run, `output/` contains files like:

- `sales_clean_YYYYMMDD_HHMMSS.csv` — cleaned line items with revenue
- `sales_by_category_YYYYMMDD_HHMMSS.csv` — totals per category
- `sales_by_day_YYYYMMDD_HHMMSS.csv` — totals per order date

Example category summary:

| category | order_count | units_sold | revenue |
|----------|-------------|------------|---------|
| Electronics | 5 | 12 | 1698.89 |
| Furniture | 3 | 4 | 938.50 |
