# Incremental Sales ETL with dbt

A dbt project that **incrementally loads** sales transactions from a raw PostgreSQL table into staging, then **aggregates** daily metrics in a mart.

## Project layout

```
incremental-sales-etl-dbt/
├── dbt_project.yml
├── profiles.yml
├── requirements.txt
├── sql/
│   ├── 01_raw_sales.sql          # raw.sales_raw DDL + seed
│   └── 02_incremental_load.sql   # optional second batch
├── models/
│   ├── staging/
│   │   ├── _sources.yml
│   │   ├── stg_sales.sql         # incremental staging
│   │   └── schema.yml
│   └── marts/
│       ├── sales_summary.sql     # daily aggregates
│       └── schema.yml
└── tests/
    └── assert_positive_line_totals.sql
```

## Lineage

```
raw.sales_raw  →  staging.stg_sales  →  marts.sales_summary
   (source)         (incremental)         (table / aggregates)
```

## Step 1: Setup dbt environment

Requires **Python 3.11 or 3.12** and [PostgreSQL](https://www.postgresql.org/) (local or Docker).

```powershell
cd incremental-sales-etl-dbt
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
$env:DBT_PROFILES_DIR = (Get-Location).Path
```

### PostgreSQL database

```powershell
# Local Postgres
createdb sales_dw

# Or Docker
docker run --name sales-pg -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=sales_dw -p 5432:5432 -d postgres:16
```

Override connection settings if needed:

```powershell
$env:DBT_PG_HOST = "localhost"
$env:DBT_PG_USER = "postgres"
$env:DBT_PG_PASSWORD = "postgres"
$env:DBT_PG_DATABASE = "sales_dw"
```

## Step 2: Prepare raw data table

```powershell
$env:PGPASSWORD = "postgres"
psql -h localhost -U postgres -d sales_dw -f sql/01_raw_sales.sql
```

This creates `raw.sales_raw` and loads three sample orders.

## Step 3: dbt models

### 3a — Staging (`stg_sales`, incremental)

- Reads from `{{ source('raw', 'sales_raw') }}`
- On incremental runs, only rows with `created_at` newer than the staging table max are processed
- Adds `line_total = quantity * price`
- `unique_key: order_id` with merge strategy for idempotent loads

### 3b — Mart (`sales_summary`)

Daily aggregates: order count, customer count, quantity, revenue, and average line total.

## Step 4: Run dbt

```powershell
dbt debug
dbt run
dbt test
```

Expected after the first run:

| order_date | order_count | total_revenue |
|------------|-------------|---------------|
| 2026-05-01 | 2           | 46.00         |
| 2026-05-02 | 1           | 31.50         |

Inspect results:

```sql
SELECT * FROM staging.stg_sales ORDER BY order_id;
SELECT * FROM marts.sales_summary ORDER BY order_date;
```

### Incremental demo (second batch)

```powershell
psql -h localhost -U postgres -d sales_dw -f sql/02_incremental_load.sql
dbt run --select stg_sales
dbt run --select sales_summary
```

`stg_sales` should add two new rows; `sales_summary` should include `2026-05-03`.

## Step 5: Stretch goals (included)

- **Tests**: `schema.yml` column tests + `assert_positive_line_totals` singular test
- **Documentation**: `dbt docs generate` && `dbt docs serve`
- **Incremental load script**: `sql/02_incremental_load.sql`

```powershell
dbt docs generate
dbt docs serve
```
