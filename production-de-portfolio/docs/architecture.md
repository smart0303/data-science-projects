# Architecture

## Data flow

1. **Extract** — `etl/extract.py` pulls `/users` and `/posts` from JSONPlaceholder with exponential-backoff retries.
2. **Transform** — `etl/transform.py` normalizes fields and stamps `extracted_at`.
3. **Load** — `etl/load.py` upserts into `raw.users` and `raw.posts`.
4. **Model** — dbt builds staging views, `dim_users` / `fct_posts`, and `analytics_post_metrics`.
5. **Validate** — dbt tests enforce keys and relationships; `run_quality_checks.py` checks counts, orphans, and freshness.
6. **Consume** — analysts run queries in `sql/analytics/` against marts and analytics schemas.

## Schema design

```
raw          staging       marts              analytics
────────     ─────────     ─────────          ─────────────────────────
users   →    stg_users →   dim_users
posts   →    stg_posts →   fct_posts    →     analytics_post_metrics
```

## Orchestration

Airflow `LocalExecutor` coordinates tasks in a single DAG with shared retry policy. PostgreSQL hosts both warehouse data (`de_portfolio`) and Airflow metadata (`airflow_meta`).

## Operational concerns

- **Idempotency** — upserts on primary keys allow safe replays.
- **Observability** — structured ETL logs; Airflow task logs for orchestration.
- **Failure handling** — API retries (Python), task retries (Airflow), dbt test gate before analytics QA.
