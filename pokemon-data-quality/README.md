# Pokémon Data Quality with dbt

A small dbt project that stages Pokémon data, runs data-quality tests, and documents lineage.

## Project layout

```
pokemon-data-quality/
├── dbt_project.yml
├── profiles.yml
├── seeds/pokemon_raw.csv
├── models/staging/stg_pokemon.sql
├── models/staging/schema.yml    # column tests on staging
├── tests/assert_positive_stats.sql
└── requirements.txt
```

## Step 1: Setup dbt environment

Use **Python 3.11 or 3.12** (dbt does not yet support 3.14).

```powershell
cd pokemon-data-quality
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
$env:DBT_PROFILES_DIR = (Get-Location).Path
```

`profiles.yml` in this folder points DuckDB at `pokemon.duckdb`. Run dbt from this directory so it picks up the profile.

## Step 2: Load seed and build staging

```powershell
dbt seed
dbt run --select stg_pokemon
```

## Step 3: Tests

- **Schema tests** (`schema.yml`): `unique` / `not_null` on `id`, `not_null` on key columns, `accepted_values` on `legendary`.
- **Singular test** (`tests/assert_positive_stats.sql`): fails if any stat is zero or negative.

## Step 4: Run tests

```powershell
dbt test
```

## Step 5: Explore lineage graph

```powershell
dbt docs generate
dbt docs serve
```

Open the URL shown in the terminal (usually http://localhost:8080). In the **Lineage** tab you will see:

`pokemon_raw` (seed) → `stg_pokemon` (view), with tests attached to `stg_pokemon`.
