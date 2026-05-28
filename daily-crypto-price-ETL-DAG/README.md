# Daily Crypto Price ETL DAG

Apache Airflow pipeline that fetches the top 10 cryptocurrencies by market cap from [CoinGecko](https://www.coingecko.com/), saves them to a dated CSV, and prints a summary.

## DAG: `daily_crypto_prices`

| Task | Description |
|------|-------------|
| `fetch_crypto_prices` | GET CoinGecko `/coins/markets` (USD, top 10) |
| `save_to_csv` | Write `data/crypto_prices_YYYY-MM-DD.csv` |
| `print_summary` | Log row count, price stats, and ranked list |

Schedule: `@daily`

## Setup (Step 1)

```powershell
cd daily-crypto-price-ETL-DAG
.\scripts\setup.ps1
```

This creates a virtual environment, installs Airflow + requests, and initializes the metadata database.

## Run the DAG (Step 3)

```powershell
.\scripts\run_dag.ps1
```

Or manually:

```powershell
$env:AIRFLOW_HOME = (Get-Location).Path
$env:AIRFLOW__CORE__DAGS_FOLDER = "$env:AIRFLOW_HOME\dags"
$env:AIRFLOW__CORE__LOAD_EXAMPLES = "False"
.\.venv\Scripts\Activate.ps1
airflow dags test daily_crypto_prices 2026-05-27
```

Output CSV files are written to `data/`.

## Optional: Web UI

```powershell
airflow standalone
```

Open http://localhost:8080 and trigger `daily_crypto_prices` from the UI.
