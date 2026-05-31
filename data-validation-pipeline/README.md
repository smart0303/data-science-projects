# Data Validation Pipeline with Great Expectations

Validate ingestion data using schema checks, null/duplicate rules, and freshness validation.

## Project layout

```
data-validation-pipeline/
├── data/
│   └── ingestion_orders.csv
├── gx/                          # Great Expectations project (created on setup)
├── setup_suite.py               # Steps 2–4: suite + expectations
├── validate.py                  # Step 5: run validation pipeline
└── requirements.txt
```

## Step 1: Install Great Expectations

Use **Python 3.11 or 3.12**.

```powershell
cd data-validation-pipeline
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Step 2: Create validation suite

```powershell
python setup_suite.py
```

This scaffolds a Great Expectations project and registers:

- Pandas datasource: `ingestion_datasource`
- Data asset: `orders`
- Expectation suite: `ingestion_orders_suite`
- Checkpoint: `ingestion_checkpoint`

## Step 3: Add schema checks

Defined in `setup_suite.py`:

- Required columns: `order_id`, `product`, `category`, `quantity`, `unit_price`, `order_date`, `ingested_at`
- Type checks on numeric columns (`order_id`, `quantity`, `unit_price`)

## Step 4: Add null/duplicate checks

Also in `setup_suite.py`:

- `not_null` on all required columns
- `unique` on `order_id`
- Freshness: latest `ingested_at` within 7 days and latest `order_date` within 30 days

## Step 5: Run validation pipeline

```powershell
python validate.py
```

If the suite has not been created yet, `validate.py` runs setup automatically.

Example output:

```
Validation success: True
Evaluated expectations: 14
Successful expectations: 14
Failed expectations: 0
  [PASS] expect_table_columns_to_match_ordered_list
  [PASS] expect_column_values_to_not_be_null
  ...
```

Exit code is `0` on success and `1` if any expectation fails.

## Sample data

`data/ingestion_orders.csv` contains daily e-commerce order rows with an `ingested_at` timestamp used for freshness checks.
