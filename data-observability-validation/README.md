# Data Observability & Validation

Monitor sensor event data with Great Expectations: duplicate detection, freshness checks, anomaly validation, and generated reports.

## Project layout

```
data-observability-validation/
├── data/
│   └── sensor_events.csv
├── checks/
│   ├── duplicates.py          # Step 2: duplicate detection
│   ├── freshness.py             # Step 3: freshness monitoring
│   └── anomalies.py             # Step 4: anomaly detection
├── gx/                          # Great Expectations project (created on setup)
├── reports/                     # Step 5: generated validation reports
├── config.py
├── setup_suite.py               # Step 1: build validation suite
├── validate.py                  # Run validation + generate reports
├── reporting.py
└── requirements.txt
```

## Step 1: Install and build validation suite

Use **Python 3.11 or 3.12**.

```powershell
cd data-observability-validation
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python setup_suite.py
```

This scaffolds a Great Expectations project with:

- Pandas datasource: `observability_datasource`
- Data asset: `sensor_events`
- Expectation suite: `sensor_events_suite`
- Checkpoint: `observability_checkpoint`

## Step 2: Duplicate checks

Defined in `checks/duplicates.py` and registered in the suite:

- `expect_column_values_to_be_unique` on `event_id`
- Composite-key scan on `(sensor_id, event_timestamp)` for business-level duplicates

## Step 3: Freshness monitoring

Defined in `checks/freshness.py`:

- Latest `loaded_at` within 24 hours
- Latest `event_timestamp` within 7 days
- Custom freshness assessment included in validation reports

## Step 4: Anomaly detection

Defined in `checks/anomalies.py`:

- Value range check on `metric_value` (0–100)
- Mean bounds to catch distribution drift
- Z-score outlier scan (threshold: 3.0) reported as a custom check

## Step 5: Generate validation reports

```powershell
python validate.py
```

If the suite has not been created yet, `validate.py` runs setup automatically.

Example output:

```
Validation success: True
Great Expectations success: True
Evaluated expectations: 12
Successful expectations: 12
Failed expectations: 0
  [PASS] expect_table_columns_to_match_ordered_list
  [PASS] expect_column_values_to_be_unique
  ...
  [PASS] freshness: Latest loaded_at is 2.1h old (limit 24h).
JSON report: reports/validation_report_20260607T120000Z.json
HTML report: reports/validation_report_20260607T120000Z.html
```

Exit code is `0` on success and `1` if any expectation or custom check fails.

## Sample data

`data/sensor_events.csv` contains IoT-style sensor readings with `loaded_at` timestamps for freshness monitoring and `metric_value` for anomaly validation.

## Configuration

Thresholds and column names live in `config.py`:

| Setting | Default |
|---------|---------|
| `MAX_LOAD_AGE_HOURS` | 24 |
| `MAX_EVENT_AGE_DAYS` | 7 |
| `Z_SCORE_THRESHOLD` | 3.0 |
| `METRIC_VALUE_MIN` / `MAX` | 0 / 100 |
