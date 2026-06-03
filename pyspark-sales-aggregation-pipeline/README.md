# PySpark Sales Aggregation Pipeline

Process CSV sales data with PySpark DataFrames: load, transform, aggregate with `groupBy()` / `agg()`, and write summary tables.

## Project layout

```
pyspark-sales-aggregation-pipeline/
├── data/sales.csv          # Input sales records
├── pipeline.py             # Five-step pipeline
├── output/                 # Written after run (gitignored)
└── requirements.txt
```

## Prerequisites

- **Python 3.10–3.12** (PySpark 3.5+)
- **Java 8 or 11** (required by Spark). Set `JAVA_HOME` if Spark cannot find Java.

## Setup

```powershell
cd pyspark-sales-aggregation-pipeline
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Run the pipeline

```powershell
python pipeline.py
```

## Pipeline steps

| Step | Description | Key APIs |
|------|-------------|----------|
| 1 | Create local Spark session | `SparkSession.builder` |
| 2 | Load `data/sales.csv` | `spark.read.csv()` |
| 3 | Parse dates, compute `line_total`, filter invalid rows | `withColumn()`, `filter()` |
| 4 | Aggregate by region, category, region+category, and month | `groupBy()`, `agg()` |
| 5 | Save CSV tables under `output/` | `DataFrame.write.csv()` |

## Output tables

After a successful run:

- `output/sales_by_region/` — total sales, units, and orders per region
- `output/sales_by_category/` — metrics per product category
- `output/sales_by_region_category/` — sales by region and category
- `output/sales_monthly_summary/` — monthly totals

Each folder contains one or more part CSV files plus Spark metadata; read the `part-*.csv` file for the result set.
