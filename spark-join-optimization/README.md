# Spark Join Optimization

Optimize large joins and partition-heavy workloads with PySpark: build multiple datasets, run multi-table joins, compare `repartition()` vs `coalesce()`, cache transformed data, and inspect execution plans.

## Project layout

```
spark-join-optimization/
├── data/
│   ├── customers.csv     # Dimension: 20 customers
│   └── products.csv      # Dimension: 20 products
├── pipeline.py           # Five-step optimization demo
├── output/               # Written after run (gitignored)
└── requirements.txt
```

Orders (~80k rows) are generated in Spark at runtime to create a partition-heavy fact table for shuffle and join exercises.

## Prerequisites

- **Python 3.10–3.12** (PySpark 3.5+)
- **Java 8 or 11** (required by Spark). Set `JAVA_HOME` if Spark cannot find Java.

## Setup

```powershell
cd spark-join-optimization
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
| 1 | Load customers/products; generate large orders fact table | `spark.read.csv()`, `spark.range()` |
| 2 | Inner joins and aggregations by customer and category | `join()`, `groupBy()`, `agg()` |
| 3 | Compare partition counts and timing after reshape | `repartition()`, `coalesce()` |
| 4 | Cache joined data; measure cold vs warm actions | `cache()`, `unpersist()` |
| 5 | Print formatted physical plans and shuffle settings | `explain()`, Spark conf |

## Repartition vs coalesce

| Operation | Shuffle | Use when |
|-----------|---------|----------|
| `repartition(n)` | Full shuffle | Increase parallelism or balance skew before heavy joins |
| `coalesce(n)` | Narrow merge (no full shuffle when reducing) | Reduce partitions before write or after aggregation |

## Output tables

After a successful run:

- `output/customer_summary/` — revenue and order counts per customer
- `output/category_summary/` — revenue per product category

Each folder contains a `part-*.csv` file with the result set.

## Spark UI

While the pipeline runs, open the Spark UI link printed in the console (typically `http://localhost:4040`) to inspect stages, shuffle read/write, and cached RDD storage from Step 4.
