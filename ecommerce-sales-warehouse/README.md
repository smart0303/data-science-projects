# E-Commerce Sales Data Warehouse

A small **star-schema** data warehouse for an e-commerce store. It practices fact tables, dimensions, and analytical marts in PostgreSQL.

## Business questions

| Question | Mart / source |
|----------|----------------|
| Daily sales revenue per product, category, and customer | `marts/daily_sales_mart.sql` |
| Average order value per customer | `marts/customer_lifetime_value_mart.sql` |
| Total sales by region (country) | `marts/sales_by_category_mart.sql` (category); region via customer country in daily mart |

## Star schema

```
                    dim_date
                        |
fact_sales ---- dim_customer
      |
 dim_product
```

### Fact

- **fact_sales** — one row per order line: quantity, price, total_amount

### Dimensions

- **dim_date** — calendar attributes (day, month, quarter, year)
- **dim_customer** — customer profile and geography
- **dim_product** — product, category, brand, list price

## Quick start

Requires [PostgreSQL](https://www.postgresql.org/) (local or Docker).

```bash
# Create database (optional)
createdb ecommerce_dw

# Run in order
psql -d ecommerce_dw -f sql/01_schema.sql
psql -d ecommerce_dw -f sql/02_seed_data.sql
psql -d ecommerce_dw -f marts/daily_sales_mart.sql
psql -d ecommerce_dw -f marts/sales_by_category_mart.sql
psql -d ecommerce_dw -f marts/customer_lifetime_value_mart.sql
```

### Docker (optional)

```bash
docker run --name ecommerce-pg -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=ecommerce_dw -p 5432:5432 -d postgres:16
set PGPASSWORD=postgres
psql -h localhost -U postgres -d ecommerce_dw -f sql/01_schema.sql
psql -h localhost -U postgres -d ecommerce_dw -f sql/02_seed_data.sql
```

## Project layout

```
ecommerce-seales-warehouse/
├── README.md
├── sql/
│   ├── 01_schema.sql      # dimensions + fact
│   └── 02_seed_data.sql   # sample rows
└── marts/
    ├── daily_sales_mart.sql
    ├── sales_by_category_mart.sql
    └── customer_lifetime_value_mart.sql
```

## Sample data snapshot

After seeding, two orders on 2026-05-01–02: Alice buys a laptop; Bob buys two mice. Use the mart queries to aggregate revenue, category mix, and customer metrics.
