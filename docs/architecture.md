# Architecture & Data Flow

## Component Overview

```mermaid
flowchart TD
    GEN["Data Generator"]
    MINIO["MinIO\nObject Storage\n:9000 API | :9001 Console"]
    RAW["raw-data bucket\nsales_YYYYMMDD.csv"]
    AF_WEB["Airflow Webserver\n:8080"]
    AF_SCH["Airflow Scheduler\nLocalExecutor"]
    PROC["processed-data bucket\n(archived CSVs)"]

    subgraph PG ["PostgreSQL :5432"]
        DB_SALES[("DB: sales\nordres | returned_orders\npurchased_products\npipeline_runs")]
        DB_AF[("DB: airflow\n(Airflow metadata)")]
        DB_MB[("DB: metabase\n(Metabase config)")]
    end

    MB["Metabase:3000 Dashboards & BI"]

    GEN -->|"Upload CSV"| RAW
    RAW -->|"S3KeySensor"| AF_SCH
    AF_WEB <-->|"REST API / UI"| AF_SCH
    AF_SCH -->|"download → validate → transform → load"| DB_SALES
    AF_SCH -->|"archive"| PROC
    AF_SCH -->|"metadata"| DB_AF
    DB_SALES -->|"SQL queries"| MB
    MB -->|"config store"| DB_MB
```

---

## DAG Task Graph

```mermaid
flowchart LR
    D["download_from_minio\n⬇ S3 → temp file"]
    V["validate_csv\n schema & nulls"]
    T["transform_data\n clean + revenue"]
    L["load_to_postgres\n bulk upsert"]
    A["archive_file\n raw → processed"]

    D --> V --> T --> L --> A
```

---

## Infrastructure

| Layer        | Technology              | Version       | Port        |
|--------------|-------------------------|---------------|-------------|
| Database     | PostgreSQL              | 16-alpine     | 5432        |
| Object Store | MinIO                   | latest        | 9000 / 9001 |
| Orchestrator | Apache Airflow          | 2.9.1         | 8080        |
| BI / Dashboards | Metabase             | latest        | 3000        |
| Generator    | Python 3.11-slim        | —             | —           |

---

## Data Model

```mermaid
erDiagram
    ORDERS {
        varchar order_id PK
        varchar customer_id
        varchar product
        varchar category
        varchar region
        int     quantity
        numeric unit_price
        numeric discount
        numeric total_revenue
        date    order_date
        varchar status
        timestamptz ingested_at
    }

    RETURNED_ORDERS {
        serial  return_id PK
        varchar order_id  FK
        varchar return_reason
        date    return_date
        numeric refund_amount
        timestamptz processed_at
    }

    PURCHASED_PRODUCTS {
        varchar product PK
        varchar category
        bigint  total_units_sold
        numeric total_revenue
        numeric avg_discount
        date    last_purchased_date
        timestamptz updated_at
    }

    PIPELINE_RUNS {
        serial  run_id PK
        varchar dag_run_id
        varchar file_processed
        int     rows_inserted
        int     rows_skipped
        varchar status
        timestamptz started_at
        timestamptz finished_at
    }

    ORDERS ||--o{ RETURNED_ORDERS : "has returns"
```

---

## Screenshots

Place dashboard screenshots in `docs/screenshots/` after completing Metabase setup.  
Suggested filenames:
- `01_monthly_revenue.png`
- `02_top_products.png`
- `03_revenue_by_region.png`
- `04_orders_over_time.png`
- `05_return_rate.png`
- `06_full_dashboard.png`
