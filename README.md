<div align="center">

# E-Commerce Sales Data Platform

**A production-ready, containerised data engineering platform for end-to-end sales analytics.**

[![CI](https://github.com/DE-E-K/DEM12/actions/workflows/ci.yml/badge.svg)](https://github.com/DE-E-K/DEM12/actions/workflows/ci.yml)
[![CD](https://github.com/DE-E-K/DEM12/actions/workflows/cd.yml/badge.svg)](https://github.com/DE-E-K/DEM12/actions/workflows/cd.yml)

*Synthetic e-commerce data flows through a fully automated pipeline:*
**Data Generator → MinIO → Apache Airflow → PostgreSQL → Metabase**

</div>

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [Services & Ports](#services--ports)
- [Database Schema](#database-schema)
- [Data Flow](#data-flow)
- [Getting Started](#getting-started)
- [Environment Variables](#environment-variables)
- [Running Tests](#running-tests)
- [CI/CD Pipeline](#cicd-pipeline)
- [Project Structure](#project-structure)
- [Metabase Dashboard Guide](#metabase-dashboard-guide)
- [Troubleshooting](#troubleshooting)
- [License](#license)

---

## Overview

This platform demonstrates a complete data engineering workflow for an e-commerce business. It generates realistic synthetic sales data, ingests it through an orchestrated ETL pipeline, stores it in a normalised relational database, and exposes it via a self-service BI tool all running in Docker containers with a single `docker compose up` command.

**Key capabilities:**

- **Automated data generation:** produces customers, products, and transaction CSVs with configurable volumes (default: 1,000 customers, 100 products, 10,000 transactions)
- **Object storage ingestion:** CSVs land in MinIO (S3-compatible) before processing
- **Orchestrated ETL:** Apache Airflow DAG runs every 15 minutes: download → validate → transform → load → archive
- **Normalised schema:** 7-table PostgreSQL database with foreign keys, computed columns, check constraints, and indexes
- **Self-service analytics:** Metabase connects directly to PostgreSQL for dashboards and ad-hoc queries
- **Full CI/CD:** GitHub Actions workflows for linting, testing, building, and deploying

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│  Docker Network: platform_network                                    │
│                                                                      │
│  ┌───────────────┐   3 CSVs    ┌─────────────────┐                   │
│  │ Data          │ ───────────►│  MinIO          │                   │
│  │ Generator     │   upload    │  :9000 (S3 API) │                   │
│  │ (Python 3.11) │             │  :9001 (Console)│                   │
│  └───────────────┘             └────────┬────────┘                   │
│                                          │                           │
│                              Scheduled poll (every 15 min)           │
│                                          │                           │
│                                 ┌────────▼──────────┐                │
│                                 │  Apache Airflow   │                │
│                                 │  Webserver  :8080 │                │
│                                 │  Scheduler        │                │
│                                 │  (LocalExecutor)  │                │
│                                 └────────┬──────────┘                │
│                                          │                           │
│                               psycopg2 bulk upsert                   │
│                                          │                           │
│                              ┌───────────▼───────────┐               │
│                              │    PostgreSQL 16      │               │
│                              │        :5432          │               │
│                              │                       │               │
│                              │  sales    (7 tables)  │               │
│                              │  airflow  (metadata)  │               │
│                              │  metabase (config)    │               │
│                              └───────────┬───────────┘               │
│                                          │                           │
│                                     SQL queries                      │
│                                          │                           │
│                              ┌───────────▼───────────┐               │
│                              │     Metabase          │               │
│                              │        :3000          │               │
│                              │  Dashboards & BI      │               │
│                              └───────────────────────┘               │
└──────────────────────────────────────────────────────────────────────┘
```

> A detailed Mermaid architecture diagram is available in [`docs/architecture.md`](docs/architecture.md).

---

## Technology Stack

| Component          | Technology               | Version   | Purpose                                    |
|--------------------|--------------------------|-----------|--------------------------------------------|
| Orchestration      | Apache Airflow           | 2.9.1     | DAG scheduling, task execution, monitoring  |
| Object Storage     | MinIO                    | latest    | S3-compatible storage for raw & archived CSVs |
| Database           | PostgreSQL               | 16-alpine | Normalised relational storage (7 tables)   |
| BI / Dashboards    | Metabase                 | latest    | Self-service analytics and visualisation    |
| Data Generator     | Python / Faker / Pandas  | 3.11      | Synthetic customer, product, and sales data |
| Infrastructure     | Docker Compose           | V2        | Multi-container orchestration               |
| CI/CD              | GitHub Actions           | —         | Automated testing, building, and deployment |
| Configuration      | Pydantic Settings        | 2.x       | Type-safe, validated environment variables  |

---

## Services & Ports

| Service             | Container Name               | URL / Port                  | Default Credentials          |
|---------------------|------------------------------|-----------------------------|------------------------------|
| Airflow Webserver   | `platform_airflow_webserver` | http://localhost:8080       | `admin` / *set in .env*      |
| Airflow Scheduler   | `platform_airflow_scheduler` | —                           | —                            |
| MinIO S3 API        | `platform_minio`             | http://localhost:9000       | `minioadmin` / *set in .env* |
| MinIO Console       | `platform_minio`             | http://localhost:9001       | `minioadmin` / *set in .env* |
| PostgreSQL          | `platform_postgres`          | `localhost:5432`            | `sales_user` / *set in .env* |
| Metabase            | `platform_metabase`          | http://localhost:3000       | First-run setup wizard       |

> All credentials are configured via the `.env` file. See [Environment Variables](#environment-variables).

---

## Database Schema

The `sales` database contains **7 normalised tables** with enforced foreign keys, check constraints, computed columns, and strategic indexes.

### Entity-Relationship Diagram

![ER Diagram](/docs/screenshots/dbschema.png)

### Table Summary

| Table                | Type        | Rows (typical) | Description                                                 |
|----------------------|-------------|----------------|-------------------------------------------------------------|
| `product_categories` | Dimension   | 5              | Category lookup (Electronics, Apparel, Home & Kitchen, Books, Sports) |
| `products`           | Dimension   | 100            | Product catalog with pricing; `margin` is a generated column |
| `customers`          | Dimension   | 2,000          | Customer profiles with region and lifetime value             |
| `orders`             | Fact        | 10,000         | Sales transactions; `total_revenue` is a generated column    |
| `returned_orders`    | Fact        | ~2,500         | Return/refund tracking with `ON DELETE CASCADE` to orders    |
| `purchased_products` | Aggregation | 100            | Per-product revenue summaries, refreshed each pipeline run   |
| `pipeline_runs`      | Audit       | per run        | ETL execution log with row counts and status                 |

### Key Constraints

- **Foreign keys** enforce referential integrity across all related tables
- **Check constraints** on `quantity > 0`, `unit_price >= 0`, `discount BETWEEN 0 AND 1`, `refund_amount >= 0`
- **Generated columns**: `products.margin` = `unit_price - cost`; `orders.total_revenue` = `quantity × unit_price × (1 - discount)`
- **Indexes** on all foreign keys, date columns, and frequently filtered columns (`region`, `status`)

---

## Data Flow

The Airflow DAG (`sales_pipeline_dag`) executes the following pipeline every **15 minutes**:

```
┌─────────────────────┐
│ run_data_generator  │ ← Optional: generates fresh CSVs
└──────────┬──────────┘
           ▼
┌─────────────────────┐
│ download_from_minio │ ← Downloads all pending CSVs from raw-data bucket
└──────────┬──────────┘
           ▼
┌─────────────────────┐
│ validate_csv        │ ← Schema validation, null checks per entity type
└──────────┬──────────┘
           ▼
┌─────────────────────┐
│ transform_data      │ ← Clean, normalise, extract returns & categories
└──────────┬──────────┘
           ▼
┌─────────────────────┐    FK-safe load order:
│ load_to_postgres    │ ←  categories → products → customers →
└──────────┬──────────┘    orders → returned_orders → purchased_products →
           │               update customer lifetime values → audit log
           ▼
┌─────────────────────┐
│ archive_file        │ ← Move CSVs from raw-data → processed-data bucket
└─────────────────────┘
```

### Generated Files

The data generator produces three timestamped CSV files per run:

| File Pattern                        | Records   | Description                        |
|-------------------------------------|-----------|------------------------------------|
| `customers_YYYYMMDD_HHMMSS.csv`    | 2,000     | Customer profiles with demographics |
| `products_YYYYMMDD_HHMMSS.csv`     | 100       | Product catalog across 5 categories |
| `sales_YYYYMMDD_HHMMSS.csv`        | 10,000    | Transaction records with statuses   |

---

## Getting Started

### Prerequisites

| Requirement               | Minimum Version |
|---------------------------|-----------------|
| Docker Desktop            | ≥ 24.0          |
| Docker Compose            | V2              |
| Git                       | any             |
| Python *(optional, for Fernet key generation)* | ≥ 3.8 |

### Step 1: Clone & Configure

```bash
git clone https://github.com/DE-E-K/DEM12.git
cd DEM12
cp .env.example .env
```

Open `.env` and replace **all** `change_me_*` placeholder values with secure passwords.

### Step 2: Generate a Fernet Key for Airflow

```bash
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Paste the output into `AIRFLOW__CORE__FERNET_KEY` in your `.env` file.

> **Tip:** If you don't have `cryptography` installed locally, run
> `pip install cryptography` first, or use any online Fernet key generator.

### Step 3: Start the Platform

```bash
docker compose up -d
```

Wait approximately 90 seconds for all health checks to pass, then verify:

```bash
docker compose ps
```

All services should report **healthy** status.

### Step 4: Generate Sample Data

```bash
docker compose --profile tools run --rm data-generator
```

This generates three synthetic CSV files and uploads them to the MinIO `raw-data` bucket.

### Step 5: Trigger the ETL Pipeline

**Option A: Via Airflow UI:**

1. Open http://localhost:8080
2. Log in with your configured admin credentials
3. Unpause the `sales_pipeline_dag` toggle (if paused)
4. Click **Trigger DAG** and watch all 6 tasks turn green

**Option B: Via CLI:**

```bash
docker exec platform_airflow_webserver airflow dags unpause sales_pipeline_dag
docker exec platform_airflow_webserver airflow dags trigger sales_pipeline_dag
```

### Step 6: Verify Data

```bash
docker exec platform_postgres psql -U sales_user -d sales \
  -c "SELECT 'product_categories' AS tbl, COUNT(*) FROM product_categories
      UNION ALL SELECT 'products', COUNT(*) FROM products
      UNION ALL SELECT 'customers', COUNT(*) FROM customers
      UNION ALL SELECT 'orders', COUNT(*) FROM orders
      UNION ALL SELECT 'returned_orders', COUNT(*) FROM returned_orders
      UNION ALL SELECT 'purchased_products', COUNT(*) FROM purchased_products
      UNION ALL SELECT 'pipeline_runs', COUNT(*) FROM pipeline_runs
      ORDER BY tbl;"
```

### Step 7: Explore Dashboards

1. Open **Metabase** at http://localhost:3000
2. Complete the first-run setup wizard
3. Add a database connection:
   - **Host:** `postgres`  |  **Port:** `5432`  |  **Database:** `sales`
   - **Username:** `metabase_user`  |  **Password:** *your MB_DB_PASS from .env*
4. Start building questions and dashboards

### Stopping the Platform

```bash
docker compose down          # Stop all containers (preserves data)
docker compose down -v       # Stop and remove all data volumes
```

---

## Environment Variables

All variables are defined in [`.env.example`](.env.example) and validated at startup via **Pydantic `BaseSettings`**. Missing or invalid values produce a clear error message.

| Variable                               | Default / Example                  | Description                                      |
|----------------------------------------|------------------------------------|--------------------------------------------------|
| `POSTGRES_HOST`                        | `postgres`                         | PostgreSQL hostname (container name)             |
| `POSTGRES_PORT`                        | `5432`                             | PostgreSQL port                                  |
| `POSTGRES_DB`                          | `sales`                            | Primary database name                            |
| `POSTGRES_USER`                        | `sales_user`                       | PostgreSQL superuser                             |
| `POSTGRES_PASSWORD`                    | `change_me_postgres`               | PostgreSQL password                              |
| `AIRFLOW_DB_PASSWORD`                  | `change_me_airflow`                | Airflow metadata DB user password                |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`  | `postgresql+psycopg2://...`        | Airflow DB connection string                     |
| `AIRFLOW__CORE__FERNET_KEY`            | `change_me_...`                    | 32-byte base64 Fernet encryption key             |
| `AIRFLOW__WEBSERVER__SECRET_KEY`       | `change_me_webserver_secret`       | Flask session secret key                         |
| `AIRFLOW_ADMIN_USERNAME`               | `admin`                            | Airflow web UI admin username                    |
| `AIRFLOW_ADMIN_PASSWORD`               | `change_me_admin`                  | Airflow web UI admin password                    |
| `AIRFLOW_ADMIN_EMAIL`                  | `admin@example.com`                | Airflow admin email                              |
| `MINIO_ROOT_USER`                      | `minioadmin`                       | MinIO root username                              |
| `MINIO_ROOT_PASSWORD`                  | `change_me_minio`                  | MinIO root password                              |
| `MINIO_ENDPOINT`                       | `http://minio:9000`               | MinIO S3 API endpoint                            |
| `MINIO_RAW_BUCKET`                     | `raw-data`                         | Bucket for incoming CSVs                         |
| `MINIO_PROCESSED_BUCKET`               | `processed-data`                   | Bucket for archived CSVs                         |
| `MB_DB_TYPE`                           | `postgres`                         | Metabase backend type                            |
| `MB_DB_DBNAME`                         | `metabase`                         | Metabase config database                         |
| `MB_DB_USER`                           | `metabase_user`                    | Metabase DB user                                 |
| `MB_DB_PASS`                           | `change_me_metabase`               | Metabase DB password                             |
| `GENERATOR_NUM_CUSTOMERS`              | `1000`                             | Number of customer records to generate           |
| `GENERATOR_NUM_TRANSACTIONS`           | `10000`                            | Number of transaction records to generate        |
| `GENERATOR_SEED`                       | `42`                               | Random seed for reproducible data                |

> **Security:** Never commit your `.env` file. It is included in `.gitignore` by default.

---

## Running Tests

### Unit Tests (No Docker Required)

```bash
pip install pytest pandas pyarrow psycopg2-binary pydantic pydantic-settings
pytest tests/ -v --ignore=tests/test_data_flow.py
```

### End-to-End Data Flow Tests

Requires the full stack running with seeded data and at least one completed DAG run:

```bash
pytest tests/test_data_flow.py -v
```

---

## CI/CD Pipeline

Three GitHub Actions workflows automate quality gates and deployment:

| Workflow                     | File                         | Trigger        | Steps                                                  |
|------------------------------|------------------------------|----------------|--------------------------------------------------------|
| **Continuous Integration**   | `ci.yml`                     | Push / PR      | Compose lint → Build images → Unit tests → DAG import check |
| **Continuous Deployment**    | `cd.yml`                     | Merge to `main`| Build & push to Docker Hub → Deploy → Health checks    |
| **Data Flow Validation**     | `data-flow-validation.yml`   | Merge to `main`| Seed data → Trigger DAG → Assert DB rows via pytest    |

### Required GitHub Secrets

Configure the following secrets in your repository settings (**Settings → Secrets and variables → Actions**):

| Secret                   | Description                                 |
|--------------------------|---------------------------------------------|
| `POSTGRES_PASSWORD`      | PostgreSQL superuser password               |
| `AIRFLOW_DB_PASSWORD`    | Airflow metadata DB password                |
| `AIRFLOW_FERNET_KEY`     | 32-byte base64 Fernet encryption key        |
| `AIRFLOW_SECRET_KEY`     | Airflow webserver Flask session secret       |
| `AIRFLOW_ADMIN_PASSWORD` | Airflow web UI admin password               |
| `MINIO_PASSWORD`         | MinIO root password                         |
| `METABASE_DB_PASSWORD`   | Metabase PostgreSQL user password           |
| `DOCKERHUB_USERNAME`     | Docker Hub username                         |
| `DOCKERHUB_TOKEN`        | Docker Hub access token                     |

---

## Project Structure

```
DEM12/
├── .github/
│   └── workflows/
│       ├── ci.yml                      # Continuous integration
│       ├── cd.yml                      # Continuous deployment
│       └── data-flow-validation.yml    # End-to-end data flow tests
│
├── dags/
│   └── sales_pipeline_dag.py           # Airflow DAG (6 tasks, 15-min schedule)
│
├── data-generator/
│   ├── Dockerfile                      # Python 3.11-slim image
│   ├── config.py                       # Generator settings (Pydantic)
│   ├── generate_data.py                # Produces 3 CSVs → uploads to MinIO
│   └── requirements.txt                # Python dependencies
│
├── include/
│   ├── config.py                       # Shared platform settings (Pydantic)
│   ├── transformations.py              # Data cleaning & transformation logic
│   └── db_loader.py                    # PostgreSQL bulk upsert operations
│
├── init-scripts/
│   ├── 00_init_users.sh                # Creates databases & users (airflow, metabase)
│   └── 01_schema.sql                   # 7-table schema with constraints & indexes
│
├── minio-init/
│   └── create_buckets.sh               # Creates raw-data & processed-data buckets
│
├── tests/
│   ├── __init__.py
│   └── test_data_flow.py               # End-to-end pytest suite
│
├── docs/
│   ├── architecture.md                 # Mermaid architecture diagram & screenshots
│   ├── dashboard.md                    # Metabase dashboard documentation
│   └── screenshots/                    # Platform & dashboard screenshots
│       ├── dagrunstatus.png            # Airflow DAG run status
│       ├── dbschema.png                # PostgreSQL table schema
│       ├── mbdashbord.png              # Metabase Sales Overview dashboard
│       ├── minioprocessedfiles.png     # MinIO processed-data bucket
│       ├── miniorawdata.png            # MinIO raw-data bucket
│       ├── monthlyrevenue char.png     # Monthly revenue bar chart
│       └── summaryq.png                # Metabase saved questions
│
├── docker-compose.yml                  # Full stack definition (8 services)
├── airflow-requirements.txt            # Airflow Python dependencies
├── .env.example                        # Environment variable template
├── .gitignore
└── README.md
```

---

## Metabase Dashboard Guide

After connecting Metabase to the `sales` database, build these visualisations and save them to a dashboard called **"Sales Overview"**:

| Chart                      | Type     | Source Table          | Metric / Grouping                                      |
|----------------------------|----------|-----------------------|--------------------------------------------------------|
| Monthly Revenue            | Bar      | `orders`              | `SUM(total_revenue)` grouped by `order_date` (month)   |
| Top 10 Products            | Bar      | `purchased_products`  | `total_revenue` sorted descending, limit 10            |
| Revenue by Region          | Pie      | `orders`              | `SUM(total_revenue)` grouped by `region`               |
| Orders Over Time           | Line     | `orders`              | `COUNT(order_id)` grouped by `order_date` (day)        |
| Return Rate                | KPI      | `returned_orders` / `orders` | Count of returns ÷ count of orders × 100        |
| Customer Lifetime Value    | Bar      | `customers`           | Top 20 by `lifetime_value` descending                  |
| Category Revenue           | Pie      | `purchased_products`  | `SUM(total_revenue)` grouped by `category_name`        |

All cards support **7 cross-filter dropdown parameters** (Start Date, End Date, Region, Status, Category, Product, Customer Name). Region, Status, Category, and Product use Metabase Field Filters for auto-populated dropdowns.

> For full dashboard technical documentation, see [`docs/dashboard.md`](docs/dashboard.md).

### Dashboard Preview

![Sales Overview Dashboard](docs/screenshots/mbdashbord.png)

![Monthly Revenue Chart](docs/screenshots/monthlyrevenue%20char.png)

---

## Troubleshooting

| Issue | Cause | Solution |
|-------|-------|----------|
| `permission denied for schema public` | PostgreSQL 15+ revoked default CREATE on public schema | Already handled by `00_init_users.sh` — grants are applied automatically |
| `$'\r': command not found` in init containers | Windows CRLF line endings in shell scripts | Convert `.sh` and `.sql` files to LF: `git config core.autocrlf input` |
| Airflow tasks fail with `ModuleNotFoundError` | Missing Python packages in Airflow container | Packages are installed via `_PIP_ADDITIONAL_REQUIREMENTS` in `docker-compose.yml` |
| MinIO connection refused | MinIO not healthy yet | Wait for health check to pass: `docker compose ps` |
| Password authentication failed (external client) | Incorrect password or special character escaping | Enter password in a GUI field (not URL); verify with `docker exec platform_postgres psql -U sales_user -d sales` |
| Data generator produces old single-CSV output | Stale Docker image | Rebuild: `docker compose build --no-cache data-generator` |

---

## License

This project is developed for educational and demonstration purposes as part of the Data Engineering Specialisation program.
