# 🚀 E-Commerce Sales Data Platform

A production-ready, containerised data platform built with Docker Compose.  
Synthetic sales data flows end-to-end: **MinIO → Airflow → PostgreSQL → Metabase**.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│  Docker Network: platform_network                                │
│                                                                  │
│  ┌─────────────┐    CSV     ┌──────────┐                        │
│  │ Data        │ ─────────► │  MinIO   │  (Object Storage)      │
│  │ Generator   │  upload    │  :9000   │                        │
│  │ (Python)    │            │  :9001   │ ◄─── Console UI        │
│  └─────────────┘            └────┬─────┘                        │
│                                  │ S3KeySensor                  │
│                             ┌────▼──────────────┐               │
│                             │  Apache Airflow   │               │
│                             │  Webserver  :8080 │               │
│                             │  Scheduler        │               │
│                             └────┬──────────────┘               │
│                                  │ psycopg2 bulk upsert         │
│                        ┌─────────▼──────────┐                   │
│                        │    PostgreSQL       │                   │
│                        │        :5432        │                   │
│                        │  DB: sales          │                   │
│                        │  DB: airflow        │                   │
│                        │  DB: metabase       │                   │
│                        └─────────┬──────────┘                   │
│                                  │ SQL queries                  │
│                        ┌─────────▼──────────┐                   │
│                        │     Metabase        │                   │
│                        │        :3000        │                   │
│                        │  Dashboards & BI    │                   │
│                        └────────────────────┘                   │
└──────────────────────────────────────────────────────────────────┘
```

---

## Services & Ports

| Service            | URL                       | Credentials (default)       |
|--------------------|---------------------------|-----------------------------|
| Airflow UI         | http://localhost:8080     | admin / change_me_admin     |
| MinIO Console      | http://localhost:9001     | minioadmin / change_me_minio |
| Metabase           | http://localhost:3000     | First-run setup wizard      |
| PostgreSQL         | localhost:5432            | sales_user / change_me_postgres |

---

## Quick Start

### Prerequisites
- Docker Desktop ≥ 24 with Compose V2
- Git

### 1 — Clone & configure

```bash
git clone https://github.com/<your-org>/DEM12.git
cd DEM12
cp .env.example .env
# Edit .env and replace all change_me_* values with real passwords
```

### 2 — Generate a Fernet key for Airflow

```bash
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# Paste the output into AIRFLOW__CORE__FERNET_KEY in .env
```

### 3 — Start the platform

```bash
docker compose up -d
```

Wait ~90 seconds for all health checks to pass, then verify:

```bash
docker compose ps   # all services should show "healthy"
```

### 4 — Generate sample data

```bash
docker compose --profile tools run --rm data-generator
```

This creates a 500-row synthetic sales CSV and uploads it to MinIO.

### 5 — Trigger the pipeline

1. Open **Airflow UI** → http://localhost:8080
2. Enable the `sales_pipeline_dag` toggle
3. Click **▶ Trigger DAG**
4. Watch all 5 tasks turn green

### 6 — View dashboards

1. Open **Metabase** → http://localhost:3000
2. Complete the first-run wizard; connect to:
   - Host: `postgres` | Port: `5432` | DB: `sales` | User: `sales_user`
3. Create a new question → Browse the `orders` table

---

## Data Flow

```
generate_data.py
      │
      ▼  sales_YYYYMMDD_HHMMSS.csv
   MinIO (raw-data bucket)
      │
      ▼  download_from_minio
   Airflow Worker (temp file)
      │
      ├─► validate_csv      — schema & null checks
      │
      ├─► transform_data    — clean, normalise, compute total_revenue
      │
      ├─► load_to_postgres  — bulk upsert orders + purchased_products
      │                       + pipeline_runs audit row
      │
      └─► archive_file      — move CSV to processed-data bucket
```

---

## Database Schema

| Table                | Purpose                              |
|----------------------|--------------------------------------|
| `orders`             | Core sales fact table                |
| `returned_orders`    | Return / refund tracking             |
| `purchased_products` | Product-level revenue aggregations   |
| `pipeline_runs`      | ETL audit log (every DAG run)        |

---

## Environment Variables

All variables are documented in `.env.example`.  
**Never commit your `.env` file.**  
All values are validated at startup via Pydantic `BaseSettings` — missing or invalid variables produce a clear error message listing every issue.

---

## GitHub Actions CI/CD

| Workflow                    | Trigger       | What it does                                              |
|-----------------------------|---------------|-----------------------------------------------------------|
| `ci.yml`                    | Every push/PR | Compose lint → build → unit tests → DAG import check     |
| `cd.yml`                    | Merge to main | Build & push to GHCR → deploy & health-check stack       |
| `data-flow-validation.yml`  | Merge to main | Seed data → trigger DAG → assert DB rows via pytest      |

### Required GitHub Secrets (for CD)

| Secret                  | Description                            |
|-------------------------|----------------------------------------|
| `POSTGRES_PASSWORD`     | PostgreSQL password                    |
| `AIRFLOW_DB_PASSWORD`   | Airflow DB user password               |
| `AIRFLOW_FERNET_KEY`    | 32-byte base64 Fernet key              |
| `AIRFLOW_SECRET_KEY`    | Flask secret key for Airflow webserver |
| `AIRFLOW_ADMIN_PASSWORD`| Airflow admin UI password              |
| `MINIO_PASSWORD`        | MinIO root password                    |
| `METABASE_DB_PASSWORD`  | Metabase PostgreSQL user password      |

---

## Running Tests

```bash
# Unit tests only (no Docker required)
pip install pytest pandas pyarrow psycopg2-binary pydantic pydantic-settings
pytest tests/ -v --ignore=tests/test_data_flow.py

# End-to-end data flow tests (requires running stack + seed data + completed DAG)
pytest tests/test_data_flow.py -v
```

---

## Folder Structure

```
DEM12/
├── .github/workflows/          # CI/CD & data flow validation
├── data-generator/             # Synthetic data generator (Dockerised)
├── dags/                       # Airflow DAG definitions
├── include/                    # Shared Python modules (config, transforms, loader)
├── init-scripts/               # PostgreSQL schema (auto-run at first start)
├── minio-init/                 # MinIO bucket creation script
├── tests/                      # pytest test suite
├── docs/                       # Architecture diagrams & screenshots
├── docker-compose.yml
├── .env.example
├── .gitignore
└── README.md
```

---

## Metabase Dashboard Guide

After connecting Metabase to PostgreSQL, build these charts and save them to a dashboard called **"Sales Overview"**:

| Chart | Type | Query |
|---|---|---|
| Monthly Revenue | Bar | `SUM(total_revenue)` grouped by `order_date` (month) |
| Top 10 Products | Bar | `SUM(total_revenue)` by `product`, limit 10 |
| Revenue by Region | Pie/Bar | `SUM(total_revenue)` by `region` |
| Orders Over Time | Line | `COUNT(order_id)` by `order_date` (day) |
| Return Rate | KPI | `returned_orders` count / `orders` count × 100 |

> Screenshots should be saved to `docs/screenshots/` after setup.
