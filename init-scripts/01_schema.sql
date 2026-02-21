-- ================================================================
-- Sales Data Platform — PostgreSQL Schema
-- ================================================================
-- This script runs automatically when the postgres container starts
-- for the first time (mounted in /docker-entrypoint-initdb.d/).
-- ================================================================

-- ── Create dedicated databases ──────────────────────────────────
SELECT 'CREATE DATABASE airflow'
  WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

SELECT 'CREATE DATABASE metabase'
  WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metabase')\gexec

-- ── Create application users ────────────────────────────────────
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow_user') THEN
    CREATE USER airflow_user WITH PASSWORD 'change_me_airflow';
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'metabase_user') THEN
    CREATE USER metabase_user WITH PASSWORD 'change_me_metabase';
  END IF;
END
$$;

GRANT ALL PRIVILEGES ON DATABASE airflow  TO airflow_user;
GRANT ALL PRIVILEGES ON DATABASE metabase TO metabase_user;
GRANT ALL PRIVILEGES ON DATABASE sales    TO sales_user;

-- ── Switch to sales database ────────────────────────────────────
\connect sales

-- ================================================================
-- 1. ORDERS — core sales fact table
-- ================================================================
CREATE TABLE IF NOT EXISTS orders (
    order_id        VARCHAR(36)     PRIMARY KEY,
    customer_id     VARCHAR(36)     NOT NULL,
    product         VARCHAR(120)    NOT NULL,
    category        VARCHAR(60)     NOT NULL,
    region          VARCHAR(60)     NOT NULL,
    quantity        INTEGER         NOT NULL CHECK (quantity > 0),
    unit_price      NUMERIC(10, 2)  NOT NULL CHECK (unit_price >= 0),
    discount        NUMERIC(5, 4)   NOT NULL DEFAULT 0 CHECK (discount BETWEEN 0 AND 1),
    total_revenue   NUMERIC(12, 2)  GENERATED ALWAYS AS
                        (quantity * unit_price * (1 - discount)) STORED,
    order_date      DATE            NOT NULL,
    status          VARCHAR(30)     NOT NULL DEFAULT 'completed',
    ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_order_date ON orders (order_date);
CREATE INDEX IF NOT EXISTS idx_orders_category   ON orders (category);
CREATE INDEX IF NOT EXISTS idx_orders_region     ON orders (region);
CREATE INDEX IF NOT EXISTS idx_orders_status     ON orders (status);

-- ================================================================
-- 2. RETURNED_ORDERS — tracks order returns / refunds
-- ================================================================
CREATE TABLE IF NOT EXISTS returned_orders (
    return_id       SERIAL          PRIMARY KEY,
    order_id        VARCHAR(36)     NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    return_reason   VARCHAR(255),
    return_date     DATE            NOT NULL,
    refund_amount   NUMERIC(12, 2)  NOT NULL CHECK (refund_amount >= 0),
    processed_at    TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_returns_order_id    ON returned_orders (order_id);
CREATE INDEX IF NOT EXISTS idx_returns_return_date ON returned_orders (return_date);

-- ================================================================
-- 3. PURCHASED_PRODUCTS — product-level aggregation table
--    Refreshed by the Airflow pipeline after each load.
-- ================================================================
CREATE TABLE IF NOT EXISTS purchased_products (
    product             VARCHAR(120)    PRIMARY KEY,
    category            VARCHAR(60)     NOT NULL,
    total_units_sold    BIGINT          NOT NULL DEFAULT 0,
    total_revenue       NUMERIC(14, 2)  NOT NULL DEFAULT 0,
    avg_discount        NUMERIC(5, 4)   NOT NULL DEFAULT 0,
    last_purchased_date DATE,
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- ================================================================
-- 4. PIPELINE_RUNS — ETL audit log
-- ================================================================
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          SERIAL          PRIMARY KEY,
    dag_run_id      VARCHAR(255)    NOT NULL,
    file_processed  VARCHAR(500)    NOT NULL,
    rows_inserted   INTEGER         NOT NULL DEFAULT 0,
    rows_skipped    INTEGER         NOT NULL DEFAULT 0,
    status          VARCHAR(30)     NOT NULL DEFAULT 'running',
    started_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_dag_run_id ON pipeline_runs (dag_run_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status     ON pipeline_runs (status);

-- Grant sales_user full access to all objects
GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA public TO sales_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO sales_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT ALL PRIVILEGES ON TABLES    TO sales_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT ALL PRIVILEGES ON SEQUENCES TO sales_user;
