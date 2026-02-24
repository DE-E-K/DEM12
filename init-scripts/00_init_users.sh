#!/bin/bash
set -e

# Default passwords if not provided in env
AIRFLOW_PASS="${AIRFLOW_DB_PASSWORD:-change_me_airflow}"
METABASE_PASS="${MB_DB_PASS:-change_me_metabase}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  SELECT 'CREATE DATABASE airflow'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

  SELECT 'CREATE DATABASE metabase'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metabase')\gexec

  DO \$\$
  BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow_user') THEN
      CREATE USER airflow_user WITH PASSWORD '${AIRFLOW_PASS}';
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'metabase_user') THEN
      CREATE USER metabase_user WITH PASSWORD '${METABASE_PASS}';
    END IF;
  END
  \$\$;

  GRANT ALL PRIVILEGES ON DATABASE airflow  TO airflow_user;
  GRANT ALL PRIVILEGES ON DATABASE metabase TO metabase_user;
  GRANT ALL PRIVILEGES ON DATABASE sales    TO sales_user;
EOSQL
