#!/bin/bash
# Dijalankan otomatis oleh image postgres SEKALI saat volume data masih kosong.
# Membuat database & user metadata Airflow, lalu role/database/schema HudsonRide (sql/04_rbac.sql).
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
     -v airflow_pw="$AIRFLOW_DB_PASSWORD" <<'SQL'
SELECT format('CREATE ROLE airflow LOGIN PASSWORD %L', :'airflow_pw')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'airflow') \gexec
SELECT 'CREATE DATABASE airflow OWNER airflow'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'airflow') \gexec
SQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
     -v etl_pw="$HR_ETL_PASSWORD" -v analyst_pw="$HR_ANALYST_PASSWORD" -v dashboard_pw="$HR_DASHBOARD_PASSWORD" \
     -f /sql/04_rbac.sql

echo "HudsonRide: role dan database siap"
