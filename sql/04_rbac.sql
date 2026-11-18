-- =====================================================================
-- 04_rbac.sql : role, database, schema, dan hak akses (least privilege)
-- Dijalankan SEKALI oleh hr_admin (superuser container) saat volume PostgreSQL
-- pertama kali dibuat, lewat docker/postgres/init/01_init.sh:
--   psql -v etl_pw=... -v analyst_pw=... -v dashboard_pw=... -f 04_rbac.sql
--
--   hr_admin      DBA: pemilik database, manajemen role, restore/DR drill
--   hr_etl        akun layanan pipeline: pemilik schema stg, dw, audit, mart (DDL + DML),
--                 hanya di database hudsonride_dw; tanpa superuser, CREATEDB, atau CREATEROLE
--   hr_analyst    Data Analyst / Data Steward: SELECT pada dw, audit, mart
--   hr_dashboard  aplikasi dashboard: SELECT hanya pada view schema mart
-- =====================================================================
\set ON_ERROR_STOP on

SELECT format('CREATE ROLE hr_etl LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD %L', :'etl_pw')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'hr_etl') \gexec
SELECT format('CREATE ROLE hr_analyst LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD %L', :'analyst_pw')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'hr_analyst') \gexec
SELECT format('CREATE ROLE hr_dashboard LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD %L', :'dashboard_pw')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'hr_dashboard') \gexec

SELECT 'CREATE DATABASE hudsonride_dw'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'hudsonride_dw') \gexec

\connect hudsonride_dw

-- tidak ada akses default untuk PUBLIC
REVOKE ALL ON DATABASE hudsonride_dw FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM PUBLIC;
-- CREATE: 01_schema.sql memakai CREATE SCHEMA IF NOT EXISTS; TEMPORARY: tabel sementara saat load
GRANT CONNECT, CREATE, TEMPORARY ON DATABASE hudsonride_dw TO hr_etl;
GRANT CONNECT ON DATABASE hudsonride_dw TO hr_analyst, hr_dashboard;

-- schema dimiliki akun pipeline
CREATE SCHEMA IF NOT EXISTS stg   AUTHORIZATION hr_etl;
CREATE SCHEMA IF NOT EXISTS dw    AUTHORIZATION hr_etl;
CREATE SCHEMA IF NOT EXISTS audit AUTHORIZATION hr_etl;
CREATE SCHEMA IF NOT EXISTS mart  AUTHORIZATION hr_etl;

-- analyst: baca dw, audit, mart (termasuk tabel/partisi yang dibuat pipeline di kemudian hari)
GRANT USAGE ON SCHEMA dw, audit, mart TO hr_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA dw, audit, mart TO hr_analyst;
ALTER DEFAULT PRIVILEGES FOR ROLE hr_etl IN SCHEMA dw, audit, mart GRANT SELECT ON TABLES TO hr_analyst;

-- dashboard: hanya view di schema mart; tidak punya akses ke stg, dw, audit
GRANT USAGE ON SCHEMA mart TO hr_dashboard;
GRANT SELECT ON ALL TABLES IN SCHEMA mart TO hr_dashboard;
ALTER DEFAULT PRIVILEGES FOR ROLE hr_etl IN SCHEMA mart GRANT SELECT ON TABLES TO hr_dashboard;
