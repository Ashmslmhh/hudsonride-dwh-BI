"""HudsonRide Bike Share - pipeline ETL ke data warehouse PostgreSQL.

Berkas ini DIHASILKAN dari notebook notebooks/ETL_HudsonRide_DWH.ipynb (sel 3-10 disalin apa adanya),
sehingga aturan T1-T17, load, DQ, dan backup identik dengan notebook. Jangan diedit manual:
ubah notebook lalu jalankan ulang tools/make_pipeline.py.

Pemakaian:  python -m etl run [--months 2024-12]    |    python -m etl dr-drill
"""
import os
from pathlib import Path


def _env_bool(name, default):
    return os.environ.get(name, '1' if default else '0').strip().lower() in ('1', 'true', 'yes')


def _env_months(name):
    v = os.environ.get(name, '').strip()
    return [m.strip() for m in v.split(',') if m.strip()] or None


# Lokasi data: folder data/ di repository (di container Airflow: /opt/airflow/data)
BASE_DIR      = os.environ.get('HR_BASE_DIR', str(Path(__file__).resolve().parents[1] / 'data'))
SRC_TRIP_DIR  = f'{BASE_DIR}/Data_CitiBike'
SRC_WEATHER   = f'{BASE_DIR}/Data_Weather*.xlsx'   # boleh lebih dari satu file, mis. tambahan Jan 2025
SRC_HOLIDAY   = f'{BASE_DIR}/Data_Holiday*.xlsx'
PIPELINE_DIR  = os.environ.get('HR_PIPELINE_DIR', f'{BASE_DIR}/pipeline')

# Mode run
RUN_MONTHS          = _env_months('HR_RUN_MONTHS')    # None = full load semua file trip; ['2024-12'] = incremental per bulan
EXPECTED_TRIP_FILES = 12      # kelengkapan sumber saat full load
USE_RAW_CACHE       = _env_bool('HR_USE_RAW_CACHE', True)    # pakai raw zone bila file sumber tidak berubah (ukuran + waktu modifikasi)
RUN_BACKUP          = _env_bool('HR_RUN_BACKUP', True)
RUN_DR_DRILL        = False   # di repo dijalankan terpisah: python -m etl dr-drill; uji restore backup terbaru ke database terpisah dan ukur RTO

# Database
DB_MODE = os.environ.get('HR_DB_MODE', 'external')     # 'colab' = PostgreSQL di runtime Colab; 'external' = PGHOST/PGPORT/PGUSER/PGPASSWORD dari Colab Secrets
DB_NAME = os.environ.get('HR_DB_NAME', 'hudsonride_dw')

# Aturan bisnis
LOCAL_TZ              = 'America/New_York'
NOAA_REPORT_TYPE      = 'FM-15'
HOLIDAY_COUNTRY       = 'US'
HOLIDAY_REGION        = 'US-NJ'
DURATION_MIN_MINUTES  = 1
DURATION_MAX_MINUTES  = 720
WEATHER_FILL_LIMIT_H  = 2
HEAVY_RAIN_MM_PER_H   = 2.5
FOG_VISIBILITY_KM     = 5
PEAK_HOURS            = [7, 8, 9, 16, 17, 18, 19]
BIKE_TYPES            = {'electric_bike': 1, 'classic_bike': 2}
EBIKE_NO_FREE_MINUTES = True   # sepeda listrik ditagih per menit sejak menit pertama
TARIFF = {                     # asumsi tarif HudsonRide, acuan Citi Bike 2024
    'member': {'key': 1, 'base_fee': 2.00, 'free_minutes': 45, 'per_minute': 0.20},
    'casual': {'key': 2, 'base_fee': 4.79, 'free_minutes': 30, 'per_minute': 0.30},
}

# Reliability
RETRY_ATTEMPTS   = 3
RETRY_BACKOFF_S  = 2
BACKUP_RETENTION = 7

# Profil data pada laporan UTS, dipakai sebagai pembanding (selisih dicatat sebagai WARNING)
UTS_BASELINE = {
    'trip_extracted': 1_052_451, 'T1_duplicate': 24, 'T3_too_short': 7_205, 'T3_too_long': 675,
    'T4_missing': 2_981, 'trip_valid': 1_041_566, 'stations': 531,
    'weather_extracted': 11_785, 'weather_fm15': 8_781, 'holidays': 12,
}

import glob, io, json, logging, re, secrets, shutil, subprocess, sys, time
from contextlib import contextmanager
from datetime import datetime

import numpy as np
import pandas as pd

try:
    from IPython.display import display
except ImportError:
    display = print

IN_COLAB = False
DIRS = {k: f'{PIPELINE_DIR}/{k}' for k in ('raw', 'clean', 'rejected', 'logs', 'backup', 'sql')}
BATCH_ID = None
LOG_FILE = None
SQL_PATHS = {}

logger = logging.getLogger('hudsonride_etl')
logger.setLevel(logging.INFO)
logger.propagate = False
_fmt = logging.Formatter('%(asctime)s | %(levelname)-7s | %(message)s', '%Y-%m-%d %H:%M:%S')


def n(x):
    """Format angka ribuan untuk log."""
    return f'{x:,}' if isinstance(x, (int, np.integer)) else str(x)


def init(batch_id=None, run_months=None):
    """Siapkan folder, BATCH_ID, logging, file SQL, dan koneksi database untuk satu proses."""
    global BATCH_ID, LOG_FILE, RUN_MONTHS
    for d in DIRS.values():
        os.makedirs(d, exist_ok=True)
    BATCH_ID = batch_id or datetime.now().strftime('B%Y%m%d_%H%M%S')
    if run_months is not None:
        RUN_MONTHS = run_months or None
    LOG_FILE = f"{DIRS['logs']}/etl_{BATCH_ID}.log"
    logger.handlers.clear()
    for h in (logging.FileHandler(LOG_FILE, encoding='utf-8'), logging.StreamHandler(sys.stdout)):
        h.setFormatter(_fmt)
        logger.addHandler(h)
    write_sql_files()
    setup_database()
    return BATCH_ID

def run_cmd(cmd, cwd=None, check=True):
    r = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True,
                       env={**os.environ, 'DEBIAN_FRONTEND': 'noninteractive'})
    if check and r.returncode != 0:
        detail = (r.stderr or r.stdout).strip()[-3000:]
        raise RuntimeError(f'{cmd[0]} gagal (exit {r.returncode}): {detail}')
    return r

def psql(sql=None, file=None, variables=None, cwd=None, single_tx=False, csv=False, db=None):
    cmd = ['psql', '-X', '-q', '-v', 'ON_ERROR_STOP=1', '-d', db or os.environ['PGDATABASE']]
    if single_tx:
        cmd.append('--single-transaction')
    if csv:
        cmd.append('--csv')
    for k, v in (variables or {}).items():
        cmd += ['-v', f'{k}={v}']
    cmd += ['-f', file] if file else ['-c', sql]
    return run_cmd(cmd, cwd=cwd).stdout

def query(sql, db=None):
    out = psql(sql, csv=True, db=db)
    return pd.read_csv(io.StringIO(out)) if out.strip() else pd.DataFrame()

def lit(v):
    """Literal SQL yang aman untuk teks bebas."""
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return 'NULL'
    return "'" + str(v).replace("'", "''") + "'"

def setup_database():
    if DB_MODE == 'colab':
        if not Path('/etc/postgresql').exists():
            logger.info('Instal PostgreSQL di runtime Colab')
            run_cmd(['apt-get', '-qq', 'update'])
            run_cmd(['apt-get', '-qq', '-y', 'install', 'postgresql', 'postgresql-contrib'])
        run_cmd(['service', 'postgresql', 'start'])
        if not os.environ.get('PGPASSWORD'):
            os.environ['PGPASSWORD'] = secrets.token_urlsafe(18)   # password acak per sesi
        as_postgres = ['sudo', '-u', 'postgres'] if shutil.which('sudo') else ['runuser', '-u', 'postgres', '--']
        run_cmd(as_postgres + ['psql', '-q', '-c', f"ALTER USER postgres PASSWORD {lit(os.environ['PGPASSWORD'])};"])
        os.environ.update(PGHOST='localhost', PGPORT='5432', PGUSER='postgres')
    else:
        def get_secret(k):
            if IN_COLAB:
                try:
                    from google.colab import userdata
                    return userdata.get(k)
                except Exception:
                    pass
            return os.environ.get(k)
        for k in ('PGHOST', 'PGPORT', 'PGUSER', 'PGPASSWORD'):
            v = get_secret(k)
            if not v:
                raise RuntimeError(f'{k} belum diset di Colab Secrets / environment variable')
            os.environ[k] = v
    for attempt in range(1, 16):
        if run_cmd(['pg_isready', '-q'], check=False).returncode == 0:
            break
        time.sleep(2)
    else:
        raise RuntimeError('PostgreSQL tidak merespons')
    if query(f'SELECT 1 AS ok FROM pg_database WHERE datname = {lit(DB_NAME)}', db='postgres').empty:
        psql(f'CREATE DATABASE "{DB_NAME}"', db='postgres')
        logger.info(f'Database {DB_NAME} dibuat')
    os.environ['PGDATABASE'] = DB_NAME
    version = query('SHOW server_version').iloc[0, 0]
    logger.info(f"PostgreSQL {version} | {os.environ['PGUSER']}@{os.environ['PGHOST']}:{os.environ['PGPORT']}/{DB_NAME} | mode {DB_MODE}")

def audit_run_start(run_mode, months):
    psql(f"INSERT INTO audit.etl_run (batch_id, status, run_mode, months) "
         f"VALUES ({lit(BATCH_ID)}, 'RUNNING', {lit(run_mode)}, {lit(months)})")

def audit_run_end(status, rows_extracted=None, rows_rejected=None, rows_loaded=None, message=None):
    try:
        psql(f"UPDATE audit.etl_run SET finished_at = now(), status = {lit(status)}, "
             f"rows_extracted = {lit(rows_extracted)}, rows_rejected = {lit(rows_rejected)}, "
             f"rows_loaded = {lit(rows_loaded)}, message = {lit(message)} WHERE batch_id = {lit(BATCH_ID)}")
    except Exception as e:
        logger.warning(f'audit.etl_run tidak dapat diperbarui: {e}')

def audit_step(name, status, info, secs):
    try:
        psql("INSERT INTO audit.etl_step_log (batch_id, step, status, rows_in, rows_out, rows_rejected, duration_sec, message) "
             f"VALUES ({lit(BATCH_ID)}, {lit(name)}, {lit(status)}, {lit(info['rows_in'])}, {lit(info['rows_out'])}, "
             f"{lit(info['rows_rejected'])}, {secs:.2f}, {lit(info['message'])})")
    except Exception as e:
        logger.warning(f'audit.etl_step_log tidak dapat ditulis: {e}')

@contextmanager
def step(name):
    """Mencatat durasi, jumlah baris, dan status setiap tahap ke log dan audit.etl_step_log."""
    info = {'rows_in': None, 'rows_out': None, 'rows_rejected': None, 'message': None}
    t0 = time.time()
    logger.info(f'[{name}] mulai')
    try:
        yield info
    except Exception as e:
        secs = time.time() - t0
        info['message'] = f'{type(e).__name__}: {e}'[:4000]
        logger.error(f'[{name}] GAGAL setelah {secs:.1f}s | {info["message"]}')
        audit_step(name, 'FAILED', info, secs)
        raise
    secs = time.time() - t0
    logger.info(f"[{name}] selesai {secs:.1f}s | in={n(info['rows_in'])} out={n(info['rows_out'])} "
                f"rejected={n(info['rows_rejected'])}" + (f" | {info['message']}" if info['message'] else ''))
    audit_step(name, 'SUCCESS', info, secs)

SCHEMA_SQL = r"""
-- 01_schema.sql : schema stg, dw, audit (idempotent)
SET client_min_messages = warning;
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dw;
CREATE SCHEMA IF NOT EXISTS audit;

-- ---------- AUDIT ----------
CREATE TABLE IF NOT EXISTS audit.etl_run (
    batch_id        text PRIMARY KEY,
    started_at      timestamptz NOT NULL DEFAULT now(),
    finished_at     timestamptz,
    status          text NOT NULL,
    run_mode        text,
    months          text,
    rows_extracted  bigint,
    rows_rejected   bigint,
    rows_loaded     bigint,
    message         text
);
CREATE TABLE IF NOT EXISTS audit.etl_step_log (
    log_id          bigserial PRIMARY KEY,
    batch_id        text NOT NULL REFERENCES audit.etl_run (batch_id),
    step            text NOT NULL,
    status          text NOT NULL,
    rows_in         bigint,
    rows_out        bigint,
    rows_rejected   bigint,
    duration_sec    numeric(10,2),
    message         text,
    logged_at       timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS audit.dq_result (
    dq_id           bigserial PRIMARY KEY,
    batch_id        text NOT NULL REFERENCES audit.etl_run (batch_id),
    check_name      text NOT NULL,
    severity        text NOT NULL,
    expected        text,
    actual          text,
    passed          boolean NOT NULL,
    checked_at      timestamptz NOT NULL DEFAULT now()
);

-- ---------- STAGING (dibuat ulang setiap run) ----------
DROP TABLE IF EXISTS stg.stg_trip, stg.stg_station, stg.stg_weather, stg.stg_date;
CREATE TABLE stg.stg_trip (
    ride_id text, rideable_type text, member_casual text, bike_type_key smallint, user_type_key smallint,
    started_at timestamp, ended_at timestamp,
    start_station_id text, start_station_name text, end_station_id text, end_station_name text,
    date_key int, time_key smallint, weather_key bigint,
    duration_min numeric(7,2), distance_km numeric(9,3), is_round_trip boolean,
    estimated_revenue numeric(10,2), source_file text
);
CREATE TABLE stg.stg_station (
    station_id text, station_name text, latitude numeric(9,6), longitude numeric(9,6), city text,
    first_seen timestamp, last_seen timestamp, usage_count int
);
CREATE TABLE stg.stg_weather (
    weather_key bigint, obs_datetime timestamp, date_key int, hour smallint,
    temperature_c numeric(5,2), dew_point_c numeric(5,2), wind_speed_ms numeric(5,2),
    visibility_km numeric(6,2), precip_mm numeric(6,2), obs_count smallint, is_imputed boolean,
    weather_condition text, temp_band text, rain_band text, wind_band text
);
CREATE TABLE stg.stg_date (
    date_key int, full_date date, day smallint, month smallint, month_name text, quarter smallint,
    year smallint, year_month text, day_of_week smallint, day_name text, is_weekend boolean,
    season text, is_holiday boolean, holiday_name text, holiday_type text, day_type text
);

-- ---------- DIMENSIONS ----------
CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_key        int PRIMARY KEY,
    full_date       date NOT NULL UNIQUE,
    day             smallint NOT NULL,
    month           smallint NOT NULL,
    month_name      text NOT NULL,
    quarter         smallint NOT NULL,
    year            smallint NOT NULL,
    year_month      text NOT NULL,
    day_of_week     smallint NOT NULL,
    day_name        text NOT NULL,
    is_weekend      boolean NOT NULL,
    season          text NOT NULL,
    is_holiday      boolean NOT NULL,
    holiday_name    text,
    holiday_type    text,
    day_type        text NOT NULL CHECK (day_type IN ('Weekday', 'Weekend', 'Holiday')),
    etl_batch_id    text NOT NULL
);
CREATE TABLE IF NOT EXISTS dw.dim_time (
    time_key        smallint PRIMARY KEY CHECK (time_key BETWEEN 0 AND 23),
    hour_label      text NOT NULL,
    time_band       text NOT NULL,
    is_peak_commute boolean NOT NULL
);
CREATE TABLE IF NOT EXISTS dw.dim_user_type (
    user_type_key   smallint PRIMARY KEY,
    user_type       text NOT NULL UNIQUE,
    base_fee        numeric(6,2) NOT NULL,
    free_minutes    smallint NOT NULL,
    per_minute      numeric(6,2) NOT NULL
);
CREATE TABLE IF NOT EXISTS dw.dim_bike_type (
    bike_type_key   smallint PRIMARY KEY,
    bike_type       text NOT NULL UNIQUE,
    bike_label      text NOT NULL,
    is_electric     boolean NOT NULL
);
CREATE TABLE IF NOT EXISTS dw.dim_weather (
    weather_key       bigint PRIMARY KEY,
    obs_datetime      timestamp NOT NULL UNIQUE,
    date_key          int NOT NULL,
    hour              smallint NOT NULL,
    temperature_c     numeric(5,2),
    dew_point_c       numeric(5,2),
    wind_speed_ms     numeric(5,2),
    visibility_km     numeric(6,2),
    precip_mm         numeric(6,2) NOT NULL,
    obs_count         smallint NOT NULL,
    is_imputed        boolean NOT NULL,
    weather_condition text NOT NULL,
    temp_band         text NOT NULL,
    rain_band         text NOT NULL,
    wind_band         text NOT NULL,
    etl_batch_id      text NOT NULL
);
CREATE TABLE IF NOT EXISTS dw.dim_station (
    station_key     serial PRIMARY KEY,
    station_id      text NOT NULL,
    station_name    text NOT NULL,
    latitude        numeric(9,6),
    longitude       numeric(9,6),
    city            text NOT NULL,
    valid_from      timestamp NOT NULL,
    valid_to        timestamp NOT NULL DEFAULT '9999-12-31',
    is_current      boolean NOT NULL DEFAULT true,
    etl_batch_id    text NOT NULL,
    UNIQUE (station_id, station_name)
);
CREATE INDEX IF NOT EXISTS ix_dim_station_current ON dw.dim_station (station_id) WHERE is_current;

-- Dimensi statis
INSERT INTO dw.dim_time (time_key, hour_label, time_band, is_peak_commute)
SELECT h, lpad(h::text, 2, '0') || ':00',
       CASE WHEN h < 6 THEN 'Dini hari (00-05)' WHEN h < 10 THEN 'Pagi (06-09)' WHEN h < 16 THEN 'Siang (10-15)'
            WHEN h < 20 THEN 'Sore (16-19)' ELSE 'Malam (20-23)' END,
       h = ANY (ARRAY[{{PEAK_HOURS}}])
FROM generate_series(0, 23) AS h
ON CONFLICT (time_key) DO UPDATE SET hour_label = EXCLUDED.hour_label, time_band = EXCLUDED.time_band,
    is_peak_commute = EXCLUDED.is_peak_commute;

INSERT INTO dw.dim_user_type (user_type_key, user_type, base_fee, free_minutes, per_minute) VALUES
{{USER_TYPE_ROWS}}
ON CONFLICT (user_type_key) DO UPDATE SET user_type = EXCLUDED.user_type, base_fee = EXCLUDED.base_fee,
    free_minutes = EXCLUDED.free_minutes, per_minute = EXCLUDED.per_minute;

INSERT INTO dw.dim_bike_type (bike_type_key, bike_type, bike_label, is_electric) VALUES
{{BIKE_TYPE_ROWS}}
ON CONFLICT (bike_type_key) DO UPDATE SET bike_type = EXCLUDED.bike_type, bike_label = EXCLUDED.bike_label,
    is_electric = EXCLUDED.is_electric;

-- ---------- FACTS ----------
CREATE TABLE IF NOT EXISTS dw.fact_trip (
    ride_id            text NOT NULL,
    started_at         timestamp NOT NULL,
    ended_at           timestamp NOT NULL,
    date_key           int NOT NULL REFERENCES dw.dim_date (date_key),
    time_key           smallint NOT NULL REFERENCES dw.dim_time (time_key),
    start_station_key  int NOT NULL REFERENCES dw.dim_station (station_key),
    end_station_key    int NOT NULL REFERENCES dw.dim_station (station_key),
    bike_type_key      smallint NOT NULL REFERENCES dw.dim_bike_type (bike_type_key),
    user_type_key      smallint NOT NULL REFERENCES dw.dim_user_type (user_type_key),
    weather_key        bigint NOT NULL REFERENCES dw.dim_weather (weather_key),
    duration_min       numeric(7,2) NOT NULL,
    distance_km        numeric(9,3),
    is_round_trip      boolean NOT NULL,
    estimated_revenue  numeric(10,2) NOT NULL,
    trip_count         smallint NOT NULL DEFAULT 1 CHECK (trip_count = 1),
    source_file        text NOT NULL,
    etl_batch_id       text NOT NULL,
    PRIMARY KEY (ride_id, started_at)
) PARTITION BY RANGE (started_at);
CREATE INDEX IF NOT EXISTS ix_fact_trip_started_brin ON dw.fact_trip USING brin (started_at);
CREATE INDEX IF NOT EXISTS ix_fact_trip_date          ON dw.fact_trip (date_key);
CREATE INDEX IF NOT EXISTS ix_fact_trip_start_station ON dw.fact_trip (start_station_key);
CREATE INDEX IF NOT EXISTS ix_fact_trip_end_station   ON dw.fact_trip (end_station_key);
CREATE INDEX IF NOT EXISTS ix_fact_trip_weather       ON dw.fact_trip (weather_key);

CREATE TABLE IF NOT EXISTS dw.fact_station_hourly (
    date_key       int NOT NULL REFERENCES dw.dim_date (date_key),
    time_key       smallint NOT NULL REFERENCES dw.dim_time (time_key),
    station_key    int NOT NULL REFERENCES dw.dim_station (station_key),
    weather_key    bigint NOT NULL REFERENCES dw.dim_weather (weather_key),
    departures         int NOT NULL,
    arrivals           int NOT NULL,
    net_flow           int NOT NULL,
    member_departures  int NOT NULL DEFAULT 0,
    casual_departures  int NOT NULL DEFAULT 0,
    avg_duration_min   numeric(7,2),
    revenue            numeric(12,2) NOT NULL,
    etl_batch_id       text NOT NULL,
    PRIMARY KEY (date_key, time_key, station_key),
    CHECK (member_departures + casual_departures = departures)
);
CREATE INDEX IF NOT EXISTS ix_fsh_station ON dw.fact_station_hourly (station_key);
CREATE INDEX IF NOT EXISTS ix_fsh_weather ON dw.fact_station_hourly (weather_key);

-- Migrasi database yang dibuat versi sebelumnya: tambahkan measure rancangan awal yang belum ada
ALTER TABLE dw.fact_trip ADD COLUMN IF NOT EXISTS trip_count smallint NOT NULL DEFAULT 1 CHECK (trip_count = 1);
ALTER TABLE dw.fact_station_hourly ADD COLUMN IF NOT EXISTS member_departures int NOT NULL DEFAULT 0;
ALTER TABLE dw.fact_station_hourly ADD COLUMN IF NOT EXISTS casual_departures int NOT NULL DEFAULT 0;
ALTER TABLE dw.fact_station_hourly ADD COLUMN IF NOT EXISTS avg_duration_min numeric(7,2);

CREATE TABLE IF NOT EXISTS dw.fact_route_monthly (
    month_key          int NOT NULL REFERENCES dw.dim_date (date_key),
    start_station_key  int NOT NULL REFERENCES dw.dim_station (station_key),
    end_station_key    int NOT NULL REFERENCES dw.dim_station (station_key),
    trips              int NOT NULL,
    member_trips       int NOT NULL,
    casual_trips       int NOT NULL,
    avg_duration_min   numeric(7,2) NOT NULL,
    total_revenue      numeric(12,2) NOT NULL,
    etl_batch_id       text NOT NULL,
    PRIMARY KEY (month_key, start_station_key, end_station_key)
);
CREATE INDEX IF NOT EXISTS ix_frm_end_station ON dw.fact_route_monthly (end_station_key);
"""

LOAD_SQL = r"""
-- 02_load_dw.sql : staging -> data warehouse dalam SATU transaksi (gagal = rollback semua)
-- psql -v ON_ERROR_STOP=1 --single-transaction -v batch_id=<BATCH_ID> -v run_mode=full|incremental -f 02_load_dw.sql
--   (dijalankan dari folder clean)
SET client_min_messages = warning;

-- 1. Staging: salinan 1:1 file CSV
TRUNCATE stg.stg_trip, stg.stg_station, stg.stg_weather, stg.stg_date;
\copy stg.stg_trip (ride_id, rideable_type, member_casual, bike_type_key, user_type_key, started_at, ended_at, start_station_id, start_station_name, end_station_id, end_station_name, date_key, time_key, weather_key, duration_min, distance_km, is_round_trip, estimated_revenue, source_file) FROM 'stg_trip.csv' WITH (FORMAT csv, HEADER true)
\copy stg.stg_station (station_id, station_name, latitude, longitude, city, first_seen, last_seen, usage_count) FROM 'stg_station.csv' WITH (FORMAT csv, HEADER true)
\copy stg.stg_weather (weather_key, obs_datetime, date_key, hour, temperature_c, dew_point_c, wind_speed_ms, visibility_km, precip_mm, obs_count, is_imputed, weather_condition, temp_band, rain_band, wind_band) FROM 'stg_weather.csv' WITH (FORMAT csv, HEADER true)
\copy stg.stg_date (date_key, full_date, day, month, month_name, quarter, year, year_month, day_of_week, day_name, is_weekend, season, is_holiday, holiday_name, holiday_type, day_type) FROM 'stg_date.csv' WITH (FORMAT csv, HEADER true)

-- 2. dim_date (upsert)
INSERT INTO dw.dim_date (date_key, full_date, day, month, month_name, quarter, year, year_month, day_of_week,
                         day_name, is_weekend, season, is_holiday, holiday_name, holiday_type, day_type, etl_batch_id)
SELECT date_key, full_date, day, month, month_name, quarter, year, year_month, day_of_week,
       day_name, is_weekend, season, is_holiday, holiday_name, holiday_type, day_type, :'batch_id'
FROM stg.stg_date
ON CONFLICT (date_key) DO UPDATE SET
    is_holiday = EXCLUDED.is_holiday, holiday_name = EXCLUDED.holiday_name, holiday_type = EXCLUDED.holiday_type,
    day_type = EXCLUDED.day_type, etl_batch_id = EXCLUDED.etl_batch_id;

-- 3. dim_weather (upsert)
INSERT INTO dw.dim_weather (weather_key, obs_datetime, date_key, hour, temperature_c, dew_point_c, wind_speed_ms,
                            visibility_km, precip_mm, obs_count, is_imputed, weather_condition, temp_band,
                            rain_band, wind_band, etl_batch_id)
SELECT weather_key, obs_datetime, date_key, hour, temperature_c, dew_point_c, wind_speed_ms,
       visibility_km, precip_mm, obs_count, is_imputed, weather_condition, temp_band,
       rain_band, wind_band, :'batch_id'
FROM stg.stg_weather
ON CONFLICT (weather_key) DO UPDATE SET
    temperature_c = EXCLUDED.temperature_c, dew_point_c = EXCLUDED.dew_point_c, wind_speed_ms = EXCLUDED.wind_speed_ms,
    visibility_km = EXCLUDED.visibility_km, precip_mm = EXCLUDED.precip_mm, obs_count = EXCLUDED.obs_count,
    is_imputed = EXCLUDED.is_imputed, weather_condition = EXCLUDED.weather_condition, temp_band = EXCLUDED.temp_band,
    rain_band = EXCLUDED.rain_band, wind_band = EXCLUDED.wind_band, etl_batch_id = EXCLUDED.etl_batch_id;

-- 4. dim_station: SCD Type 2 pada station_name (koordinat dan kota Type 1)
INSERT INTO dw.dim_station (station_id, station_name, latitude, longitude, city, valid_from, etl_batch_id)
SELECT station_id, station_name, latitude, longitude, city, first_seen, :'batch_id'
FROM stg.stg_station
ON CONFLICT (station_id, station_name) DO UPDATE SET
    valid_from = LEAST(dw.dim_station.valid_from, EXCLUDED.valid_from),
    latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude, city = EXCLUDED.city;

WITH v AS (
    SELECT station_key,
           LEAD(valid_from) OVER (PARTITION BY station_id ORDER BY valid_from, station_key) AS next_from
    FROM dw.dim_station
)
UPDATE dw.dim_station d
SET valid_to = COALESCE(v.next_from, '9999-12-31'), is_current = (v.next_from IS NULL)
FROM v
WHERE d.station_key = v.station_key
  AND (d.valid_to IS DISTINCT FROM COALESCE(v.next_from, '9999-12-31') OR d.is_current IS DISTINCT FROM (v.next_from IS NULL));

-- 5. fact_trip (idempotent)
--    File bulanan dapat memuat perjalanan yang dimulai di bulan sebelumnya (mis. mulai 30 Nov, tercatat di file Des).
--    Karena itu baris diganti per FILE SUMBER, bukan per bulan started_at:
--      full load   : seluruh fact_trip dikosongkan
--      incremental : hanya baris yang berasal dari file yang dimuat ulang yang dihapus
--    ride_id yang sudah ada dari file lain dilewati (aturan T1: kemunculan pertama dipertahankan).
DO $$
DECLARE m date; part text;
BEGIN
    FOR m IN SELECT DISTINCT date_trunc('month', started_at)::date FROM stg.stg_trip ORDER BY 1 LOOP
        part := 'fact_trip_' || to_char(m, 'YYYY_MM');
        EXECUTE format('CREATE TABLE IF NOT EXISTS dw.%I PARTITION OF dw.fact_trip FOR VALUES FROM (%L) TO (%L)',
                       part, m, (m + interval '1 month')::date);
    END LOOP;
END $$;

SELECT (:'run_mode' = 'full') AS is_full_load \gset
\if :is_full_load
TRUNCATE dw.fact_trip;
\else
DELETE FROM dw.fact_trip WHERE source_file IN (SELECT DISTINCT source_file FROM stg.stg_trip);
\endif

CREATE TEMP TABLE load_skip ON COMMIT DROP AS
SELECT t.ride_id FROM stg.stg_trip t
WHERE EXISTS (SELECT 1 FROM dw.fact_trip f WHERE f.ride_id = t.ride_id);

INSERT INTO dw.fact_trip (ride_id, started_at, ended_at, date_key, time_key, start_station_key, end_station_key,
                          bike_type_key, user_type_key, weather_key, duration_min, distance_km, is_round_trip,
                          estimated_revenue, source_file, etl_batch_id)
SELECT t.ride_id, t.started_at, t.ended_at, t.date_key, t.time_key, ss.station_key, es.station_key,
       t.bike_type_key, t.user_type_key, t.weather_key, t.duration_min, t.distance_km, t.is_round_trip,
       t.estimated_revenue, t.source_file, :'batch_id'
FROM stg.stg_trip t
JOIN dw.dim_station ss ON ss.station_id = t.start_station_id AND ss.station_name = t.start_station_name
JOIN dw.dim_station es ON es.station_id = t.end_station_id   AND es.station_name = t.end_station_name
WHERE NOT EXISTS (SELECT 1 FROM load_skip k WHERE k.ride_id = t.ride_id);

CREATE TEMP TABLE load_meta ON COMMIT DROP AS SELECT :'batch_id'::text AS batch_id;

-- 6. Jendela agregasi: bulan yang dimuat + 1 hari (kedatangan lintas bulan)
CREATE TEMP TABLE load_window ON COMMIT DROP AS
SELECT date_trunc('month', min(started_at)) AS lo,
       date_trunc('month', max(started_at)) + interval '1 month' + interval '1 day' AS hi
FROM stg.stg_trip;

-- 7. fact_station_hourly: keberangkatan per jam mulai, kedatangan per jam selesai
DELETE FROM dw.fact_station_hourly h
USING load_window w
WHERE h.date_key >= to_char(w.lo, 'YYYYMMDD')::int AND h.date_key < to_char(w.hi, 'YYYYMMDD')::int;

INSERT INTO dw.fact_station_hourly (date_key, time_key, station_key, weather_key, departures, arrivals, net_flow,
                                    member_departures, casual_departures, avg_duration_min, revenue, etl_batch_id)
WITH dep AS (
    SELECT f.date_key, f.time_key, f.start_station_key AS station_key,
           count(*) AS departures,
           count(*) FILTER (WHERE u.user_type = 'member') AS member_departures,
           count(*) FILTER (WHERE u.user_type = 'casual') AS casual_departures,
           round(avg(f.duration_min), 2) AS avg_duration_min,
           sum(f.estimated_revenue) AS revenue
    FROM dw.fact_trip f
    JOIN dw.dim_user_type u ON u.user_type_key = f.user_type_key
    CROSS JOIN load_window w
    WHERE f.started_at >= w.lo AND f.started_at < w.hi
    GROUP BY 1, 2, 3
), arr AS (
    SELECT to_char(f.ended_at, 'YYYYMMDD')::int AS date_key, extract(hour FROM f.ended_at)::smallint AS time_key,
           f.end_station_key AS station_key, count(*) AS arrivals
    FROM dw.fact_trip f, load_window w
    WHERE f.ended_at >= w.lo AND f.ended_at < w.hi
    GROUP BY 1, 2, 3
)
SELECT COALESCE(d.date_key, a.date_key), COALESCE(d.time_key, a.time_key), COALESCE(d.station_key, a.station_key),
       COALESCE(d.date_key, a.date_key)::bigint * 100 + COALESCE(d.time_key, a.time_key),
       COALESCE(d.departures, 0), COALESCE(a.arrivals, 0),
       COALESCE(a.arrivals, 0) - COALESCE(d.departures, 0),
       COALESCE(d.member_departures, 0), COALESCE(d.casual_departures, 0), d.avg_duration_min,
       COALESCE(d.revenue, 0), :'batch_id'
FROM dep d
FULL JOIN arr a ON a.date_key = d.date_key AND a.time_key = d.time_key AND a.station_key = d.station_key;

-- 8. fact_route_monthly
DELETE FROM dw.fact_route_monthly
WHERE month_key / 100 IN (SELECT DISTINCT to_char(started_at, 'YYYYMM')::int FROM stg.stg_trip);

INSERT INTO dw.fact_route_monthly (month_key, start_station_key, end_station_key, trips, member_trips, casual_trips,
                                   avg_duration_min, total_revenue, etl_batch_id)
SELECT to_char(date_trunc('month', f.started_at), 'YYYYMMDD')::int, f.start_station_key, f.end_station_key,
       count(*), count(*) FILTER (WHERE u.user_type = 'member'), count(*) FILTER (WHERE u.user_type = 'casual'),
       round(avg(f.duration_min), 2), sum(f.estimated_revenue), :'batch_id'
FROM dw.fact_trip f
JOIN dw.dim_user_type u ON u.user_type_key = f.user_type_key
WHERE date_trunc('month', f.started_at) IN (SELECT DISTINCT date_trunc('month', started_at) FROM stg.stg_trip)
GROUP BY 1, 2, 3;

-- 9. Rekonsiliasi wajib sebelum COMMIT: bila tidak cocok, seluruh load dibatalkan
DO $$
DECLARE n_stg bigint; rev_stg numeric; n_fact bigint; rev_fact numeric; n_count bigint;
        n_dep bigint; rev_dep numeric; n_seg bigint; n_route bigint; n_win bigint; rev_win numeric;
BEGIN
    -- baris yang dimuat batch ini = staging dikurangi ride_id yang sudah ada dari file lain
    SELECT count(*), COALESCE(sum(estimated_revenue), 0) INTO n_stg, rev_stg
      FROM stg.stg_trip t WHERE NOT EXISTS (SELECT 1 FROM load_skip k WHERE k.ride_id = t.ride_id);
    SELECT count(*), COALESCE(sum(estimated_revenue), 0), COALESCE(sum(trip_count), 0) INTO n_fact, rev_fact, n_count
      FROM dw.fact_trip WHERE etl_batch_id = (SELECT batch_id FROM load_meta);
    -- fakta agregat dibandingkan dengan seluruh fact_trip pada bulan yang diagregasi ulang
    SELECT count(*), COALESCE(sum(estimated_revenue), 0) INTO n_win, rev_win
      FROM dw.fact_trip
     WHERE date_trunc('month', started_at) IN (SELECT DISTINCT date_trunc('month', started_at) FROM stg.stg_trip);
    SELECT COALESCE(sum(departures), 0), COALESCE(sum(revenue), 0),
           COALESCE(sum(member_departures + casual_departures), 0) INTO n_dep, rev_dep, n_seg
      FROM dw.fact_station_hourly
     WHERE date_key / 100 IN (SELECT DISTINCT to_char(started_at, 'YYYYMM')::int FROM stg.stg_trip);
    SELECT COALESCE(sum(trips), 0) INTO n_route
      FROM dw.fact_route_monthly
     WHERE month_key / 100 IN (SELECT DISTINCT to_char(started_at, 'YYYYMM')::int FROM stg.stg_trip);
    IF n_fact <> n_stg THEN
        RAISE EXCEPTION 'Rekonsiliasi gagal: stg_trip % baris, fact_trip % baris (lookup dimensi tidak lengkap)', n_stg, n_fact;
    END IF;
    IF n_count <> n_fact THEN
        RAISE EXCEPTION 'Rekonsiliasi gagal: sum(trip_count) % vs % baris fact_trip', n_count, n_fact;
    END IF;
    IF rev_fact <> rev_stg THEN
        RAISE EXCEPTION 'Rekonsiliasi gagal: revenue stg % vs fact_trip %', rev_stg, rev_fact;
    END IF;
    IF n_dep <> n_win OR rev_dep <> rev_win THEN
        RAISE EXCEPTION 'Rekonsiliasi gagal: fact_station_hourly % trip / % revenue, fact_trip % / %', n_dep, rev_dep, n_win, rev_win;
    END IF;
    IF n_seg <> n_dep THEN
        RAISE EXCEPTION 'Rekonsiliasi gagal: member + casual departures % vs departures %', n_seg, n_dep;
    END IF;
    IF n_route <> n_win THEN
        RAISE EXCEPTION 'Rekonsiliasi gagal: fact_route_monthly % trip, fact_trip %', n_route, n_win;
    END IF;
END $$;
"""

DQ_SQL = r"""
-- 03_dq_checks.sql : pemeriksaan kualitas data setelah load, hasil disimpan di audit.dq_result
-- psql -v ON_ERROR_STOP=1 -v batch_id=<BATCH_ID> -v dur_min=1 -v dur_max=720 -f 03_dq_checks.sql
SET client_min_messages = warning;

INSERT INTO audit.dq_result (batch_id, check_name, severity, expected, actual, passed)
SELECT :'batch_id', 'Setiap baris stg_trip ada di fact_trip', 'CRITICAL', s.n::text, f.n::text, s.n = f.n
FROM (SELECT count(*) AS n FROM stg.stg_trip) s,
     (SELECT count(*) AS n FROM stg.stg_trip t WHERE EXISTS (SELECT 1 FROM dw.fact_trip x WHERE x.ride_id = t.ride_id)) f;

INSERT INTO audit.dq_result (batch_id, check_name, severity, expected, actual, passed)
SELECT :'batch_id', 'ride_id unik di fact_trip', 'CRITICAL', '0', (count(*) - count(DISTINCT ride_id))::text,
       count(*) = count(DISTINCT ride_id)
FROM dw.fact_trip;

INSERT INTO audit.dq_result (batch_id, check_name, severity, expected, actual, passed)
SELECT :'batch_id', 'Durasi di luar batas ' || :'dur_min' || '-' || :'dur_max' || ' menit', 'CRITICAL', '0', count(*)::text, count(*) = 0
FROM dw.fact_trip WHERE duration_min < :dur_min OR duration_min > :dur_max;

INSERT INTO audit.dq_result (batch_id, check_name, severity, expected, actual, passed)
SELECT :'batch_id', 'Revenue atau jarak negatif', 'CRITICAL', '0', count(*)::text, count(*) = 0
FROM dw.fact_trip WHERE estimated_revenue < 0 OR distance_km < 0;

INSERT INTO audit.dq_result (batch_id, check_name, severity, expected, actual, passed)
SELECT :'batch_id', 'Total departures fact_station_hourly = total fact_trip', 'CRITICAL', t.n::text, h.n::text, t.n = h.n
FROM (SELECT count(*) AS n FROM dw.fact_trip) t, (SELECT COALESCE(sum(departures), 0) AS n FROM dw.fact_station_hourly) h;

INSERT INTO audit.dq_result (batch_id, check_name, severity, expected, actual, passed)
SELECT :'batch_id', 'Total arrivals = total departures (net flow global = 0)', 'WARNING', '0',
       COALESCE(sum(net_flow), 0)::text, COALESCE(sum(net_flow), 0) = 0
FROM dw.fact_station_hourly;

INSERT INTO audit.dq_result (batch_id, check_name, severity, expected, actual, passed)
SELECT :'batch_id', 'Revenue fact_trip = fact_station_hourly', 'CRITICAL', t.v::text, h.v::text, t.v = h.v
FROM (SELECT COALESCE(sum(estimated_revenue), 0) AS v FROM dw.fact_trip) t,
     (SELECT COALESCE(sum(revenue), 0) AS v FROM dw.fact_station_hourly) h;

INSERT INTO audit.dq_result (batch_id, check_name, severity, expected, actual, passed)
SELECT :'batch_id', 'Trip dan revenue fact_route_monthly = fact_trip', 'CRITICAL',
       t.n || ' / ' || t.v, r.n || ' / ' || r.v, t.n = r.n AND t.v = r.v
FROM (SELECT count(*) AS n, COALESCE(sum(estimated_revenue), 0) AS v FROM dw.fact_trip) t,
     (SELECT COALESCE(sum(trips), 0) AS n, COALESCE(sum(total_revenue), 0) AS v FROM dw.fact_route_monthly) r;

INSERT INTO audit.dq_result (batch_id, check_name, severity, expected, actual, passed)
SELECT :'batch_id', 'dim_station: tepat 1 versi current per station_id', 'CRITICAL', '0', count(*)::text, count(*) = 0
FROM (SELECT station_id FROM dw.dim_station GROUP BY station_id HAVING count(*) FILTER (WHERE is_current) <> 1) x;

INSERT INTO audit.dq_result (batch_id, check_name, severity, expected, actual, passed)
SELECT :'batch_id', 'dim_station: valid_from < valid_to', 'CRITICAL', '0', count(*)::text, count(*) = 0
FROM dw.dim_station WHERE valid_from >= valid_to;

INSERT INTO audit.dq_result (batch_id, check_name, severity, expected, actual, passed)
SELECT :'batch_id', 'dim_date tanpa tanggal yang terlewat', 'WARNING', (max(full_date) - min(full_date) + 1)::text,
       count(*)::text, count(*) = max(full_date) - min(full_date) + 1
FROM dw.dim_date;

INSERT INTO audit.dq_result (batch_id, check_name, severity, expected, actual, passed)
SELECT :'batch_id', 'dim_weather tanpa jam yang terlewat', 'WARNING',
       (extract(epoch FROM max(obs_datetime) - min(obs_datetime)) / 3600 + 1)::bigint::text, count(*)::text,
       count(*) = (extract(epoch FROM max(obs_datetime) - min(obs_datetime)) / 3600 + 1)::bigint
FROM dw.dim_weather;

INSERT INTO audit.dq_result (batch_id, check_name, severity, expected, actual, passed)
SELECT :'batch_id', 'Jam cuaca hasil imputasi / Unknown', 'INFO', '-',
       count(*) FILTER (WHERE is_imputed) || ' imputasi / ' || count(*) FILTER (WHERE weather_condition = 'Unknown') || ' unknown dari ' || count(*),
       true
FROM dw.dim_weather;

INSERT INTO audit.dq_result (batch_id, check_name, severity, expected, actual, passed)
SELECT :'batch_id', 'Perjalanan dengan cuaca Unknown', 'INFO', '-', count(*)::text, true
FROM dw.fact_trip f JOIN dw.dim_weather w ON w.weather_key = f.weather_key
WHERE w.weather_condition = 'Unknown';
"""

def render_schema_sql():
    user_rows = ',\n'.join(
        f"({v['key']}, {lit(k)}, {v['base_fee']:.2f}, {v['free_minutes']}, {v['per_minute']:.2f})"
        for k, v in sorted(TARIFF.items(), key=lambda kv: kv[1]['key']))
    bike_rows = ',\n'.join(
        f"({key}, {lit(name)}, {lit(name.replace('_bike', '').title())}, {str(name == 'electric_bike').lower()})"
        for name, key in sorted(BIKE_TYPES.items(), key=lambda kv: kv[1]))
    return (SCHEMA_SQL.replace('{{PEAK_HOURS}}', ', '.join(map(str, PEAK_HOURS)))
                      .replace('{{USER_TYPE_ROWS}}', user_rows)
                      .replace('{{BIKE_TYPE_ROWS}}', bike_rows))


REPO_SQL_DIR = Path(__file__).resolve().parents[1] / 'sql'


def write_sql_files():
    bodies = {'01_schema.sql': render_schema_sql(), '02_load_dw.sql': LOAD_SQL, '03_dq_checks.sql': DQ_SQL}
    for name, body in bodies.items():
        SQL_PATHS[name] = f"{DIRS['sql']}/{name}"
        Path(SQL_PATHS[name]).write_text(body.lstrip(), encoding='utf-8')
    SQL_PATHS['05_datamart.sql'] = str(REPO_SQL_DIR / '05_datamart.sql')

TRIP_REQUIRED = ['ride_id', 'rideable_type', 'started_at', 'ended_at', 'start_station_name', 'start_station_id',
                 'end_station_name', 'end_station_id', 'start_lat', 'start_lng', 'end_lat', 'end_lng', 'member_casual']
TRIP_TEXT_COLS = ['ride_id', 'rideable_type', 'member_casual', 'start_station_id', 'start_station_name',
                  'end_station_id', 'end_station_name']
WEATHER_REQUIRED = ['DATE', 'REPORT_TYPE', 'TMP', 'DEW', 'WND', 'VIS']
WEATHER_OPTIONAL = ['AA1', 'AW1']
HOLIDAY_REQUIRED = ['country_code', 'year', 'date', 'english_name', 'is_global', 'holiday_type', 'applicable_regions']
MANIFEST_PATH = f"{DIRS['raw']}/_manifest.json"
MONTH_RE = re.compile(r'(20\d{2})[-_]?(0[1-9]|1[0-2])')

def _manifest():
    return json.loads(Path(MANIFEST_PATH).read_text()) if Path(MANIFEST_PATH).exists() else {}

def _read_source(path, dtype=None):
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            if path.lower().endswith('.csv'):
                return pd.read_csv(path, dtype=dtype, low_memory=False)
            return pd.read_excel(path, dtype=dtype)
        except FileNotFoundError:
            raise
        except Exception as e:
            logger.warning(f'Gagal membaca {Path(path).name} (percobaan {attempt}/{RETRY_ATTEMPTS}): {e}')
            if attempt == RETRY_ATTEMPTS:
                raise IOError(f'File sumber tidak dapat dibaca setelah {RETRY_ATTEMPTS} percobaan: {path}') from e
            time.sleep(RETRY_BACKOFF_S * attempt)

_PARQUET_WARNED = False

def _raw_write(df, stem):
    global _PARQUET_WARNED
    try:
        path = f"{DIRS['raw']}/{stem}.parquet"
        df.to_parquet(path, index=False)
    except Exception as e:
        path = f"{DIRS['raw']}/{stem}.pkl"
        df.to_pickle(path)
        if not _PARQUET_WARNED:
            logger.warning(f'Parquet tidak dapat dipakai ({type(e).__name__}: {e}); raw zone disimpan sebagai pickle')
            _PARQUET_WARNED = True
    return path

def _raw_read(path):
    return pd.read_parquet(path) if path.endswith('.parquet') else pd.read_pickle(path)

def ingest(path, dataset, dtype=None, required=(), optional=()):
    """Baca satu file sumber → validasi kolom → raw zone (dengan cache berbasis manifest)."""
    stat = os.stat(path)
    signature = f'{stat.st_size}:{int(stat.st_mtime)}'
    manifest = _manifest()
    entry = manifest.get(path)
    if USE_RAW_CACHE and entry and entry['signature'] == signature and Path(entry['raw_path']).exists():
        df = _raw_read(entry['raw_path'])
        logger.info(f'  {Path(path).name}: {len(df):,} baris (raw cache {entry["batch_id"]})')
        return df
    df = _read_source(path, dtype)
    df.columns = [str(c).strip() for c in df.columns]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f'{Path(path).name}: kolom wajib tidak ditemukan {missing}')
    for c in optional:
        if c not in df.columns:
            df[c] = np.nan
            logger.warning(f'{Path(path).name}: kolom opsional {c} tidak ada, diisi kosong')
    df['source_file'] = Path(path).name
    raw_path = _raw_write(df, f'{dataset}__{Path(path).stem}')
    manifest[path] = {'signature': signature, 'raw_path': raw_path, 'rows': len(df), 'batch_id': BATCH_ID,
                      'ingested_at': datetime.now().isoformat(timespec='seconds')}
    Path(MANIFEST_PATH).write_text(json.dumps(manifest, indent=2))
    logger.info(f'  {Path(path).name}: {len(df):,} baris -> raw zone')
    return df

def list_trip_files():
    files = sorted(f for f in glob.glob(f'{SRC_TRIP_DIR}/*')
                   if f.lower().endswith(('.xlsx', '.xls', '.csv')) and not Path(f).name.startswith(('~$', '.')))
    if not files:
        raise FileNotFoundError(f'Tidak ada file trip di {SRC_TRIP_DIR}')
    by_month = {}
    for f in files:
        m = MONTH_RE.search(Path(f).name)
        if not m:
            raise ValueError(f'Bulan tidak dapat dibaca dari nama file: {Path(f).name}')
        key = f'{m.group(1)}-{m.group(2)}'
        if key in by_month:
            raise ValueError(f'Lebih dari satu file untuk bulan {key}: {Path(by_month[key]).name}, {Path(f).name}')
        by_month[key] = f
    if RUN_MONTHS is None:
        if EXPECTED_TRIP_FILES and len(by_month) != EXPECTED_TRIP_FILES:
            raise ValueError(f'Sumber trip tidak lengkap: {len(by_month)} file ({", ".join(sorted(by_month))}), '
                             f'diharapkan {EXPECTED_TRIP_FILES}')
        return dict(sorted(by_month.items()))
    missing = [m for m in RUN_MONTHS if m not in by_month]
    if missing:
        raise FileNotFoundError(f'File trip untuk bulan {missing} tidak ditemukan di {SRC_TRIP_DIR}')
    return {m: by_month[m] for m in sorted(RUN_MONTHS)}

def single_file(pattern, label):
    files = sorted(glob.glob(pattern))
    if not files:
        raise FileNotFoundError(f'File {label} tidak ditemukan: {pattern}')
    return files

def extract_trips():
    files = list_trip_files()
    dtype = {c: str for c in TRIP_TEXT_COLS}
    frames = [ingest(f, 'trip', dtype=dtype, required=TRIP_REQUIRED) for f in files.values()]
    return pd.concat(frames, ignore_index=True), files

def extract_weather():
    files = single_file(SRC_WEATHER, 'cuaca NOAA')
    frames = [ingest(f, 'weather', dtype=str, required=WEATHER_REQUIRED, optional=WEATHER_OPTIONAL) for f in files]
    return pd.concat(frames, ignore_index=True), files

def extract_holiday():
    files = single_file(SRC_HOLIDAY, 'hari libur')
    if len(files) > 1:
        raise ValueError(f'Lebih dari satu file hari libur: {files}')
    return ingest(files[0], 'holiday', dtype=str, required=HOLIDAY_REQUIRED), files

KEY_COLS = ['ride_id', 'started_at', 'ended_at', 'start_station_id', 'end_station_id',
            'start_lat', 'start_lng', 'end_lat', 'end_lng', 'bike_type_key', 'user_type_key']

def clean_text(s):
    """Trim spasi; string kosong menjadi NaN; NaN tetap NaN (aman untuk pandas 2 dan 3)."""
    out = s.where(s.isna(), s.astype(str).str.strip())
    return out.mask(out.eq(''))

def haversine_km(lat1, lng1, lat2, lng2):
    p1, p2 = np.radians(lat1), np.radians(lat2)
    a = np.sin((p2 - p1) / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(np.radians(lng2 - lng1) / 2) ** 2
    return 2 * 6371.0 * np.arcsin(np.sqrt(a))

def transform_trips(raw):
    df = raw.copy()
    stats = {'trip_extracted': len(df)}
    rejected = []

    def reject(mask, rule, reason):
        nonlocal df
        mask = mask.fillna(False).astype(bool)
        bad = df[mask].copy()
        bad['reject_rule'], bad['reject_reason'] = rule, reason
        rejected.append(bad)
        df = df[~mask].copy()
        return int(mask.sum())

    for c in TRIP_TEXT_COLS:
        df[c] = clean_text(df[c])

    # T1 ride_id kosong dan ganda (UTS T1)
    t1_null = reject(df['ride_id'].isna(), 'T1', 'ride_id_kosong')
    stats['T1_duplicate'] = reject(df.duplicated('ride_id', keep='first'), 'T1', 'ride_id_ganda')
    logger.info(f"  T1 ride_id kosong: {t1_null:,} | ganda: {stats['T1_duplicate']:,}")

    # T2 parse timestamp dan hitung duration_min (UTS T2)
    for c in ('started_at', 'ended_at'):
        df[c] = pd.to_datetime(df[c], format='mixed', errors='coerce')
    t2 = reject(df['started_at'].isna() | df['ended_at'].isna(), 'T2', 'timestamp_tidak_valid')
    df['duration_min'] = (df['ended_at'] - df['started_at']).dt.total_seconds() / 60
    logger.info(f'  T2 timestamp tidak valid: {t2:,} | median durasi {df["duration_min"].median():.1f} menit')

    # T3 durasi tidak wajar (UTS T2)
    stats['T3_too_short'] = reject(df['duration_min'] < DURATION_MIN_MINUTES, 'T3', f'durasi_kurang_{DURATION_MIN_MINUTES}_menit')
    stats['T3_too_long'] = reject(df['duration_min'] > DURATION_MAX_MINUTES, 'T3', f'durasi_lebih_{DURATION_MAX_MINUTES}_menit')
    logger.info(f"  T3 durasi < {DURATION_MIN_MINUTES} menit: {stats['T3_too_short']:,} | > {DURATION_MAX_MINUTES} menit: {stats['T3_too_long']:,}")

    # T4 stasiun dan koordinat kosong (UTS T3)
    for c in ('start_lat', 'start_lng', 'end_lat', 'end_lng'):
        df[c] = pd.to_numeric(df[c], errors='coerce')
    req = ['start_station_id', 'end_station_id', 'start_lat', 'start_lng', 'end_lat', 'end_lng']
    stats['T4_missing'] = reject(df[req].isna().any(axis=1), 'T4', 'stasiun_atau_koordinat_kosong')
    logger.info(f"  T4 stasiun/koordinat kosong: {stats['T4_missing']:,}")

    # T5 standardisasi teks, validasi domain, user_type_key dan bike_type_key (UTS T4)
    df['rideable_type'] = df['rideable_type'].str.lower()
    df['member_casual'] = df['member_casual'].str.lower()
    df['bike_type_key'] = df['rideable_type'].map(BIKE_TYPES)
    df['user_type_key'] = df['member_casual'].map({k: v['key'] for k, v in TARIFF.items()})
    t5 = reject(df['bike_type_key'].isna() | df['user_type_key'].isna(), 'T5', 'nilai_domain_tidak_valid')
    df['bike_type_key'] = df['bike_type_key'].astype('int64')
    df['user_type_key'] = df['user_type_key'].astype('int64')
    logger.info(f'  T5 domain tidak valid: {t5:,} | jenis sepeda {df["rideable_type"].value_counts().to_dict()} '
                f'| tipe pengguna {df["member_casual"].value_counts().to_dict()}')

    # T6 date_key, time_key, weather_key (UTS T4)
    st = df['started_at']
    df['date_key'] = (st.dt.year * 10000 + st.dt.month * 100 + st.dt.day).astype('int64')
    df['time_key'] = st.dt.hour.astype('int64')
    df['weather_key'] = df['date_key'] * 100 + df['time_key']
    logger.info(f"  T6 periode {st.min()} s.d. {st.max()}")

    # T7 estimated_revenue (UTS T5)
    user = df['member_casual']
    base = user.map({k: v['base_fee'] for k, v in TARIFF.items()})
    free = user.map({k: v['free_minutes'] for k, v in TARIFF.items()}).astype(float)
    rate = user.map({k: v['per_minute'] for k, v in TARIFF.items()})
    if EBIKE_NO_FREE_MINUTES:
        free = free.where(df['rideable_type'] != 'electric_bike', 0.0)
    billed_minutes = np.ceil(df['duration_min'].round(6))
    df['estimated_revenue'] = (base + (billed_minutes - free).clip(lower=0) * rate).round(2)
    df['duration_min'] = df['duration_min'].round(2)
    logger.info(f"  T7 total estimasi pendapatan ${df['estimated_revenue'].sum():,.2f}")

    # T8 jarak dan perjalanan pulang-pergi (tambahan UAS)
    df['distance_km'] = haversine_km(df['start_lat'], df['start_lng'], df['end_lat'], df['end_lng']).round(3)
    df['is_round_trip'] = df['start_station_id'].eq(df['end_station_id'])
    logger.info(f"  T8 rata-rata jarak {df['distance_km'].mean():.2f} km | round trip {int(df['is_round_trip'].sum()):,}")

    rej = pd.concat(rejected, ignore_index=True) if rejected else pd.DataFrame()
    stats['trip_valid'] = len(df)
    return df.reset_index(drop=True), rej, stats

def build_stations(trip):
    """T9: versi stasiun per (station_id, station_name). Nama kosong diisi kode stasiun."""
    filled = 0
    for side in ('start', 'end'):
        na = trip[f'{side}_station_name'].isna()
        filled += int(na.sum())
        trip.loc[na, f'{side}_station_name'] = trip.loc[na, f'{side}_station_id']
    cols = ['station_id', 'station_name', 'lat', 'lng', 'ts']
    usage = pd.concat([
        trip[['start_station_id', 'start_station_name', 'start_lat', 'start_lng', 'started_at']].set_axis(cols, axis=1),
        trip[['end_station_id', 'end_station_name', 'end_lat', 'end_lng', 'ended_at']].set_axis(cols, axis=1),
    ], ignore_index=True)
    st = (usage.groupby(['station_id', 'station_name'], as_index=False)
               .agg(latitude=('lat', 'median'), longitude=('lng', 'median'),
                    first_seen=('ts', 'min'), last_seen=('ts', 'max'), usage_count=('ts', 'size')))
    sid = st['station_id'].astype(str)
    st['city'] = np.select([sid.str.startswith('JC').to_numpy(dtype=bool), sid.str.startswith('HB').to_numpy(dtype=bool)],
                           ['Jersey City', 'Hoboken'], default='New York City')
    st[['latitude', 'longitude']] = st[['latitude', 'longitude']].round(6)
    n_ids = st['station_id'].nunique()
    multi = int((st.groupby('station_id').size() > 1).sum())
    logger.info(f'  T9 stasiun unik: {n_ids:,} | versi: {len(st):,} | stasiun berganti nama (SCD2): {multi:,} '
                f'| nama kosong diisi kode: {filled:,} | kota {st.drop_duplicates("station_id")["city"].value_counts().to_dict()}')
    return st, {'stations': n_ids, 'station_versions': len(st)}

def _code(v):
    if v is None or (isinstance(v, float) and np.isnan(v)) or pd.isna(v):
        return None
    s = str(v).strip()
    return s if s and s.lower() != 'nan' else None

def parse_temp(v):
    """TMP/DEW: '+0056,1' atau 561 -> 5.6 °C; 9999 = tidak terukur."""
    s = _code(v)
    if s is None:
        return np.nan
    if ',' in s:                                # format asli: nilai, kode mutu
        s, has_quality_digit = s.split(',')[0], False
    else:                                       # format Excel: koma hilang, digit terakhir = kode mutu
        s, has_quality_digit = s.split('.')[0], True
    neg = s.startswith('-')
    s = s.lstrip('+-')
    if not s.isdigit():
        return np.nan
    if has_quality_digit:
        val = int(s[:-1]) if len(s) > 1 else 0  # '+0000,1' tersimpan di Excel sebagai 1
    else:
        val = int(s)
    return np.nan if val == 9999 else (-val if neg else val) / 10

def parse_wind(v):
    """WND: '999,9,C,0000,5' -> elemen ke-4 = kecepatan m/s x 10."""
    s = _code(v)
    p = s.split(',') if s else []
    if len(p) < 4 or not p[3].isdigit() or int(p[3]) == 9999:
        return np.nan
    return int(p[3]) / 10

def parse_visibility(v):
    """VIS: '016093,5,N,5' -> 16.093 km; 999999 = tidak terukur."""
    s = _code(v)
    if s is None:
        return np.nan
    head = s.split(',')[0].split('.')[0]
    if not head.isdigit() or int(head) == 999999:
        return np.nan
    return int(head) / 1000

def parse_precip(v):
    """AA1: '01,0000,9,5' atau 1000095 -> curah hujan mm; kosong = 0."""
    s = _code(v)
    if s is None:
        return 0.0
    if ',' in s:
        p = s.split(',')
        depth = p[1] if len(p) > 1 else ''
    else:
        depth = s.split('.')[0].zfill(8)[2:6]
    if not depth.isdigit() or int(depth) == 9999:
        return 0.0
    return int(depth) / 10

def parse_aw(v):
    """AW1: '61,5' atau 615 -> kode cuaca 61."""
    s = _code(v)
    if s is None:
        return np.nan
    head = s.split(',')[0] if ',' in s else s.split('.')[0][:-1]
    return int(head) if head.isdigit() else np.nan

def band(series, bins, labels):
    b = pd.cut(series, bins=bins, labels=labels)
    return b.cat.add_categories('Unknown').fillna('Unknown').astype(str)

def transform_weather(raw, cal_start, cal_end):
    stats = {'weather_extracted': len(raw)}

    # T10 laporan rutin (UTS T7)
    w = raw[raw['REPORT_TYPE'].astype(str).str.strip() == NOAA_REPORT_TYPE].copy()
    stats['weather_fm15'] = len(w)
    logger.info(f'  T10 laporan {NOAA_REPORT_TYPE}: {len(w):,} dari {len(raw):,} | jenis lain {raw["REPORT_TYPE"].astype(str).str.strip().value_counts().to_dict()}')

    # T11 zona waktu (UTS T7)
    ts = pd.to_datetime(w['DATE'], format='mixed', errors='coerce')
    bad_ts = int(ts.isna().sum())
    w, ts = w[ts.notna()].copy(), ts[ts.notna()]
    if ts.dt.tz is None:
        ts = ts.dt.tz_localize('UTC')
    w['obs_datetime'] = ts.dt.tz_convert(LOCAL_TZ).dt.tz_localize(None).dt.floor('h')
    logger.info(f'  T11 UTC -> {LOCAL_TZ}, timestamp tidak valid: {bad_ts:,}')

    # T12 parsing kode (UTS T7)
    w['temperature_c'] = w['TMP'].map(parse_temp)
    w['dew_point_c'] = w['DEW'].map(parse_temp)
    w['wind_speed_ms'] = w['WND'].map(parse_wind)
    w['visibility_km'] = w['VIS'].map(parse_visibility)
    w['precip_mm'] = w['AA1'].map(parse_precip)
    w['aw_code'] = w['AW1'].map(parse_aw)
    filled = {c: f"{w[c].notna().mean():.1%}" for c in ['temperature_c', 'dew_point_c', 'wind_speed_ms', 'visibility_km', 'aw_code']}
    logger.info(f'  T12 kelengkapan hasil parsing: {filled}')

    # T13 satu baris per jam + lengkapi jam kosong (UTS T7)
    hourly = (w.groupby('obs_datetime')
                .agg(temperature_c=('temperature_c', 'mean'), dew_point_c=('dew_point_c', 'mean'),
                     wind_speed_ms=('wind_speed_ms', 'mean'), visibility_km=('visibility_km', 'mean'),
                     precip_mm=('precip_mm', 'sum'), aw_code=('aw_code', 'max'), obs_count=('obs_datetime', 'size')))
    hours = pd.date_range(cal_start, cal_end + pd.Timedelta(hours=23), freq='h')
    hourly = hourly.reindex(hours)
    hourly.index.name = 'obs_datetime'
    hourly['obs_count'] = hourly['obs_count'].fillna(0).astype('int64')
    num = ['temperature_c', 'dew_point_c', 'wind_speed_ms', 'visibility_km']
    hourly[num] = hourly[num].ffill(limit=WEATHER_FILL_LIMIT_H).bfill(limit=WEATHER_FILL_LIMIT_H)
    hourly['precip_mm'] = hourly['precip_mm'].fillna(0.0)
    hourly['is_imputed'] = hourly['obs_count'].eq(0)
    no_obs = int(hourly['is_imputed'].sum())
    still_missing = int(hourly['temperature_c'].isna().sum())
    logger.info(f'  T13 jam dalam periode: {len(hours):,} | tanpa laporan: {no_obs:,} | tetap kosong setelah isi ≤{WEATHER_FILL_LIMIT_H} jam: {still_missing:,}')

    # T14 kondisi cuaca, urutan prioritas (UTS T7)
    aw, rain, vis = hourly['aw_code'], hourly['precip_mm'], hourly['visibility_km']
    hourly['weather_condition'] = np.select(
        [hourly['temperature_c'].isna().to_numpy(), aw.between(70, 79).to_numpy(), (rain >= HEAVY_RAIN_MM_PER_H).to_numpy(),
         ((rain > 0) | aw.between(50, 69)).to_numpy(), (vis < FOG_VISIBILITY_KM).to_numpy()],
        ['Unknown', 'Snow', 'Heavy Rain', 'Light Rain', 'Fog/Mist'], default='Clear/Cloudy')
    logger.info(f"  T14 kondisi cuaca: {hourly['weather_condition'].value_counts().to_dict()}")

    # T15 kelompok suhu, hujan, angin (UTS T7)
    hourly['temp_band'] = band(hourly['temperature_c'], [-60, 0, 10, 20, 28, 60], ['<=0 C', '0-10 C', '10-20 C', '20-28 C', '>28 C'])
    hourly['rain_band'] = np.select([rain.eq(0).to_numpy(), (rain < HEAVY_RAIN_MM_PER_H).to_numpy()],
                                    ['Tidak hujan', 'Hujan ringan'], default='Hujan lebat')
    hourly['wind_band'] = band(hourly['wind_speed_ms'], [-1, 3, 6, 100], ['Tenang <3 m/s', 'Sedang 3-6 m/s', 'Kencang >6 m/s'])

    dim = hourly.reset_index()
    t = dim['obs_datetime']
    dim['date_key'] = (t.dt.year * 10000 + t.dt.month * 100 + t.dt.day).astype('int64')
    dim['hour'] = t.dt.hour.astype('int64')
    dim['weather_key'] = dim['date_key'] * 100 + dim['hour']
    dim[num + ['precip_mm']] = dim[num + ['precip_mm']].round(2)
    stats['weather_hours'] = len(dim)
    stats['weather_unknown_hours'] = still_missing
    return dim, stats

SEASON = {12: 'Winter', 1: 'Winter', 2: 'Winter', 3: 'Spring', 4: 'Spring', 5: 'Spring',
          6: 'Summer', 7: 'Summer', 8: 'Summer', 9: 'Fall', 10: 'Fall', 11: 'Fall'}

def transform_calendar(holiday_raw, cal_start, cal_end, trip_years):
    h = holiday_raw.copy()
    h['date_val'] = pd.to_datetime(h['date'], format='mixed', errors='coerce').dt.normalize()
    h['year_num'] = pd.to_numeric(h['year'], errors='coerce')
    country = h['country_code'].astype(str).str.strip().str.upper()
    region = h['applicable_regions'].astype(str).str.strip()
    years = set(range(cal_start.year, cal_end.year + 1))

    # T16 libur nasional AS + New Jersey (UTS T8)
    mask = (country.eq(HOLIDAY_COUNTRY) & h['year_num'].isin(years) & h['date_val'].notna()
            & (region.eq('All') | region.str.contains(HOLIDAY_REGION, regex=False)))
    hol = h[mask].copy()
    hol['_global'] = hol['is_global'].astype(str).str.strip().str.lower().isin(['true', '1', 'yes'])
    hol = hol.sort_values('_global', ascending=False).drop_duplicates('date_val')
    hol = hol[(hol['date_val'] >= cal_start) & (hol['date_val'] <= cal_end)].sort_values('date_val')
    in_trip_years = int(hol['date_val'].dt.year.isin(trip_years).sum())
    logger.info(f'  T16 hari libur {HOLIDAY_COUNTRY}/{HOLIDAY_REGION} dalam periode: {len(hol)} '
                f'(tahun data {sorted(trip_years)}: {in_trip_years}) dari {len(h):,} baris sumber')
    for r in hol.itertuples():
        logger.info(f'      {r.date_val.date()}  {r.english_name}  [{r.holiday_type}]')

    # T17 dim_date (UTS T8)
    days = pd.date_range(cal_start, cal_end, freq='D')
    d = pd.DataFrame({
        'date_key': (days.year * 10000 + days.month * 100 + days.day).astype('int64'),
        'full_date': days.date,
        'day': days.day, 'month': days.month, 'month_name': days.month_name(), 'quarter': days.quarter,
        'year': days.year, 'year_month': days.strftime('%Y-%m'), 'day_of_week': days.dayofweek + 1,
        'day_name': days.day_name(), 'is_weekend': days.dayofweek >= 5, 'season': days.month.map(SEASON),
    })
    info = hol.set_index(hol['date_val'].dt.date)
    d['is_holiday'] = d['full_date'].isin(info.index)
    d['holiday_name'] = d['full_date'].map(info['english_name'])
    d['holiday_type'] = d['full_date'].map(info['holiday_type'])
    d['day_type'] = np.select([d['is_holiday'].to_numpy(), d['is_weekend'].to_numpy()], ['Holiday', 'Weekend'], default='Weekday')
    logger.info(f"  T17 tanggal: {len(d)} ({d['full_date'].min()} s.d. {d['full_date'].max()}) | {d['day_type'].value_counts().to_dict()}")
    return d, {'holidays': in_trip_years, 'dates': len(d)}

STG_COLS = {
    'trip': ['ride_id', 'rideable_type', 'member_casual', 'bike_type_key', 'user_type_key', 'started_at', 'ended_at',
             'start_station_id', 'start_station_name', 'end_station_id', 'end_station_name', 'date_key', 'time_key',
             'weather_key', 'duration_min', 'distance_km', 'is_round_trip', 'estimated_revenue', 'source_file'],
    'station': ['station_id', 'station_name', 'latitude', 'longitude', 'city', 'first_seen', 'last_seen', 'usage_count'],
    'weather': ['weather_key', 'obs_datetime', 'date_key', 'hour', 'temperature_c', 'dew_point_c', 'wind_speed_ms',
                'visibility_km', 'precip_mm', 'obs_count', 'is_imputed', 'weather_condition', 'temp_band',
                'rain_band', 'wind_band'],
    'date': ['date_key', 'full_date', 'day', 'month', 'month_name', 'quarter', 'year', 'year_month', 'day_of_week',
             'day_name', 'is_weekend', 'season', 'is_holiday', 'holiday_name', 'holiday_type', 'day_type'],
}

def validate_before_load(trip, stations, weather, dates, n_extracted, n_rejected):
    end = trip['ended_at']
    end_date_key = end.dt.year * 10000 + end.dt.month * 100 + end.dt.day
    end_weather_key = end_date_key * 100 + end.dt.hour
    pairs = pd.MultiIndex.from_frame(stations[['station_id', 'station_name']])
    checks = {
        'rekonsiliasi extract = valid + rejected': n_extracted == len(trip) + n_rejected,
        'ride_id unik': trip['ride_id'].is_unique,
        'kolom kunci tidak kosong': bool(trip[KEY_COLS].notna().all().all()),
        f'durasi {DURATION_MIN_MINUTES}-{DURATION_MAX_MINUTES} menit': bool(trip['duration_min'].between(DURATION_MIN_MINUTES, DURATION_MAX_MINUTES).all()),
        'jarak dan revenue tidak negatif': bool((trip['distance_km'] >= 0).all() and (trip['estimated_revenue'] >= 0).all()),
        'date_key mulai & selesai ada di dim_date': bool(trip['date_key'].isin(dates['date_key']).all() and end_date_key.isin(dates['date_key']).all()),
        'weather_key mulai & selesai ada di dim_weather': bool(trip['weather_key'].isin(weather['weather_key']).all() and end_weather_key.isin(weather['weather_key']).all()),
        'stasiun asal ada di dim_station': bool(pd.MultiIndex.from_frame(trip[['start_station_id', 'start_station_name']]).isin(pairs).all()),
        'stasiun tujuan ada di dim_station': bool(pd.MultiIndex.from_frame(trip[['end_station_id', 'end_station_name']]).isin(pairs).all()),
        'key dimensi unik (date, weather, station version)': dates['date_key'].is_unique and weather['weather_key'].is_unique and not stations.duplicated(['station_id', 'station_name']).any(),
    }
    for name, ok in checks.items():
        (logger.info if ok else logger.error)(f"  {'OK   ' if ok else 'GAGAL'} {name}")
    failed = [k for k, ok in checks.items() if not ok]
    if failed:
        raise ValueError(f'Validasi pra-load gagal: {failed}')
    return len(checks)

def compare_baseline(stats):
    if RUN_MONTHS is not None:
        return
    diffs = 0
    for k, expected in UTS_BASELINE.items():
        actual = stats.get(k)
        if actual is None:
            continue
        if actual == expected:
            logger.info(f'  baseline UTS {k}: {actual:,} (sama)')
        else:
            diffs += 1
            logger.warning(f'  baseline UTS {k}: {actual:,} vs UTS {expected:,} (selisih {actual - expected:+,})')
    logger.info(f'  perbandingan baseline UTS: {len(UTS_BASELINE) - diffs} sama, {diffs} berbeda')

def write_staging(trip, stations, weather, dates, rejected):
    fmt = '%Y-%m-%d %H:%M:%S.%f'
    out = {
        'trip': (trip, f"{DIRS['clean']}/stg_trip.csv"),
        'station': (stations, f"{DIRS['clean']}/stg_station.csv"),
        'weather': (weather, f"{DIRS['clean']}/stg_weather.csv"),
        'date': (dates, f"{DIRS['clean']}/stg_date.csv"),
    }
    for key, (df, path) in out.items():
        df.to_csv(path, columns=STG_COLS[key], index=False, date_format=fmt)
        logger.info(f'  {Path(path).name}: {len(df):,} baris')
    if len(rejected):
        rpath = f"{DIRS['rejected']}/rejected_trip_{BATCH_ID}.csv"
        cols = [c for c in TRIP_REQUIRED + ['source_file', 'duration_min', 'reject_rule', 'reject_reason'] if c in rejected.columns]
        rejected.to_csv(rpath, columns=cols, index=False, date_format=fmt)
        summary = rejected.groupby(['reject_rule', 'reject_reason']).size().to_dict()
        logger.info(f'  {Path(rpath).name}: {len(rejected):,} baris | {summary}')
    return sum(len(df) for df, _ in out.values())

def load_dw():
    run_mode = 'full' if RUN_MONTHS is None else 'incremental'
    psql(file=SQL_PATHS['02_load_dw.sql'], variables={'batch_id': BATCH_ID, 'run_mode': run_mode},
         cwd=DIRS['clean'], single_tx=True)

def dw_counts():
    sql = ' UNION ALL '.join(
        f"SELECT '{t}' AS tabel, count(*) AS baris FROM {t}" for t in
        ['stg.stg_trip', 'stg.stg_station', 'stg.stg_weather', 'stg.stg_date',
         'dw.dim_date', 'dw.dim_time', 'dw.dim_station', 'dw.dim_user_type', 'dw.dim_bike_type', 'dw.dim_weather',
         'dw.fact_trip', 'dw.fact_station_hourly', 'dw.fact_route_monthly'])
    return query(sql)

def run_dq():
    psql(file=SQL_PATHS['03_dq_checks.sql'],
         variables={'batch_id': BATCH_ID, 'dur_min': DURATION_MIN_MINUTES, 'dur_max': DURATION_MAX_MINUTES})
    dq = query(f'SELECT check_name, severity, expected, actual, passed FROM audit.dq_result '
               f'WHERE batch_id = {lit(BATCH_ID)} ORDER BY dq_id')
    dq['passed'] = dq['passed'].astype(str).str.lower().isin(['t', 'true'])
    for r in dq.itertuples():
        level = logger.info if r.passed else (logger.error if r.severity == 'CRITICAL' else logger.warning)
        level(f"  {'LOLOS' if r.passed else 'GAGAL'} [{r.severity}] {r.check_name} | expected {r.expected} | actual {r.actual}")
    return dq

def backup_dw():
    path = f"{DIRS['backup']}/{DB_NAME}_{BATCH_ID}.dump"
    t0 = time.time()
    run_cmd(['pg_dump', '-Fc', '-d', DB_NAME, '-f', path])
    secs = time.time() - t0
    old = sorted(glob.glob(f"{DIRS['backup']}/{DB_NAME}_B*.dump"))[:-BACKUP_RETENTION]
    for f in old:
        os.remove(f)
    size_mb = os.path.getsize(path) / 1024 ** 2
    return path, size_mb, secs, len(old)

def dr_drill(backup_path=None):
    """Restore backup ke database terpisah, bandingkan jumlah baris, ukur waktu pulih (RTO)."""
    backups = sorted(glob.glob(f"{DIRS['backup']}/{DB_NAME}_B*.dump"))
    backup_path = backup_path or (backups[-1] if backups else None)
    if not backup_path:
        raise FileNotFoundError('Belum ada file backup')
    test_db = f'{DB_NAME}_restore_test'
    with step('R1 DR drill: restore backup') as s:
        psql(f'DROP DATABASE IF EXISTS "{test_db}"', db='postgres')
        psql(f'CREATE DATABASE "{test_db}"', db='postgres')
        t0 = time.time()
        run_cmd(['pg_restore', '--no-owner', '-d', test_db, backup_path])
        rto = time.time() - t0
        tables = ['dw.dim_station', 'dw.dim_date', 'dw.dim_weather', 'dw.fact_trip', 'dw.fact_station_hourly', 'dw.fact_route_monthly']
        sql = ' UNION ALL '.join(f"SELECT '{t}' AS tabel, count(*) AS baris FROM {t}" for t in tables)
        cmp = query(sql).merge(query(sql, db=test_db), on='tabel', suffixes=('_produksi', '_restore'))
        cmp['sama'] = cmp['baris_produksi'] == cmp['baris_restore']
        display(cmp)
        psql(f'DROP DATABASE "{test_db}"', db='postgres')
        if not cmp['sama'].all():
            raise RuntimeError('Jumlah baris hasil restore tidak sama dengan database produksi')
        s['rows_out'] = int(cmp.loc[cmp['tabel'] == 'dw.fact_trip', 'baris_restore'].iloc[0])
        s['message'] = f'{Path(backup_path).name} dipulihkan dalam {rto:.1f} detik (RTO terukur)'
    return rto


# =====================================================================================
# Tahap pipeline. Notebook menjalankan semuanya dalam satu fungsi (run_pipeline);
# di repo setiap tahap menjadi task Airflow terpisah dengan BATCH_ID yang sama.
# Ringkasan antartahap disimpan di pipeline/logs/state_<BATCH_ID>.json.
# =====================================================================================

def audit_run_start(run_mode, months):
    # idempotent: aman bila task Airflow di-retry
    psql(f"INSERT INTO audit.etl_run (batch_id, status, run_mode, months) "
         f"VALUES ({lit(BATCH_ID)}, 'RUNNING', {lit(run_mode)}, {lit(months)}) "
         f"ON CONFLICT (batch_id) DO UPDATE SET status = 'RUNNING', finished_at = NULL, message = NULL")


def _state_path():
    return f"{DIRS['logs']}/state_{BATCH_ID}.json"


def state(**updates):
    data = {}
    if os.path.exists(_state_path()):
        data = json.loads(Path(_state_path()).read_text(encoding='utf-8'))
    if updates:
        data.update(updates)
        Path(_state_path()).write_text(json.dumps(data, indent=1), encoding='utf-8')
    return data


def prepare():
    run_mode = 'full' if RUN_MONTHS is None else 'incremental'
    psql(file=SQL_PATHS['01_schema.sql'])
    audit_run_start(run_mode, 'all' if RUN_MONTHS is None else ','.join(RUN_MONTHS))
    state(run_mode=run_mode, run_months=RUN_MONTHS, extracted=None, rejected=None, loaded=None, dq_all_passed=None)
    logger.info(f'RUN {BATCH_ID} dimulai | mode {run_mode} | bulan {RUN_MONTHS or "semua"}')


def extract():
    with step('E1 extract trip') as s:
        raw_trip, trip_files = extract_trips()
        s['rows_out'] = len(raw_trip)
        s['message'] = f"{len(trip_files)} file ({', '.join(trip_files)})"
    with step('E2 extract cuaca NOAA') as s:
        raw_weather, weather_files = extract_weather()
        s['rows_out'] = len(raw_weather)
        s['message'] = ', '.join(Path(f).name for f in weather_files)
    with step('E3 extract hari libur') as s:
        raw_holiday, holiday_files = extract_holiday()
        s['rows_out'] = len(raw_holiday)
        s['message'] = Path(holiday_files[0]).name
    state(extracted=len(raw_trip))


def _read_raw_zone():
    """Task transform berjalan di proses terpisah: data mentah dibaca ulang dari raw zone (cache extract)."""
    level = logger.level
    logger.setLevel(logging.WARNING)
    try:
        return extract_trips()[0], extract_weather()[0], extract_holiday()[0]
    finally:
        logger.setLevel(level)


def transform():
    raw_trip, raw_weather, raw_holiday = _read_raw_zone()
    stats = {}
    with step('T1-T8 transform trip') as s:
        trip, rejected, st = transform_trips(raw_trip)
        stats.update(st)
        s['rows_in'], s['rows_out'], s['rows_rejected'] = len(raw_trip), len(trip), len(rejected)
        if len(rejected):
            s['message'] = ', '.join(f'{r}:{c:,}' for r, c in rejected['reject_reason'].value_counts().items())
    with step('T9 transform stasiun (SCD2)') as s:
        stations, st = build_stations(trip)
        stats.update(st)
        s['rows_out'] = len(stations)
        s['message'] = f"{st['stations']} stasiun, {st['station_versions']} versi"

    cal_start = trip['started_at'].min().normalize()
    cal_end = trip['ended_at'].max().normalize()
    trip_years = sorted(trip['started_at'].dt.year.unique().tolist())

    with step('T10-T15 transform cuaca NOAA') as s:
        weather, st = transform_weather(raw_weather, cal_start, cal_end)
        stats.update(st)
        s['rows_in'], s['rows_out'] = len(raw_weather), len(weather)
        s['message'] = f"{st['weather_unknown_hours']} jam Unknown"
    with step('T16-T17 transform hari libur dan kalender') as s:
        dates, st = transform_calendar(raw_holiday, cal_start, cal_end, trip_years)
        stats.update(st)
        s['rows_in'], s['rows_out'] = len(raw_holiday), len(dates)
        s['message'] = f"{st['holidays']} hari libur pada tahun data"
    with step('V1 validasi pra-load') as s:
        n_checks = validate_before_load(trip, stations, weather, dates, len(raw_trip), len(rejected))
        compare_baseline(stats)
        s['message'] = f'{n_checks} pemeriksaan lolos'
    with step('S1 tulis staging CSV') as s:
        s['rows_out'] = write_staging(trip, stations, weather, dates, rejected)
        s['message'] = DIRS['clean']
    state(rejected=len(rejected), staged=len(trip))


def load():
    with step('L1 load data warehouse') as s:
        load_dw()
        loaded = int(query(f'SELECT count(*) AS n FROM dw.fact_trip WHERE etl_batch_id = {lit(BATCH_ID)}').iloc[0, 0])
        s['rows_in'], s['rows_out'] = state().get('staged'), loaded
        counts = dw_counts()
        s['message'] = ' | '.join(f'{r.tabel}={r.baris:,}' for r in counts.itertuples() if r.tabel.startswith('dw.fact'))
    state(loaded=loaded)


def dq():
    with step('Q1 data quality checks') as s:
        res = run_dq()
        s['rows_out'] = len(res)
        s['message'] = f"{int(res['passed'].sum())}/{len(res)} lolos"
        critical = res[(~res['passed']) & (res['severity'] == 'CRITICAL')]
        if len(critical):
            raise ValueError(f"DQ CRITICAL gagal: {critical['check_name'].tolist()}")
    state(dq_all_passed=bool(res['passed'].all()))


def mart():
    with step('M1 data mart (view per stakeholder)') as s:
        psql(file=SQL_PATHS['05_datamart.sql'])
        objs = query("SELECT count(*) FILTER (WHERE c.relkind = 'v') AS v, count(*) FILTER (WHERE c.relkind = 'm') AS m "
                     "FROM pg_class c JOIN pg_namespace s ON s.oid = c.relnamespace WHERE s.nspname = 'mart'").iloc[0]
        s['rows_out'] = int(objs['v'] + objs['m'])
        s['message'] = f"{int(objs['m'])} materialized view + {int(objs['v'])} view di schema mart"


def backup():
    if not RUN_BACKUP:
        logger.info('Backup dilewati (HR_RUN_BACKUP = 0)')
        return
    with step('B1 backup database') as s:
        path, size_mb, secs, removed = backup_dw()
        s['message'] = f'{Path(path).name} ({size_mb:.1f} MB, {secs:.1f} s, {removed} backup lama dihapus)'


def finish():
    st = state()
    status = 'SUCCESS' if st.get('dq_all_passed') else 'SUCCESS_WITH_WARNINGS'
    audit_run_end(status, st.get('extracted'), st.get('rejected'), st.get('loaded'))
    logger.info(f"RUN {status} | extracted {n(st.get('extracted'))} | rejected {n(st.get('rejected'))} | "
                f"loaded {n(st.get('loaded'))}")
    return status


def fail(message):
    st = state()
    try:   # pastikan run tercatat walaupun gagal sebelum tahap prepare selesai
        run_mode = 'full' if RUN_MONTHS is None else 'incremental'
        psql(f"INSERT INTO audit.etl_run (batch_id, status, run_mode, months) VALUES ({lit(BATCH_ID)}, 'FAILED', "
             f"{lit(run_mode)}, {lit('all' if RUN_MONTHS is None else ','.join(RUN_MONTHS))}) ON CONFLICT (batch_id) DO NOTHING")
    except Exception as e:
        logger.warning(f'audit.etl_run tidak dapat ditulis: {e}')
    audit_run_end('FAILED', st.get('extracted'), st.get('rejected'), st.get('loaded'), str(message)[:4000])
    logger.error(f'RUN FAILED | {message} | log: {LOG_FILE}')


def _ensure_schema():
    t = query("SELECT coalesce(to_regclass('audit.etl_run')::text, '') AS t").iloc[0, 0]
    if not isinstance(t, str) or not t:
        psql(file=SQL_PATHS['01_schema.sql'])


def refresh_source(source):
    """Jadwal terpisah per sumber (rancangan awal): cuaca harian, hari libur tahunan.
    Memeriksa file sumber dan memperbarui raw zone; DAG bulanan memakai raw zone terbaru ini."""
    steps = {'weather': ('E2 extract cuaca NOAA', extract_weather),
             'holiday': ('E3 extract hari libur', extract_holiday)}
    name, fn = steps[source]
    _ensure_schema()
    audit_run_start(f'refresh_{source}', 'all')
    try:
        with step(name) as s:
            raw, files = fn()
            s['rows_out'] = len(raw)
            s['message'] = ', '.join(Path(f).name for f in files)
        audit_run_end('SUCCESS', len(raw), None, None, f'raw zone {source} diperbarui')
        logger.info(f'REFRESH {source} SUCCESS | {len(raw):,} baris')
        return len(raw)
    except Exception as e:
        audit_run_end('FAILED', None, None, None, f'{type(e).__name__}: {e}'[:4000])
        logger.error(f'REFRESH {source} FAILED | {type(e).__name__}: {e}')
        raise


def run_all(run_months=None, batch_id=None):
    """Seluruh tahap dalam satu proses (setara run_pipeline() di notebook)."""
    init(batch_id, run_months)
    prepare()
    try:
        for stage in (extract, transform, load, dq, mart, backup):
            stage()
        return finish()
    except Exception as e:
        fail(f'{type(e).__name__}: {e}')
        raise
