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
       h = ANY (ARRAY[7, 8, 9, 16, 17, 18, 19])
FROM generate_series(0, 23) AS h
ON CONFLICT (time_key) DO UPDATE SET hour_label = EXCLUDED.hour_label, time_band = EXCLUDED.time_band,
    is_peak_commute = EXCLUDED.is_peak_commute;

INSERT INTO dw.dim_user_type (user_type_key, user_type, base_fee, free_minutes, per_minute) VALUES
(1, 'member', 2.00, 45, 0.20),
(2, 'casual', 4.79, 30, 0.30)
ON CONFLICT (user_type_key) DO UPDATE SET user_type = EXCLUDED.user_type, base_fee = EXCLUDED.base_fee,
    free_minutes = EXCLUDED.free_minutes, per_minute = EXCLUDED.per_minute;

INSERT INTO dw.dim_bike_type (bike_type_key, bike_type, bike_label, is_electric) VALUES
(1, 'electric_bike', 'Electric', true),
(2, 'classic_bike', 'Classic', false)
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
