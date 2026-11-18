-- =====================================================================
-- HudsonRide Bike Share - Data Warehouse (PostgreSQL 16)
-- Schema dw: galaxy schema / fact constellation
--   6 dimensi : dim_date, dim_time, dim_station (SCD Type 2), dim_user_type,
--               dim_bike_type, dim_weather
--   3 fakta   : fact_trip            (grain: 1 perjalanan)
--               fact_station_hourly  (grain: 1 stasiun x 1 jam)
--               fact_route_monthly   (grain: 1 rute asal-tujuan x 1 bulan)
--
-- Cara pakai:
--   createdb hudsonride_dw
--   psql -d hudsonride_dw -v ON_ERROR_STOP=1 -f hudsonride_dw_schema.sql
--
-- File ini sama dengan bagian schema dw pada pipeline/sql/01_schema.sql
-- yang dijalankan notebook ETL (schema stg dan audit tidak disertakan).
-- =====================================================================

SET client_min_messages = warning;
CREATE SCHEMA IF NOT EXISTS dw;

-- ---------------------------------------------------------------------
-- DIMENSI
-- ---------------------------------------------------------------------

-- Kalender harian; conformed untuk ketiga fakta
CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_key        int PRIMARY KEY,                -- YYYYMMDD
    full_date       date NOT NULL UNIQUE,
    day             smallint NOT NULL,
    month           smallint NOT NULL,
    month_name      text NOT NULL,
    quarter         smallint NOT NULL,
    year            smallint NOT NULL,
    year_month      text NOT NULL,                  -- YYYY-MM
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

-- Jam dalam sehari (0-23)
CREATE TABLE IF NOT EXISTS dw.dim_time (
    time_key        smallint PRIMARY KEY CHECK (time_key BETWEEN 0 AND 23),
    hour_label      text NOT NULL,                  -- 00:00 ... 23:00
    time_band       text NOT NULL,
    is_peak_commute boolean NOT NULL
);

-- Tipe pengguna + asumsi tarif (dipakai untuk estimated_revenue)
CREATE TABLE IF NOT EXISTS dw.dim_user_type (
    user_type_key   smallint PRIMARY KEY,
    user_type       text NOT NULL UNIQUE,           -- member / casual
    base_fee        numeric(6,2) NOT NULL,
    free_minutes    smallint NOT NULL,
    per_minute      numeric(6,2) NOT NULL
);

-- Jenis sepeda
CREATE TABLE IF NOT EXISTS dw.dim_bike_type (
    bike_type_key   smallint PRIMARY KEY,
    bike_type       text NOT NULL UNIQUE,           -- electric_bike / classic_bike
    bike_label      text NOT NULL,
    is_electric     boolean NOT NULL
);

-- Cuaca per jam (NOAA KCDW, waktu lokal New York)
CREATE TABLE IF NOT EXISTS dw.dim_weather (
    weather_key       bigint PRIMARY KEY,           -- YYYYMMDDHH
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

-- Stasiun, SCD Type 2 pada station_name; role-playing (asal dan tujuan)
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

-- Isi dimensi statis
INSERT INTO dw.dim_time (time_key, hour_label, time_band, is_peak_commute)
SELECT h, lpad(h::text, 2, '0') || ':00',
       CASE WHEN h < 6 THEN 'Dini hari (00-05)' WHEN h < 10 THEN 'Pagi (06-09)' WHEN h < 16 THEN 'Siang (10-15)'
            WHEN h < 20 THEN 'Sore (16-19)' ELSE 'Malam (20-23)' END,
       h = ANY (ARRAY[7, 8, 9, 16, 17, 18, 19])
FROM generate_series(0, 23) AS h
ON CONFLICT (time_key) DO NOTHING;

INSERT INTO dw.dim_user_type (user_type_key, user_type, base_fee, free_minutes, per_minute) VALUES
(1, 'member', 2.00, 45, 0.20),
(2, 'casual', 4.79, 30, 0.30)
ON CONFLICT (user_type_key) DO NOTHING;

INSERT INTO dw.dim_bike_type (bike_type_key, bike_type, bike_label, is_electric) VALUES
(1, 'electric_bike', 'Electric', true),
(2, 'classic_bike', 'Classic', false)
ON CONFLICT (bike_type_key) DO NOTHING;

-- ---------------------------------------------------------------------
-- FAKTA
-- ---------------------------------------------------------------------

-- Satu baris per perjalanan; partisi per bulan pada started_at
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
    PRIMARY KEY (ride_id, started_at)            -- kolom partisi wajib masuk primary key
) PARTITION BY RANGE (started_at);

-- Partisi Januari-Desember 2024 (pipeline juga membuatnya otomatis saat load)
DO $$
DECLARE m date;
BEGIN
    FOR m IN SELECT generate_series(date '2024-01-01', date '2024-12-01', interval '1 month')::date LOOP
        EXECUTE format('CREATE TABLE IF NOT EXISTS dw.%I PARTITION OF dw.fact_trip FOR VALUES FROM (%L) TO (%L)',
                       'fact_trip_' || to_char(m, 'YYYY_MM'), m, (m + interval '1 month')::date);
    END LOOP;
END $$;

CREATE INDEX IF NOT EXISTS ix_fact_trip_started_brin ON dw.fact_trip USING brin (started_at);
CREATE INDEX IF NOT EXISTS ix_fact_trip_date          ON dw.fact_trip (date_key);
CREATE INDEX IF NOT EXISTS ix_fact_trip_start_station ON dw.fact_trip (start_station_key);
CREATE INDEX IF NOT EXISTS ix_fact_trip_end_station   ON dw.fact_trip (end_station_key);
CREATE INDEX IF NOT EXISTS ix_fact_trip_weather       ON dw.fact_trip (weather_key);

-- Satu baris per stasiun per jam (keberangkatan dan kedatangan)
CREATE TABLE IF NOT EXISTS dw.fact_station_hourly (
    date_key           int NOT NULL REFERENCES dw.dim_date (date_key),
    time_key           smallint NOT NULL REFERENCES dw.dim_time (time_key),
    station_key        int NOT NULL REFERENCES dw.dim_station (station_key),
    weather_key        bigint NOT NULL REFERENCES dw.dim_weather (weather_key),
    departures         int NOT NULL,
    arrivals           int NOT NULL,
    net_flow           int NOT NULL,                -- arrivals - departures
    member_departures  int NOT NULL DEFAULT 0,
    casual_departures  int NOT NULL DEFAULT 0,
    avg_duration_min   numeric(7,2),                -- NULL bila jam itu tanpa keberangkatan
    revenue            numeric(12,2) NOT NULL,
    etl_batch_id       text NOT NULL,
    PRIMARY KEY (date_key, time_key, station_key),
    CHECK (member_departures + casual_departures = departures)
);
CREATE INDEX IF NOT EXISTS ix_fsh_station ON dw.fact_station_hourly (station_key);
CREATE INDEX IF NOT EXISTS ix_fsh_weather ON dw.fact_station_hourly (weather_key);

-- Satu baris per rute asal-tujuan per bulan
CREATE TABLE IF NOT EXISTS dw.fact_route_monthly (
    month_key          int NOT NULL REFERENCES dw.dim_date (date_key),   -- tanggal 1 bulan tersebut
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

-- ---------------------------------------------------------------------
-- Keterangan tabel (tampil di pgAdmin / dbdiagram)
-- ---------------------------------------------------------------------
COMMENT ON TABLE dw.dim_date            IS 'Kalender harian 2024 dengan penanda hari libur AS/New Jersey dan day_type';
COMMENT ON TABLE dw.dim_time            IS 'Jam 0-23 dengan time_band dan penanda jam sibuk';
COMMENT ON TABLE dw.dim_station         IS 'Stasiun, SCD Type 2 pada station_name; role-playing asal/tujuan';
COMMENT ON TABLE dw.dim_user_type       IS 'member/casual beserta asumsi tarif';
COMMENT ON TABLE dw.dim_bike_type       IS 'electric_bike/classic_bike';
COMMENT ON TABLE dw.dim_weather         IS 'Cuaca per jam NOAA KCDW, waktu lokal New York';
COMMENT ON TABLE dw.fact_trip           IS 'Grain: satu perjalanan';
COMMENT ON TABLE dw.fact_station_hourly IS 'Grain: satu stasiun dalam satu jam';
COMMENT ON TABLE dw.fact_route_monthly  IS 'Grain: satu pasangan stasiun asal-tujuan dalam satu bulan';
