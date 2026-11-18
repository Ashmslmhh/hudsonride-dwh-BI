-- =====================================================================
-- HudsonRide Bike Share - Data Warehouse
-- Versi MySQL KHUSUS untuk menggambar ERD di MySQL Workbench.
-- Database yang dipakai pipeline tetap PostgreSQL (hudsonride_dw_schema.sql);
-- tipe data di sini disesuaikan ke MySQL: text -> VARCHAR, boolean -> TINYINT(1),
-- timestamp -> DATETIME. Partisi bulanan fact_trip tidak ditulis karena MySQL
-- tidak mendukung foreign key pada tabel berpartisi.
--
-- MySQL Workbench: File > Import > Reverse Engineer MySQL Create Script...
--   pilih file ini, centang "Place imported objects on a diagram", lalu Execute.
-- =====================================================================

CREATE DATABASE IF NOT EXISTS hudsonride_dw;
USE hudsonride_dw;

-- ---------------------------- DIMENSI --------------------------------

CREATE TABLE dim_date (
    date_key        INT NOT NULL,
    full_date       DATE NOT NULL,
    `day`           SMALLINT NOT NULL,
    `month`         SMALLINT NOT NULL,
    month_name      VARCHAR(12) NOT NULL,
    `quarter`       SMALLINT NOT NULL,
    `year`          SMALLINT NOT NULL,
    `year_month`    CHAR(7) NOT NULL,
    day_of_week     SMALLINT NOT NULL,
    day_name        VARCHAR(12) NOT NULL,
    is_weekend      TINYINT(1) NOT NULL,
    season          VARCHAR(10) NOT NULL,
    is_holiday      TINYINT(1) NOT NULL,
    holiday_name    VARCHAR(80),
    holiday_type    VARCHAR(40),
    day_type        VARCHAR(10) NOT NULL,
    etl_batch_id    VARCHAR(20) NOT NULL,
    PRIMARY KEY (date_key),
    UNIQUE KEY uq_dim_date_full_date (full_date)
) ENGINE=InnoDB;

CREATE TABLE dim_time (
    time_key        SMALLINT NOT NULL,
    hour_label      CHAR(5) NOT NULL,
    time_band       VARCHAR(20) NOT NULL,
    is_peak_commute TINYINT(1) NOT NULL,
    PRIMARY KEY (time_key)
) ENGINE=InnoDB;

CREATE TABLE dim_user_type (
    user_type_key   SMALLINT NOT NULL,
    user_type       VARCHAR(10) NOT NULL,
    base_fee        DECIMAL(6,2) NOT NULL,
    free_minutes    SMALLINT NOT NULL,
    per_minute      DECIMAL(6,2) NOT NULL,
    PRIMARY KEY (user_type_key),
    UNIQUE KEY uq_dim_user_type (user_type)
) ENGINE=InnoDB;

CREATE TABLE dim_bike_type (
    bike_type_key   SMALLINT NOT NULL,
    bike_type       VARCHAR(20) NOT NULL,
    bike_label      VARCHAR(20) NOT NULL,
    is_electric     TINYINT(1) NOT NULL,
    PRIMARY KEY (bike_type_key),
    UNIQUE KEY uq_dim_bike_type (bike_type)
) ENGINE=InnoDB;

CREATE TABLE dim_weather (
    weather_key       BIGINT NOT NULL,
    obs_datetime      DATETIME NOT NULL,
    date_key          INT NOT NULL,
    `hour`            SMALLINT NOT NULL,
    temperature_c     DECIMAL(5,2),
    dew_point_c       DECIMAL(5,2),
    wind_speed_ms     DECIMAL(5,2),
    visibility_km     DECIMAL(6,2),
    precip_mm         DECIMAL(6,2) NOT NULL,
    obs_count         SMALLINT NOT NULL,
    is_imputed        TINYINT(1) NOT NULL,
    weather_condition VARCHAR(20) NOT NULL,
    temp_band         VARCHAR(20) NOT NULL,
    rain_band         VARCHAR(20) NOT NULL,
    wind_band         VARCHAR(20) NOT NULL,
    etl_batch_id      VARCHAR(20) NOT NULL,
    PRIMARY KEY (weather_key),
    UNIQUE KEY uq_dim_weather_obs (obs_datetime)
) ENGINE=InnoDB;

CREATE TABLE dim_station (
    station_key     INT NOT NULL AUTO_INCREMENT,
    station_id      VARCHAR(20) NOT NULL,
    station_name    VARCHAR(120) NOT NULL,
    latitude        DECIMAL(9,6),
    longitude       DECIMAL(9,6),
    city            VARCHAR(30) NOT NULL,
    valid_from      DATETIME NOT NULL,
    valid_to        DATETIME NOT NULL DEFAULT '9999-12-31 00:00:00',
    is_current      TINYINT(1) NOT NULL DEFAULT 1,
    etl_batch_id    VARCHAR(20) NOT NULL,
    PRIMARY KEY (station_key),
    UNIQUE KEY uq_dim_station_version (station_id, station_name)
) ENGINE=InnoDB;

-- ----------------------------- FAKTA ---------------------------------

CREATE TABLE fact_trip (
    ride_id            VARCHAR(32) NOT NULL,
    started_at         DATETIME NOT NULL,
    ended_at           DATETIME NOT NULL,
    date_key           INT NOT NULL,
    time_key           SMALLINT NOT NULL,
    start_station_key  INT NOT NULL,
    end_station_key    INT NOT NULL,
    bike_type_key      SMALLINT NOT NULL,
    user_type_key      SMALLINT NOT NULL,
    weather_key        BIGINT NOT NULL,
    duration_min       DECIMAL(7,2) NOT NULL,
    distance_km        DECIMAL(9,3),
    is_round_trip      TINYINT(1) NOT NULL,
    estimated_revenue  DECIMAL(10,2) NOT NULL,
    trip_count         SMALLINT NOT NULL DEFAULT 1,
    source_file        VARCHAR(80) NOT NULL,
    etl_batch_id       VARCHAR(20) NOT NULL,
    PRIMARY KEY (ride_id, started_at),
    CONSTRAINT fk_trip_date          FOREIGN KEY (date_key)          REFERENCES dim_date (date_key),
    CONSTRAINT fk_trip_time          FOREIGN KEY (time_key)          REFERENCES dim_time (time_key),
    CONSTRAINT fk_trip_start_station FOREIGN KEY (start_station_key) REFERENCES dim_station (station_key),
    CONSTRAINT fk_trip_end_station   FOREIGN KEY (end_station_key)   REFERENCES dim_station (station_key),
    CONSTRAINT fk_trip_bike_type     FOREIGN KEY (bike_type_key)     REFERENCES dim_bike_type (bike_type_key),
    CONSTRAINT fk_trip_user_type     FOREIGN KEY (user_type_key)     REFERENCES dim_user_type (user_type_key),
    CONSTRAINT fk_trip_weather       FOREIGN KEY (weather_key)       REFERENCES dim_weather (weather_key)
) ENGINE=InnoDB;

CREATE TABLE fact_station_hourly (
    date_key           INT NOT NULL,
    time_key           SMALLINT NOT NULL,
    station_key        INT NOT NULL,
    weather_key        BIGINT NOT NULL,
    departures         INT NOT NULL,
    arrivals           INT NOT NULL,
    net_flow           INT NOT NULL,
    member_departures  INT NOT NULL DEFAULT 0,
    casual_departures  INT NOT NULL DEFAULT 0,
    avg_duration_min   DECIMAL(7,2),
    revenue            DECIMAL(12,2) NOT NULL,
    etl_batch_id       VARCHAR(20) NOT NULL,
    PRIMARY KEY (date_key, time_key, station_key),
    CONSTRAINT fk_fsh_date    FOREIGN KEY (date_key)    REFERENCES dim_date (date_key),
    CONSTRAINT fk_fsh_time    FOREIGN KEY (time_key)    REFERENCES dim_time (time_key),
    CONSTRAINT fk_fsh_station FOREIGN KEY (station_key) REFERENCES dim_station (station_key),
    CONSTRAINT fk_fsh_weather FOREIGN KEY (weather_key) REFERENCES dim_weather (weather_key)
) ENGINE=InnoDB;

CREATE TABLE fact_route_monthly (
    month_key          INT NOT NULL,
    start_station_key  INT NOT NULL,
    end_station_key    INT NOT NULL,
    trips              INT NOT NULL,
    member_trips       INT NOT NULL,
    casual_trips       INT NOT NULL,
    avg_duration_min   DECIMAL(7,2) NOT NULL,
    total_revenue      DECIMAL(12,2) NOT NULL,
    etl_batch_id       VARCHAR(20) NOT NULL,
    PRIMARY KEY (month_key, start_station_key, end_station_key),
    CONSTRAINT fk_frm_month         FOREIGN KEY (month_key)         REFERENCES dim_date (date_key),
    CONSTRAINT fk_frm_start_station FOREIGN KEY (start_station_key) REFERENCES dim_station (station_key),
    CONSTRAINT fk_frm_end_station   FOREIGN KEY (end_station_key)   REFERENCES dim_station (station_key)
) ENGINE=InnoDB;
