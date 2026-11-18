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
