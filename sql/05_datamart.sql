-- =====================================================================
-- 05_datamart.sql : data mart (schema mart) untuk dashboard per stakeholder
-- Dijalankan pipeline (task build_mart) setelah DQ lolos. Materialized view dibangun ulang
-- setiap run sehingga dashboard membaca agregat siap pakai, bukan menghitung ulang 1 juta baris.
-- Role hr_dashboard hanya boleh membaca objek di schema ini (lihat 04_rbac.sql).
-- =====================================================================
SET client_min_messages = warning;

-- schema mart sudah dibuat oleh 04_rbac.sql; blok ini untuk lingkungan tanpa RBAC (mis. Colab)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'mart') THEN
        EXECUTE 'CREATE SCHEMA mart';
    END IF;
END $$;

BEGIN;

DROP MATERIALIZED VIEW IF EXISTS mart.trip_daily, mart.station_hourly, mart.weather_hourly, mart.route_monthly CASCADE;
DROP VIEW IF EXISTS mart.station, mart.kpi_monthly, mart.etl_run_history, mart.dq_latest CASCADE;

-- Stasiun dengan nama dan lokasi terkini (versi SCD2 lama dipetakan ke nama terkini)
CREATE VIEW mart.station AS
SELECT s.station_key, s.station_id, cur.station_name, cur.city, cur.latitude, cur.longitude,
       s.station_name AS station_name_versi, s.valid_from, s.valid_to, s.is_current
FROM dw.dim_station s
JOIN dw.dim_station cur ON cur.station_id = s.station_id AND cur.is_current;

-- Ringkasan Eksekutif, Cuaca & Kalender, Rute & Pendapatan: perjalanan harian per segmen
CREATE MATERIALIZED VIEW mart.trip_daily AS
SELECT d.full_date, d.year_month, d.month, d.month_name, d.quarter, d.season, d.day_of_week, d.day_name,
       d.day_type, d.is_holiday, d.holiday_name, u.user_type, b.bike_label AS bike_type,
       count(*)                                   AS trips,
       sum(f.estimated_revenue)                   AS revenue,
       round(avg(f.duration_min), 2)              AS avg_duration_min,
       round(avg(f.distance_km), 3)               AS avg_distance_km,
       count(*) FILTER (WHERE f.is_round_trip)    AS round_trips
FROM dw.fact_trip f
JOIN dw.dim_date d      ON d.date_key = f.date_key
JOIN dw.dim_user_type u ON u.user_type_key = f.user_type_key
JOIN dw.dim_bike_type b ON b.bike_type_key = f.bike_type_key
GROUP BY d.full_date, d.year_month, d.month, d.month_name, d.quarter, d.season, d.day_of_week, d.day_name,
         d.day_type, d.is_holiday, d.holiday_name, u.user_type, b.bike_label;

-- Permintaan & Stasiun: keberangkatan, kedatangan, net flow per stasiun per jam
CREATE MATERIALIZED VIEW mart.station_hourly AS
SELECT d.full_date, d.year_month, d.month_name, d.day_name, d.day_type, d.is_holiday,
       t.time_key AS hour, t.time_band, t.is_peak_commute,
       st.station_id, st.station_name, st.city, st.latitude, st.longitude,
       w.weather_condition, w.temp_band, w.rain_band,
       h.departures, h.arrivals, h.net_flow, h.member_departures, h.casual_departures,
       h.avg_duration_min, h.revenue
FROM dw.fact_station_hourly h
JOIN dw.dim_date d    ON d.date_key = h.date_key
JOIN dw.dim_time t    ON t.time_key = h.time_key
JOIN mart.station st  ON st.station_key = h.station_key
JOIN dw.dim_weather w ON w.weather_key = h.weather_key;

-- Cuaca & Kalender: jumlah perjalanan untuk SETIAP jam (termasuk jam tanpa perjalanan)
CREATE MATERIALIZED VIEW mart.weather_hourly AS
SELECT w.obs_datetime, d.full_date, d.year_month, d.day_type, d.is_holiday, w.hour,
       w.weather_condition, w.temperature_c, w.precip_mm, w.wind_speed_ms, w.visibility_km,
       w.temp_band, w.rain_band, w.wind_band, w.is_imputed,
       COALESCE(sum(h.departures), 0) AS trips
FROM dw.dim_weather w
JOIN dw.dim_date d ON d.date_key = w.date_key
LEFT JOIN dw.fact_station_hourly h ON h.weather_key = w.weather_key
WHERE w.obs_datetime >= (SELECT date_trunc('day', min(started_at)) FROM dw.fact_trip)
  AND w.obs_datetime <  (SELECT date_trunc('day', max(started_at)) + interval '1 day' FROM dw.fact_trip)
GROUP BY w.obs_datetime, d.full_date, d.year_month, d.day_type, d.is_holiday, w.hour, w.weather_condition,
         w.temperature_c, w.precip_mm, w.wind_speed_ms, w.visibility_km, w.temp_band, w.rain_band,
         w.wind_band, w.is_imputed;

-- Rute & Pendapatan: rute asal-tujuan per bulan dengan nama stasiun terkini
CREATE MATERIALIZED VIEW mart.route_monthly AS
SELECT d.year_month, r.month_key,
       s1.station_id AS start_station_id, s1.station_name AS start_station_name,
       s2.station_id AS end_station_id,   s2.station_name AS end_station_name,
       r.trips, r.member_trips, r.casual_trips, r.avg_duration_min, r.total_revenue
FROM dw.fact_route_monthly r
JOIN dw.dim_date d     ON d.date_key = r.month_key
JOIN mart.station s1   ON s1.station_key = r.start_station_key
JOIN mart.station s2   ON s2.station_key = r.end_station_key;

-- Ringkasan Eksekutif: KPI bulanan dan pertumbuhan dibanding bulan sebelumnya
CREATE VIEW mart.kpi_monthly AS
WITH m AS (
    SELECT year_month, sum(trips) AS trips, sum(revenue) AS revenue,
           sum(trips) FILTER (WHERE user_type = 'member') AS member_trips,
           sum(avg_duration_min * trips) / sum(trips) AS avg_duration_min
    FROM mart.trip_daily GROUP BY year_month
)
SELECT year_month, trips, revenue,
       round(100.0 * member_trips / trips, 1)          AS member_share_pct,
       round(avg_duration_min, 2)                       AS avg_duration_min,
       round(100.0 * (trips - lag(trips) OVER w) / lag(trips) OVER w, 1) AS trips_growth_pct
FROM m WINDOW w AS (ORDER BY year_month);

-- Kualitas Data: riwayat run dan hasil DQ run sukses terakhir
CREATE VIEW mart.etl_run_history AS
SELECT batch_id, status, run_mode, months, rows_extracted, rows_rejected, rows_loaded, started_at, finished_at,
       round(extract(epoch FROM finished_at - started_at)::numeric, 1) AS duration_sec
FROM audit.etl_run;

CREATE VIEW mart.dq_latest AS
SELECT q.batch_id, q.check_name, q.severity, q.expected, q.actual, q.passed, q.checked_at
FROM audit.dq_result q
WHERE q.batch_id = (SELECT batch_id FROM audit.dq_result ORDER BY checked_at DESC LIMIT 1);

CREATE INDEX ON mart.trip_daily (full_date);
CREATE INDEX ON mart.station_hourly (full_date, station_id);
CREATE INDEX ON mart.route_monthly (year_month);

-- objek dibuat ulang, jadi hak baca diberikan lagi secara eksplisit (bila role tersedia)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'hr_dashboard') THEN
        EXECUTE 'GRANT SELECT ON ALL TABLES IN SCHEMA mart TO hr_dashboard';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'hr_analyst') THEN
        EXECUTE 'GRANT SELECT ON ALL TABLES IN SCHEMA mart TO hr_analyst';
    END IF;
END $$;

COMMIT;
