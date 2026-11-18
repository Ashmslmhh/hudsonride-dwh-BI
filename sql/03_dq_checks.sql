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
