-- Run in Athena, workgroup vehicle-telemetry, database vehicle_telemetry
-- 1. Sanity check: latest rows
SELECT * FROM telemetry ORDER BY ingest_ts DESC LIMIT 20;

-- 2. Row count per vehicle
SELECT vehicle_id, count(*) AS readings FROM telemetry GROUP BY vehicle_id ORDER BY 1;

-- 3. Tyre pressure: lowest reading per vehicle (support-team view)
SELECT vehicle_id,
       min(least(tyre_fl_psi, tyre_fr_psi, tyre_rl_psi, tyre_rr_psi)) AS min_psi,
       count_if(least(tyre_fl_psi, tyre_fr_psi, tyre_rl_psi, tyre_rr_psi) < 28) AS low_events
FROM telemetry GROUP BY vehicle_id;

-- 4. Fuel efficiency (driver view)
SELECT vehicle_id, round(avg(fuel_efficiency_kmpl),2) AS avg_kmpl, round(min(fuel_level_pct),1) AS fuel_left_pct
FROM telemetry GROUP BY vehicle_id;

-- 5. Harsh acceleration events (accelerator > 85%)
SELECT vehicle_id, event_time, accelerator_pct, speed_kmph
FROM telemetry WHERE accelerator_pct > 85 ORDER BY event_time DESC LIMIT 50;

-- 6. Clutch usage share
SELECT vehicle_id, round(100.0*count_if(clutch_pct>0)/count(*),1) AS clutch_pressed_pct
FROM telemetry GROUP BY vehicle_id;
