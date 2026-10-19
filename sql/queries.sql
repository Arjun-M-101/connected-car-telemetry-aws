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

-- ===== v4: schema drift and data quality =====
-- 7. Readings the pipeline flagged (out of range, not numeric, id mismatch ...)
SELECT vehicle_id, event_time, dq_issues FROM telemetry WHERE dq_issues IS NOT NULL ORDER BY ingest_ts DESC LIMIT 50;

-- 8. Unknown / new sensor fields captured in `extras` (JSON text) - nothing is lost
SELECT vehicle_id, event_time, extras FROM telemetry WHERE extras IS NOT NULL ORDER BY ingest_ts DESC LIMIT 20;

-- 9. Read one new field out of `extras` without changing any table
SELECT vehicle_id, json_extract_scalar(extras, '$.battery_v') AS battery_v, json_extract_scalar(extras, '$.fw_version') AS fw_version
FROM telemetry WHERE extras IS NOT NULL LIMIT 20;

-- 10. Which message formats are in use?
SELECT schema_version, count(*) AS readings FROM telemetry GROUP BY schema_version;

-- 11. Is every vehicle still reporting? (age of the last message per vehicle)
SELECT vehicle_id, max(from_iso8601_timestamp(event_time)) AS last_event,
       date_diff('minute', max(from_iso8601_timestamp(event_time)), now()) AS minutes_ago
FROM telemetry WHERE dt >= date_format(current_date - interval '1' day, '%Y-%m-%d') GROUP BY vehicle_id ORDER BY minutes_ago DESC;

-- telemetry_dedup (v5.0): one row per (vehicle, event_time). QoS 1 and the device outbox can deliver a reading
-- twice; the raw history keeps both on purpose (nothing is lost), reports should use this form.
SELECT * FROM (
  SELECT t.*, row_number() OVER (PARTITION BY vehicle_id, event_time ORDER BY ingest_ts) AS rn
  FROM telemetry t
  WHERE dt >= date_format(current_date - interval '2' day, '%Y-%m-%d')
) WHERE rn = 1;

-- clock and time problems flagged by the validation Lambda (v5.0)
SELECT vehicle_id, event_time, dq_issues FROM telemetry
WHERE dq_issues LIKE '%event_time%' AND dt >= date_format(current_date - interval '2' day, '%Y-%m-%d');
