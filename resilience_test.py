"""Resilience test (v5.0): sends the messy situations real cars produce and checks, on the live stack, that the
pipeline does the right thing. Vehicles RT-001 and RT-002 never mix with the demo cars.

  python3 resilience_test.py            # real-time checks (about 1 minute)
  python3 resilience_test.py --athena   # also checks the history table (adds about 4 minutes)

Checks:
  1. late reading (replayed from the device outbox) does not overwrite newer state
  2. duplicate (QoS 1 re-delivery) is harmless
  3. device clock 2 hours ahead is rejected for the latest state, flagged future_event_time in history
  4. impossible value (-5 psi) is not stored and raises no alert, flagged out_of_range in history
  5. "Z" time format with milliseconds compares correctly with "+00:00"
  6. low tyre: alert becomes active once; 28.5 psi (inside the hysteresis band) keeps it; 30 psi clears it
"""
import argparse, json, time
from datetime import datetime, timedelta, timezone
import boto3

ok_all = True


def check(name, cond, detail=""):
    global ok_all
    ok_all &= bool(cond)
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f"  ({detail})" if detail else ""))


def iso(dt, z=False):
    s = dt.isoformat(timespec="milliseconds" if z else "seconds")
    return s.replace("+00:00", "Z") if z else s


def base(vid, t, **kw):
    m = {"vehicle_id": vid, "event_time": t, "tyre_fl_psi": 33.0, "tyre_fr_psi": 33.0, "tyre_rl_psi": 33.0,
         "tyre_rr_psi": 33.0, "accelerator_pct": 20.0, "clutch_pct": 0.0, "speed_kmph": 10.0,
         "fuel_level_pct": 60.0, "fuel_efficiency_kmpl": 15.0}
    m.update(kw)
    return m


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--region", default="ap-south-1")
    ap.add_argument("--table", default="vehicle-latest-state")
    ap.add_argument("--athena", action="store_true", help="also check the history table (waits for Firehose)")
    a = ap.parse_args()
    ep = boto3.client("iot", region_name=a.region).describe_endpoint(endpointType="iot:Data-ATS")["endpointAddress"]
    iot = boto3.client("iot-data", region_name=a.region, endpoint_url=f"https://{ep}")
    ddb = boto3.client("dynamodb", region_name=a.region)

    def send(m, vid=None):
        iot.publish(topic=f"vehicles/{vid or m['vehicle_id']}/telemetry", qos=1, payload=json.dumps(m))

    def item(vid):
        return ddb.get_item(TableName=a.table, Key={"vehicle_id": {"S": vid}}, ConsistentRead=True).get("Item", {})

    for vid in ("RT-001", "RT-002"):   # start clean, so a re-run gives the same result
        ddb.delete_item(TableName=a.table, Key={"vehicle_id": {"S": vid}})

    now = datetime.now(timezone.utc)
    print("Part 1: late, duplicate, future-clock, impossible value and mixed time formats (RT-001)")
    r_new = base("RT-001", iso(now - timedelta(seconds=5)), speed_kmph=20.0)
    msgs = [base("RT-001", iso(now - timedelta(seconds=10)), speed_kmph=10.0),
            r_new,
            base("RT-001", iso(now - timedelta(seconds=8)), speed_kmph=99.0),        # late replay
            r_new,                                                                   # duplicate
            base("RT-001", iso(now + timedelta(hours=2)), speed_kmph=77.0),          # clock 2 h ahead
            base("RT-001", iso(now - timedelta(seconds=4)), speed_kmph=30.0, tyre_rr_psi=-5),  # sensor fault
            base("RT-001", iso(now - timedelta(seconds=3), z=True), speed_kmph=40.0)]          # newest, Z format
    for m in msgs:
        send(m)
    time.sleep(8)
    it = item("RT-001")
    speed = float(it.get("speed_kmph", {}).get("N", "nan"))
    check("newest valid reading is the latest state (speed 40)", speed == 40.0, f"speed={speed}")
    check("late reading (speed 99) did not overwrite", speed != 99.0)
    check("future-clock reading (speed 77) was rejected", speed != 77.0)
    check("Z-format time with milliseconds compared correctly", it.get("event_time", {}).get("S", "").endswith("Z"),
          it.get("event_time", {}).get("S", "missing"))
    check("no alert from the impossible -5 psi value", "alert_active" not in it)

    print("Part 2: low tyre with hysteresis (RT-002)")
    t = now
    for i in range(5):
        t += timedelta(seconds=1)
        send(base("RT-002", iso(t), tyre_fl_psi=25.0))
        time.sleep(0.5)
    time.sleep(6)
    it = item("RT-002")
    check("alert active after low readings (1 e-mail, not 5)", it.get("alert_active", {}).get("BOOL") is True)
    t += timedelta(seconds=1)
    send(base("RT-002", iso(t), tyre_fl_psi=28.5))
    time.sleep(6)
    check("28.5 psi is inside the hysteresis band: alert stays active", item("RT-002").get("alert_active", {}).get("BOOL") is True)
    t += timedelta(seconds=1)
    send(base("RT-002", iso(t), tyre_fl_psi=30.0))
    time.sleep(6)
    check("30 psi clears the alert (1 'back to normal' e-mail)", "alert_active" not in item("RT-002"))
    print("  E-mail: expect 'Low tyre pressure: RT-002' and 'Tyre pressure back to normal: RT-002' (2 in total).")

    if a.athena:
        print("Part 3: history table (waiting 240 s for the validation step and Firehose to write) ...")
        time.sleep(240)
        ath = boto3.client("athena", region_name=a.region)
        day = now.strftime("%Y-%m-%d")
        q = ath.start_query_execution(
            QueryString=f"SELECT speed_kmph, dq_issues FROM telemetry WHERE vehicle_id = 'RT-001' AND dt >= '{day}'",
            QueryExecutionContext={"Database": "vehicle_telemetry"}, WorkGroup="vehicle-telemetry")["QueryExecutionId"]
        while True:
            st = ath.get_query_execution(QueryExecutionId=q)["QueryExecution"]["Status"]["State"]
            if st in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            time.sleep(1)
        rows = []
        if st == "SUCCEEDED":
            res = ath.get_query_results(QueryExecutionId=q)["ResultSet"]["Rows"][1:]
            rows = [tuple(c.get("VarCharValue") for c in r["Data"]) for r in res]
        flags = {r[1] for r in rows}
        check("history kept every message (7, including the duplicate and the late one)", len(rows) == 7, f"{len(rows)} rows")
        check("future-clock reading flagged future_event_time", "future_event_time" in flags)
        check("-5 psi flagged out_of_range:tyre_rr_psi", "out_of_range:tyre_rr_psi" in flags)
        print("  De-duplicate in reports with the query 'telemetry_dedup' in sql/queries.sql.")

    print("\nRESULT: " + ("ALL CHECKS PASSED" if ok_all else "SOME CHECKS FAILED (see above)"))
    raise SystemExit(0 if ok_all else 1)


if __name__ == "__main__":
    main()
