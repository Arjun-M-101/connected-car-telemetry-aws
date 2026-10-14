"""Send a fixed set of test events for vehicle CAR-900 to check validation, schema drift and alerts.
Usage: python3 send_test_events.py            # safe events only
       python3 send_test_events.py --alert    # also sends one low-tyre event (this DOES email you)
Then see README.md, Playbook step 5 (schema drift and validation) for what to expect in Athena."""
import argparse, json
from datetime import datetime, timezone

def base():
    return {"vehicle_id": "CAR-900", "event_time": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "tyre_fl_psi": 33.0, "tyre_fr_psi": 33.0, "tyre_rl_psi": 32.5, "tyre_rr_psi": 32.8,
            "accelerator_pct": 20.0, "clutch_pct": 0.0, "speed_kmph": 55.0,
            "fuel_level_pct": 70.0, "fuel_efficiency_kmpl": 15.5}

def build_events(include_alert=False):
    ev = []
    ev.append(("valid", base()))
    ev.append(("new-fields (schema drift)", {**base(), "battery_v": 12.6, "fw_version": "2.0.0",
                                              "gps": {"lat": 13.0827, "lon": 80.2707}}))
    ev.append(("numbers-as-strings", {**base(), "tyre_fl_psi": "31.5", "speed_kmph": "60"}))
    ev.append(("non-numeric value", {**base(), "tyre_fl_psi": "flat"}))
    ev.append(("out-of-range value", {**base(), "tyre_rr_psi": -5, "fuel_level_pct": 140}))
    ev.append(("payload id differs from topic id", {**base(), "vehicle_id": "CAR-777"}))
    p = base(); del p["vehicle_id"]
    ev.append(("no vehicle_id in payload (topic id used)", p))
    if include_alert:
        ev.append(("LOW TYRE (sends alert email)", {**base(), "tyre_fl_psi": 22.0}))
    return ev

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--alert", action="store_true")
    ap.add_argument("--region", default="ap-south-1")
    a = ap.parse_args()
    import boto3
    ep = boto3.client("iot", region_name=a.region).describe_endpoint(endpointType="iot:Data-ATS")["endpointAddress"]
    iot = boto3.client("iot-data", region_name=a.region, endpoint_url=f"https://{ep}")
    for name, payload in build_events(a.alert):
        iot.publish(topic="vehicles/CAR-900/telemetry", qos=1, payload=json.dumps(payload))
        print("sent:", name)
    print("Wait ~2 minutes (Firehose buffer) before querying Athena.")

if __name__ == "__main__":
    main()
