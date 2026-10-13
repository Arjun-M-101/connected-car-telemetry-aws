"""Fake car sensors -> AWS IoT Core. Usage: python simulator.py --vehicles 3 --count 120 [--leak]
Needs: pip install boto3, and AWS credentials with iot:DescribeEndpoint + iot:Publish."""
import argparse, json, random, time
from datetime import datetime, timezone
import boto3

def make_state(vid):
    return {"vehicle_id": vid, "tyres": [random.uniform(32, 35) for _ in range(4)],
            "fuel": random.uniform(60, 90), "speed": 0.0}

def step(s, leak):
    pedal = max(0, min(100, random.gauss(35, 25)))
    clutch = 100.0 if random.random() < 0.08 else 0.0
    s["speed"] = max(0, min(140, s["speed"] + (pedal - 30) * 0.15 + random.gauss(0, 1)))
    s["tyres"] = [t + random.gauss(0, 0.05) for t in s["tyres"]]
    if leak and s["vehicle_id"].endswith("1"):
        s["tyres"][0] -= 0.25                      # front-left slow leak on vehicle ...1
    kmpl = max(4, min(25, 22 - pedal * 0.12 + random.gauss(0, 0.8)))
    s["fuel"] = max(0, s["fuel"] - 0.01 - pedal / 20000)
    t = s["tyres"]
    return {"vehicle_id": s["vehicle_id"],
            "event_time": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "tyre_fl_psi": round(t[0], 1), "tyre_fr_psi": round(t[1], 1),
            "tyre_rl_psi": round(t[2], 1), "tyre_rr_psi": round(t[3], 1),
            "accelerator_pct": round(pedal, 1), "clutch_pct": clutch,
            "speed_kmph": round(s["speed"], 1), "fuel_level_pct": round(s["fuel"], 1),
            "fuel_efficiency_kmpl": round(kmpl, 1)}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--vehicles", type=int, default=3)
    ap.add_argument("--count", type=int, default=60, help="batches to send; 0 = forever")
    ap.add_argument("--interval", type=float, default=2.0)
    ap.add_argument("--leak", action="store_true", help="simulate a slow tyre leak (triggers alert email)")
    ap.add_argument("--region", default="ap-south-1")
    a = ap.parse_args()
    ep = boto3.client("iot", region_name=a.region).describe_endpoint(endpointType="iot:Data-ATS")["endpointAddress"]
    iot = boto3.client("iot-data", region_name=a.region, endpoint_url=f"https://{ep}")
    cars = [make_state(f"CAR-{i:03d}") for i in range(1, a.vehicles + 1)]
    n = 0
    while a.count == 0 or n < a.count:   # --count 0 = run until Ctrl+C (live demo)
        n += 1
        for c in cars:
            msg = step(c, a.leak)
            iot.publish(topic=f"vehicles/{c['vehicle_id']}/telemetry", qos=1, payload=json.dumps(msg))
        print(f"batch {n}/{a.count or 'inf'} sent: {msg}")
        time.sleep(a.interval)

if __name__ == "__main__":
    main()
