"""Load test (v5.0): N simulated vehicles publish 1 reading per second for S seconds through AWS IoT Core,
then the script measures how long the newest reading takes to appear in DynamoDB (the real-time path).

Usage (CloudShell, stack deployed):  python3 load_test.py --vehicles 100 --seconds 60
Vehicles are named LT-0001 ... so they never mix with the demo cars CAR-001 ... in queries.
Cost: 100 vehicles x 60 s = 6,000 messages, well under one US cent of IoT Core, Kinesis and Lambda charges."""
import argparse, json, statistics, threading, time
from concurrent.futures import ThreadPoolExecutor
import boto3
from botocore.config import Config
import simulator as sim


def pct(values, p):
    if not values:
        return float("nan")
    v = sorted(values)
    return v[min(len(v) - 1, int(round(p / 100 * (len(v) - 1))))]


def summary(name, values, unit="ms"):
    if not values:
        return f"{name}: no samples"
    return (f"{name}: n={len(values)}  p50={pct(values, 50):.0f} {unit}  p95={pct(values, 95):.0f} {unit}  "
            f"max={max(values):.0f} {unit}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--vehicles", type=int, default=100)
    ap.add_argument("--seconds", type=int, default=60)
    ap.add_argument("--workers", type=int, default=32, help="parallel publish threads")
    ap.add_argument("--sample", type=int, default=20, help="vehicles whose end-to-end latency is measured")
    ap.add_argument("--table", default="vehicle-latest-state")
    ap.add_argument("--region", default="ap-south-1")
    a = ap.parse_args()

    cfg = Config(max_pool_connections=a.workers + 4, retries={"max_attempts": 3, "mode": "standard"})
    ep = boto3.client("iot", region_name=a.region).describe_endpoint(endpointType="iot:Data-ATS")["endpointAddress"]
    iot = boto3.client("iot-data", region_name=a.region, endpoint_url=f"https://{ep}", config=cfg)
    ddb = boto3.client("dynamodb", region_name=a.region, config=cfg)

    cars = [sim.make_state(f"LT-{i:04d}") for i in range(1, a.vehicles + 1)]
    lock, pub_ms, errors, last = threading.Lock(), [], [], {}

    def send(car):
        msg = sim.step(car, leak=False)
        t0 = time.time()
        try:
            iot.publish(topic=f"vehicles/{car['vehicle_id']}/telemetry", qos=1, payload=json.dumps(msg))
            with lock:
                pub_ms.append((time.time() - t0) * 1000)
                last[car["vehicle_id"]] = (msg["event_time"], time.time())
        except Exception as e:  # throttling or network: counted, not fatal
            with lock:
                errors.append(type(e).__name__)

    print(f"Publishing: {a.vehicles} vehicles x 1 msg/s for {a.seconds} s = {a.vehicles * a.seconds} messages ...")
    start = time.time()
    with ThreadPoolExecutor(a.workers) as pool:
        for sec in range(a.seconds):
            tick = time.time()
            list(pool.map(send, cars))
            behind = time.time() - tick
            if sec % 10 == 9:
                print(f"  {sec + 1:4d} s: sent {len(pub_ms)}, errors {len(errors)}")
            time.sleep(max(0.0, 1.0 - behind))   # one reading per vehicle per second (event_time has 1 s resolution)
    took = time.time() - start
    sent = len(pub_ms)

    print("Measuring end-to-end latency to DynamoDB (newest reading of each sampled vehicle) ...")
    e2e, missing = [], []
    for vid in sorted(last)[: a.sample]:
        want, t_pub = last[vid]
        deadline = time.time() + 60
        while True:
            item = ddb.get_item(TableName=a.table, Key={"vehicle_id": {"S": vid}},
                                ConsistentRead=True).get("Item", {})
            if item.get("event_time", {}).get("S", "") >= want:
                e2e.append((time.time() - t_pub) * 1000)
                break
            if time.time() > deadline:
                missing.append(vid)
                break
            time.sleep(0.2)

    print("\n================ LOAD TEST RESULT ================")
    print(f"messages sent      : {sent} of {a.vehicles * a.seconds} in {took:.1f} s  ->  {sent / took:.0f} msg/s")
    print(f"publish errors     : {len(errors)} {dict((e, errors.count(e)) for e in set(errors)) if errors else ''}")
    print(summary("publish call (client -> IoT Core)", pub_ms))
    print(summary("end-to-end (publish -> DynamoDB)", e2e) + ("  (upper bound: includes 0.2 s polling)" if e2e else ""))
    if missing:
        print(f"NOT in DynamoDB after 60 s: {missing}")
    ok = not missing and sent >= 0.99 * a.vehicles * a.seconds
    print("VERDICT            : " + ("PASS" if ok else "CHECK (see above)"))
    print("Then: CloudWatch dashboard 'vehicle-telemetry' (stream records, throttles, state lag) and in Athena,\n"
          "after the Firehose buffer time:  SELECT count(*) FROM telemetry WHERE vehicle_id LIKE 'LT-%';")
    if e2e:
        print(json.dumps({"vehicles": a.vehicles, "seconds": a.seconds, "sent": sent, "msg_per_s": round(sent / took),
                          "errors": len(errors), "publish_p95_ms": round(pct(pub_ms, 95)),
                          "e2e_p50_ms": round(statistics.median(e2e)), "e2e_p95_ms": round(pct(e2e, 95))}))


if __name__ == "__main__":
    main()
