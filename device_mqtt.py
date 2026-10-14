"""A 'real-device style' sender: connects to AWS IoT Core over MQTT + TLS with an X.509 certificate,
exactly like production telematics hardware would (simulator.py instead uses AWS credentials over HTTPS).

Run after:  bash scripts/provision_device.sh CAR-101
  pip install awsiotsdk
  python3 device_mqtt.py --thing CAR-101 --count 30 --interval 2
Extra fields (seq, fw_version, battery_v) are sent on purpose: the pipeline keeps unknown fields in `extras`.
A small outbox keeps messages while the connection is down and re-sends them in order afterwards.
"""
import argparse, json, time
from collections import deque
import simulator as sim

class Outbox:
    """Store-and-forward buffer. Oldest messages are dropped if it overflows (bounded memory)."""
    def __init__(self, maxlen=1000):
        self.q = deque(maxlen=maxlen)
    def add(self, msg):
        self.q.append(msg)
    def flush(self, send):
        while self.q:
            try:
                send(self.q[0])
            except Exception:
                return False
            self.q.popleft()
        return True
    def __len__(self):
        return len(self.q)

def build_message(state, seq, leak=False):
    msg = sim.step(state, leak)
    msg.update({"seq": seq, "schema_version": "1", "fw_version": "1.0.3",
                "battery_v": round(12.4 + (seq % 7) * 0.05, 2)})
    return msg

def result_line(sent, buffered, interruptions, spoof_topic=None):
    """One clear verdict line printed at the end of a run (also used by the spoof security test)."""
    if spoof_topic:
        if sent == 0 and (buffered or interruptions):
            return (f"RESULT: publishing to vehicles/{spoof_topic}/telemetry was REJECTED "
                    f"({interruptions} connection drops, {buffered} messages not delivered). "
                    "This is the EXPECTED outcome: the device policy blocks other vehicles' topics.")
        return (f"RESULT: WARNING {sent} messages were accepted on another vehicle's topic. "
                "Check that vehicle-device-policy is attached and unchanged.")
    if buffered:
        return f"RESULT: {sent} sent, {buffered} still buffered (connection problems; they were not delivered)."
    return f"RESULT: all {sent} messages sent."

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--thing", required=True, help="e.g. CAR-101 (must be provisioned)")
    ap.add_argument("--count", type=int, default=30, help="0 = run forever")
    ap.add_argument("--interval", type=float, default=2.0)
    ap.add_argument("--leak", action="store_true")
    ap.add_argument("--endpoint", help="IoT data endpoint; looked up automatically if omitted")
    ap.add_argument("--region", default="ap-south-1")
    ap.add_argument("--spoof-topic", help="SECURITY TEST ONLY: publish as another vehicle id (should be denied by the device policy)")
    a = ap.parse_args()
    import boto3
    from awscrt import mqtt
    from awsiot import mqtt_connection_builder
    endpoint = a.endpoint or boto3.client("iot", region_name=a.region).describe_endpoint(
        endpointType="iot:Data-ATS")["endpointAddress"]
    d = f"certs/{a.thing}"
    stats = {"interruptions": 0}
    def interrupted(connection, error, **kw):
        stats["interruptions"] += 1
        print("connection interrupted:", error)
    conn = mqtt_connection_builder.mtls_from_path(
        endpoint=endpoint, cert_filepath=f"{d}/cert.pem", pri_key_filepath=f"{d}/private.key",
        ca_filepath=f"{d}/AmazonRootCA1.pem", client_id=a.thing,
        # spoof test: no persistent session, so rejected messages are not replayed on every reconnect
        clean_session=bool(a.spoof_topic), keep_alive_secs=30,
        on_connection_interrupted=interrupted,
        on_connection_resumed=lambda connection, return_code, session_present, **kw: print("connection resumed"))
    conn.connect().result(timeout=20)
    print(f"connected as {a.thing} to {endpoint}")
    topic = f"vehicles/{a.spoof_topic or a.thing}/telemetry"
    outbox, state, seq = Outbox(), sim.make_state(a.thing), 0

    def send(msg):
        fut, _ = conn.publish(topic=topic, payload=json.dumps(msg), qos=mqtt.QoS.AT_LEAST_ONCE)
        fut.result(timeout=10)

    sent = 0
    try:
        while a.count == 0 or seq < a.count:
            seq += 1
            before = len(outbox) + 1
            outbox.add(build_message(state, seq, a.leak))
            ok = outbox.flush(send)
            sent += before - len(outbox)
            print(f"seq {seq} {'sent' if ok else 'BUFFERED (offline), pending=' + str(len(outbox))}")
            time.sleep(a.interval)
    except KeyboardInterrupt:
        pass
    finally:
        try:
            conn.disconnect().result(timeout=10)
        except Exception as e:  # AWS IoT may already have closed the connection (e.g. policy denial)
            print(f"disconnect did not complete cleanly ({type(e).__name__}); the connection is closed anyway")
        print(result_line(sent, len(outbox), stats["interruptions"], a.spoof_topic))

if __name__ == "__main__":
    main()
