"""Tests for the v5.0 latest-state / alert Lambda and the offline check, both embedded in template.yaml.
They run against a small fake DynamoDB that evaluates the same update and condition expressions."""
import base64, json, os, re, sys, types
import pytest
from test_transform import load_template

T0 = 1791972000000   # 2026-10-14T10:00:00+00:00 in epoch ms


class ClientError(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


def _val(v):
    if "S" in v:
        return v["S"]
    if "N" in v:
        return float(v["N"])
    if "BOOL" in v:
        return v["BOOL"]
    raise ValueError(v)


class FakeDdb:
    """UpdateItem with SET/REMOVE, conditions made of attribute_not_exists / = / < / <= joined by AND or OR,
    ReturnValues=UPDATED_OLD, and Scan. Enough to check the Lambda logic, not a DynamoDB emulator."""
    def __init__(self):
        self.items, self.fail_next = {}, 0

    def _cond(self, item, expr, vals):
        def one(c):
            c = c.strip()
            m = re.fullmatch(r"attribute_not_exists\((\w+)\)", c)
            if m:
                return m.group(1) not in item
            m = re.fullmatch(r"(\w+)\s*(<=|<|=)\s*(:\w+)", c)
            a, op, v = m.groups()
            if a not in item:
                return False
            x, y = item[a], _val(vals[v])
            return x <= y if op == "<=" else x < y if op == "<" else x == y
        if " OR " in expr:
            return any(one(c) for c in expr.split(" OR "))
        return all(one(c) for c in expr.split(" AND "))

    def update_item(self, TableName, Key, UpdateExpression, ConditionExpression=None,
                    ExpressionAttributeValues=None, ReturnValues=None, ExpressionAttributeNames=None):
        if self.fail_next:
            self.fail_next -= 1
            raise ClientError("ProvisionedThroughputExceededException")
        vid, vals = Key["vehicle_id"]["S"], ExpressionAttributeValues or {}
        item = self.items.get(vid, {})
        if ConditionExpression and not self._cond(item, ConditionExpression, vals):
            raise ClientError("ConditionalCheckFailedException")
        old, touched = dict(item), []
        m = re.match(r"(?:SET (?P<set>.*?))?\s*(?:REMOVE (?P<rem>.*))?$", UpdateExpression.strip())
        for part in filter(None, (m.group("set") or "").split(",")):
            k, v = [x.strip() for x in part.split("=")]
            item[k] = _val(vals[v]); touched.append(k)
        for k in filter(None, (x.strip() for x in (m.group("rem") or "").split(","))):
            item.pop(k, None); touched.append(k)
        self.items[vid] = item
        # UPDATED_OLD: the old value of every attribute the update touched, if it existed before
        return {"Attributes": {k: old[k] for k in touched if k in old}} if ReturnValues == "UPDATED_OLD" else {}

    def scan(self, TableName, ProjectionExpression=None, ExclusiveStartKey=None):
        rows = []
        for vid, it in sorted(self.items.items()):
            row = {"vehicle_id": {"S": vid}}
            if "ingest_ts" in it:
                row["ingest_ts"] = {"N": str(int(it["ingest_ts"]))}
            if "offline_since" in it:
                row["offline_since"] = {"N": str(int(it["offline_since"]))}
            rows.append(row)
        return {"Items": rows}


class FakeSns:
    def __init__(self):
        self.sent, self.fail = [], False

    def publish(self, **kw):
        if self.fail:
            raise RuntimeError("sns down")
        self.sent.append(kw)

    def subjects(self):
        return [m["Subject"] for m in self.sent]


def load(fn_name, env):
    ddb, sns = FakeDdb(), FakeSns()
    fake_boto3 = types.SimpleNamespace(client=lambda name, **k: ddb if name == "dynamodb" else sns)
    exc = types.ModuleType("botocore.exceptions"); exc.ClientError = ClientError
    saved = {k: sys.modules.get(k) for k in ("boto3", "botocore", "botocore.exceptions")}
    sys.modules.update({"boto3": fake_boto3, "botocore": types.ModuleType("botocore"), "botocore.exceptions": exc})
    os.environ.update(env)
    try:
        code = load_template()["Resources"][fn_name]["Properties"]["Code"]["ZipFile"]
        ns = {}
        exec(compile(code, fn_name, "exec"), ns)
    finally:
        for k, v in saved.items():
            if v is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = v
    return types.SimpleNamespace(ns=ns, handler=ns["handler"], ddb=ddb, sns=sns)


@pytest.fixture
def lam():
    return load("StateFunction", {"TABLE": "vehicle-latest-state", "TOPIC": "arn:alert", "LOW_PSI": "28",
                                  "COOLDOWN": "900", "REARM_MARGIN_PSI": "1.0", "MAX_FUTURE_SKEW_S": "300"})


def iso(sec):
    s = T0 // 1000 + sec
    from datetime import datetime, timezone
    return datetime.fromtimestamp(s, timezone.utc).isoformat(timespec="seconds")


def reading(sec, vid="CAR-001", psi=33.0, **kw):
    m = {"vehicle_id": vid, "topic_vehicle_id": vid, "event_time": iso(sec), "tyre_fl_psi": psi,
         "tyre_fr_psi": 33.0, "tyre_rl_psi": 33.0, "tyre_rr_psi": 33.0, "speed_kmph": 40.0,
         "ingest_ts": T0 + sec * 1000 + 500}
    m.update(kw)
    return m


def event(*msgs):
    recs = []
    for i, m in enumerate(msgs):
        data = m if isinstance(m, bytes) else json.dumps(m).encode()
        recs.append({"kinesis": {"data": base64.b64encode(data).decode(), "sequenceNumber": str(100 + i)}})
    return {"Records": recs}


def run(lam, *msgs, now=None):
    if now is not None:
        lam.ns["time"] = types.SimpleNamespace(time=lambda: now)
    return lam.handler(event(*msgs), None)


NOW = T0 / 1000 + 60   # Lambda wall clock: one minute after T0


# ---------------------------------------------------------------- latest state
def test_newest_reading_is_stored(lam):
    assert run(lam, reading(0, speed_kmph=10.0), reading(2, speed_kmph=20.0), now=NOW) == {"batchItemFailures": []}
    item = lam.ddb.items["CAR-001"]
    assert item["event_time"] == iso(2) and item["speed_kmph"] == 20.0 and item["event_ms"] == T0 + 2000


def test_late_or_replayed_reading_never_overwrites_newer_state(lam):
    run(lam, reading(5, speed_kmph=50.0), now=NOW)
    run(lam, reading(1, speed_kmph=5.0), now=NOW)            # buffered reading arrives late
    assert lam.ddb.items["CAR-001"]["speed_kmph"] == 50.0


def test_times_in_different_formats_compare_correctly(lam):
    # "Z" with milliseconds is newer than "+00:00" one second earlier; a string compare would get this wrong
    run(lam, reading(1, event_time=iso(1)), now=NOW)
    run(lam, reading(1, event_time=iso(2).replace("+00:00", ".000Z"), speed_kmph=77.0), now=NOW)
    assert lam.ddb.items["CAR-001"]["speed_kmph"] == 77.0
    run(lam, reading(1, event_time="2026-10-14T15:30:01+05:30", speed_kmph=1.0), now=NOW)   # = 10:00:01 UTC: older
    assert lam.ddb.items["CAR-001"]["speed_kmph"] == 77.0


def test_device_clock_in_the_future_cannot_freeze_the_state(lam):
    run(lam, reading(0, event_time="2030-01-01T00:00:00+00:00", speed_kmph=99.0), now=NOW)
    run(lam, reading(3, speed_kmph=30.0), now=NOW)
    assert lam.ddb.items["CAR-001"]["speed_kmph"] == 30.0


def test_bad_or_zoneless_time_is_not_stored(lam):
    run(lam, reading(0, event_time="2026-10-14 10:00"), reading(0, event_time="2026-10-14T10:00:00"), now=NOW)
    assert "CAR-001" not in lam.ddb.items


def test_vehicle_id_comes_from_topic_not_payload(lam):
    run(lam, reading(0, vid="CAR-002") | {"vehicle_id": "CAR-999"}, now=NOW)
    assert "CAR-002" in lam.ddb.items and "CAR-999" not in lam.ddb.items


def test_out_of_range_and_text_values_are_not_stored(lam):
    run(lam, reading(0, tyre_rr_psi=-5, speed_kmph=900, tyre_fr_psi="abc"), now=NOW)
    item = lam.ddb.items["CAR-001"]
    assert "tyre_rr_psi" not in item and "speed_kmph" not in item and "tyre_fr_psi" not in item


def test_value_missing_in_the_newest_reading_is_not_kept_from_an_older_one(lam):
    run(lam, reading(0, fuel_level_pct=55.0), now=NOW)
    run(lam, reading(2), now=NOW)                               # no fuel_level_pct in this reading
    assert "fuel_level_pct" not in lam.ddb.items["CAR-001"]


def test_bad_records_are_skipped_not_retried(lam):
    res = run(lam, b"not json", {"no": "ids"}, b"[1,2]", reading(0), now=NOW)
    assert res == {"batchItemFailures": []} and "CAR-001" in lam.ddb.items


def test_failed_write_is_reported_for_retry_only_for_that_record(lam):
    lam.ddb.fail_next = 1
    res = run(lam, reading(0, vid="CAR-001"), reading(0, vid="CAR-002"), now=NOW)
    assert res == {"batchItemFailures": [{"itemIdentifier": "100"}]}
    assert "CAR-002" in lam.ddb.items


def test_same_reading_twice_is_harmless(lam):
    run(lam, reading(0, psi=20.0), reading(0, psi=20.0), now=NOW)        # QoS 1 duplicate
    assert len(lam.sns.sent) == 1


# ---------------------------------------------------------------- alerts
def test_low_tyre_alert_once_per_cooldown_then_a_reminder(lam):
    run(lam, *[reading(s, psi=25.0) for s in range(0, 20, 2)], now=NOW)
    assert lam.sns.subjects() == ["Low tyre pressure: CAR-001"]          # v4.1 sent 10 here
    body = json.loads(lam.sns.sent[0]["Message"])
    assert body["low_tyres_psi"] == {"tyre_fl_psi": 25.0} and body["first_alert"] is True
    attrs = lam.sns.sent[0]["MessageAttributes"]
    assert attrs["vehicle_id"]["StringValue"] == "CAR-001" and attrs["alert_type"]["StringValue"] == "low_tyre"
    run(lam, reading(30, psi=24.0), now=NOW + 901)                       # still low after the cool-down
    assert lam.sns.subjects()[-1] == "Still low tyre pressure: CAR-001"


def test_recovery_needs_the_hysteresis_margin(lam):
    run(lam, reading(0, psi=27.0), now=NOW)
    run(lam, reading(2, psi=28.5), now=NOW)       # above 28 but below 28 + 1: no "back to normal" yet
    assert len(lam.sns.sent) == 1
    run(lam, reading(4, psi=29.2), now=NOW)       # above the re-arm level
    assert lam.sns.subjects()[-1] == "Tyre pressure back to normal: CAR-001"
    run(lam, reading(6, psi=27.5), now=NOW + 1)   # low again after recovery: new alert at once
    assert lam.sns.subjects()[-1] == "Low tyre pressure: CAR-001"


def test_no_recovery_message_without_an_active_alert(lam):
    run(lam, reading(0, psi=33.0), reading(2, psi=33.0), now=NOW)
    assert lam.sns.sent == []


def test_alerts_are_per_vehicle(lam):
    run(lam, reading(0, vid="CAR-001", psi=20.0), reading(0, vid="CAR-002", psi=20.0), now=NOW)
    assert len(lam.sns.sent) == 2


def test_no_alert_from_stale_or_impossible_readings(lam):
    run(lam, reading(5), now=NOW)
    run(lam, reading(1, psi=10.0), now=NOW)           # old, replayed reading
    run(lam, reading(7, psi=-3.0), now=NOW)           # sensor fault: -3 psi is not a real pressure
    assert lam.sns.sent == []


def test_alert_is_retried_if_sns_fails(lam):
    lam.sns.fail = True
    res = run(lam, reading(0, psi=20.0), now=NOW)
    assert res["batchItemFailures"] == [{"itemIdentifier": "100"}]
    assert "alert_until" not in lam.ddb.items["CAR-001"] and "alert_active" not in lam.ddb.items["CAR-001"]
    lam.sns.fail = False
    run(lam, reading(0, psi=20.0), now=NOW)            # Lambda's retry of the same record
    assert lam.sns.subjects() == ["Low tyre pressure: CAR-001"]


def test_metrics_line_is_printed(lam, capsys):
    run(lam, reading(0), reading(0, event_time="2030-01-01T00:00:00+00:00"), now=NOW)
    line = [l for l in capsys.readouterr().out.splitlines() if l.startswith('{"_aws"')][0]
    emf = json.loads(line)
    assert emf["Updated"] == 1 and emf["FutureSkew"] == 1 and emf["Stage"] == "state"


# ---------------------------------------------------------------- offline check
@pytest.fixture
def off():
    return load("OfflineFunction", {"TABLE": "vehicle-latest-state", "TOPIC": "arn:alert", "OFFLINE_AFTER_MIN": "10"})


def test_offline_vehicles_are_reported_once_in_one_email(off):
    now_s = T0 / 1000 + 3600
    off.ddb.items = {"CAR-001": {"ingest_ts": T0}, "CAR-002": {"ingest_ts": T0 + 1000},
                     "CAR-003": {"ingest_ts": now_s * 1000 - 60000}}            # CAR-003 sent 1 minute ago
    off.ns["time"] = types.SimpleNamespace(time=lambda: now_s)
    assert off.handler({}, None) == {"seen": 3, "offline": 2, "newly_offline": 2}
    assert len(off.sns.sent) == 1 and off.sns.sent[0]["Subject"] == "2 vehicle(s) went offline"
    assert off.handler({}, None)["newly_offline"] == 0 and len(off.sns.sent) == 1   # not reported again


def test_a_new_reading_clears_offline(lam):
    lam.ddb.items["CAR-001"] = {"event_ms": T0 - 999999, "ingest_ts": T0 - 999999, "offline_since": T0}
    run(lam, reading(0), now=NOW)
    assert "offline_since" not in lam.ddb.items["CAR-001"]
