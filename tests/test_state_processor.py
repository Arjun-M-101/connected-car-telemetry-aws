"""Tests for the v5.0 latest-state / alert Lambda embedded in template.yaml (runs with fake DynamoDB and SNS)."""
import base64, json, os, sys, types
import pytest
from test_transform import load_template


class ClientError(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class FakeDdb:
    """Just enough of DynamoDB UpdateItem: SET / REMOVE and the two conditions the Lambda uses."""
    def __init__(self):
        self.items, self.fail_next = {}, False

    def update_item(self, TableName, Key, UpdateExpression, ConditionExpression=None,
                    ExpressionAttributeNames=None, ExpressionAttributeValues=None):
        if self.fail_next:
            self.fail_next = False
            raise ClientError("ProvisionedThroughputExceededException")
        vid = Key["vehicle_id"]["S"]
        item = self.items.setdefault(vid, {})
        vals = ExpressionAttributeValues or {}
        if ConditionExpression and "#t" in ConditionExpression:
            if "event_time" in item and not item["event_time"] <= vals[":t"]["S"]:
                raise ClientError("ConditionalCheckFailedException")
        if ConditionExpression and "alert_until" in ConditionExpression:
            if "alert_until" in item and not item["alert_until"] < float(vals[":n"]["N"]):
                raise ClientError("ConditionalCheckFailedException")
        if UpdateExpression.startswith("REMOVE"):
            item.pop(UpdateExpression.split()[1], None)
            return
        for part in UpdateExpression[4:].split(", "):
            k, v = [x.strip() for x in part.split("=")]
            k = (ExpressionAttributeNames or {}).get(k, k)
            val = vals[v]
            item[k] = val["S"] if "S" in val else float(val["N"])


class FakeSns:
    def __init__(self):
        self.sent, self.fail = [], False

    def publish(self, **kw):
        if self.fail:
            raise RuntimeError("sns down")
        self.sent.append(kw)


@pytest.fixture
def lam():
    ddb, sns = FakeDdb(), FakeSns()
    fake_boto3 = types.SimpleNamespace(client=lambda name, **k: ddb if name == "dynamodb" else sns)
    exc = types.ModuleType("botocore.exceptions"); exc.ClientError = ClientError
    saved = {k: sys.modules.get(k) for k in ("boto3", "botocore", "botocore.exceptions")}
    sys.modules.update({"boto3": fake_boto3, "botocore": types.ModuleType("botocore"), "botocore.exceptions": exc})
    os.environ.update(TABLE="vehicle-latest-state", TOPIC="arn:alert", LOW_PSI="28", COOLDOWN="900")
    try:
        code = load_template()["Resources"]["StateFunction"]["Properties"]["Code"]["ZipFile"]
        ns = {}
        exec(compile(code, "state_lambda", "exec"), ns)
    finally:
        for k, v in saved.items():
            if v is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = v
    return types.SimpleNamespace(handler=ns["handler"], ddb=ddb, sns=sns)


def reading(t, vid="CAR-001", psi=33.0, **kw):
    m = {"vehicle_id": vid, "topic_vehicle_id": vid, "event_time": t, "tyre_fl_psi": psi, "tyre_fr_psi": 33.0,
         "tyre_rl_psi": 33.0, "tyre_rr_psi": 33.0, "speed_kmph": 40.0, "ingest_ts": 1791000000000}
    m.update(kw)
    return m


def event(*msgs):
    recs = []
    for i, m in enumerate(msgs):
        data = m if isinstance(m, bytes) else json.dumps(m).encode()
        recs.append({"kinesis": {"data": base64.b64encode(data).decode(), "sequenceNumber": str(100 + i)}})
    return {"Records": recs}


def test_newest_reading_is_stored(lam):
    assert lam.handler(event(reading("2026-10-19T10:00:00+00:00", speed_kmph=10.0),
                             reading("2026-10-19T10:00:02+00:00", speed_kmph=20.0)), None) == {"batchItemFailures": []}
    item = lam.ddb.items["CAR-001"]
    assert item["event_time"] == "2026-10-19T10:00:02+00:00" and item["speed_kmph"] == 20.0


def test_late_or_replayed_reading_never_overwrites_newer_state(lam):
    lam.handler(event(reading("2026-10-19T10:00:05+00:00", speed_kmph=50.0)), None)
    lam.handler(event(reading("2026-10-19T10:00:01+00:00", speed_kmph=5.0)), None)   # buffered replay arrives late
    assert lam.ddb.items["CAR-001"]["speed_kmph"] == 50.0


def test_vehicle_id_comes_from_topic_not_payload(lam):
    lam.handler(event(reading("2026-10-19T10:00:00+00:00", vid="CAR-002") | {"vehicle_id": "CAR-999"}), None)
    assert "CAR-002" in lam.ddb.items and "CAR-999" not in lam.ddb.items


def test_low_tyre_alert_is_sent_once_per_cooldown(lam):
    msgs = [reading(f"2026-10-19T10:00:{s:02d}+00:00", psi=25.0) for s in range(0, 20, 2)]
    lam.handler(event(*msgs), None)
    assert len(lam.sns.sent) == 1                       # v4 sent one e-mail per message (10 here)
    body = json.loads(lam.sns.sent[0]["Message"])
    assert body["vehicle_id"] == "CAR-001" and body["low_tyres_psi"] == {"tyre_fl_psi": 25.0}


def test_alerts_are_per_vehicle(lam):
    lam.handler(event(reading("2026-10-19T10:00:00+00:00", vid="CAR-001", psi=20.0),
                      reading("2026-10-19T10:00:00+00:00", vid="CAR-002", psi=20.0)), None)
    assert len(lam.sns.sent) == 2


def test_no_alert_for_normal_pressure_or_stale_reading(lam):
    lam.handler(event(reading("2026-10-19T10:00:05+00:00")), None)
    lam.handler(event(reading("2026-10-19T10:00:01+00:00", psi=10.0)), None)   # old, replayed reading
    assert lam.sns.sent == []


def test_bad_records_are_skipped_not_retried(lam):
    res = lam.handler(event(b"not json", {"no": "ids"}, reading("2026-10-19T10:00:00+00:00", tyre_fr_psi="abc")), None)
    assert res == {"batchItemFailures": []}
    assert "tyre_fr_psi" not in lam.ddb.items["CAR-001"]          # non-numeric value is not stored


def test_failed_write_is_reported_for_retry_only_for_that_record(lam):
    lam.ddb.fail_next = True
    res = lam.handler(event(reading("2026-10-19T10:00:00+00:00", vid="CAR-001"),
                            reading("2026-10-19T10:00:00+00:00", vid="CAR-002")), None)
    assert res == {"batchItemFailures": [{"itemIdentifier": "100"}]}
    assert "CAR-002" in lam.ddb.items


def test_alert_is_retried_if_sns_fails(lam):
    lam.sns.fail = True
    res = lam.handler(event(reading("2026-10-19T10:00:00+00:00", psi=20.0)), None)
    assert res["batchItemFailures"] == [{"itemIdentifier": "100"}]
    assert "alert_until" not in lam.ddb.items["CAR-001"]          # cool-down released ...
    lam.sns.fail = False
    lam.handler(event(reading("2026-10-19T10:00:00+00:00", psi=20.0)), None)   # ... so Lambda's retry alerts
    assert len(lam.sns.sent) == 1


def test_metrics_line_is_printed(lam, capsys):
    lam.handler(event(reading("2026-10-19T10:00:00+00:00")), None)
    line = [l for l in capsys.readouterr().out.splitlines() if l.startswith('{"_aws"')][0]
    emf = json.loads(line)
    assert emf["Updated"] == 1 and emf["Stage"] == "state"
