"""Tests for the validation/normalisation Lambda that is embedded in template.yaml (single source of truth)."""
import base64, json, pathlib
import pytest
import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]

def load_template():
    class L(yaml.SafeLoader):
        pass
    def any_tag(loader, suffix, node):
        if isinstance(node, yaml.ScalarNode):
            return loader.construct_scalar(node)
        if isinstance(node, yaml.SequenceNode):
            return loader.construct_sequence(node)
        return loader.construct_mapping(node)
    L.add_multi_constructor("!", any_tag)
    return yaml.load((ROOT / "template.yaml").read_text(), Loader=L)

@pytest.fixture(scope="module")
def mod():
    code = load_template()["Resources"]["TransformFunction"]["Properties"]["Code"]["ZipFile"]
    ns = {}
    exec(compile(code, "transform_lambda", "exec"), ns)
    return type("M", (), ns)

def good(**kw):
    m = {"vehicle_id": "CAR-001", "event_time": "2026-10-14T10:00:00+00:00",
         "tyre_fl_psi": 33.1, "tyre_fr_psi": 33.0, "tyre_rl_psi": 32.9, "tyre_rr_psi": 33.2,
         "accelerator_pct": 20.0, "clutch_pct": 0.0, "speed_kmph": 50.0,
         "fuel_level_pct": 70.0, "fuel_efficiency_kmpl": 15.0, "ingest_ts": 1791972001000}  # 1 s after event_time
    m.update(kw)
    return m

def run(mod, *payloads):
    ev = {"records": [{"recordId": str(i), "data": base64.b64encode(
        p if isinstance(p, bytes) else json.dumps(p).encode()).decode()} for i, p in enumerate(payloads)]}
    res = mod.handler(ev, None)["records"]
    out = []
    for r in res:
        row = json.loads(base64.b64decode(r["data"])) if r["result"] == "Ok" else None
        out.append((r["result"], row))
    return out

def test_valid_message_passes_unchanged(mod):
    (res, row), = run(mod, good())
    assert res == "Ok" and row["tyre_fl_psi"] == 33.1 and row["dq_issues"] is None
    assert row["extras"] is None and row["schema_version"] == "1"

def test_output_is_one_json_line(mod):
    ev = {"records": [{"recordId": "1", "data": base64.b64encode(json.dumps(good()).encode()).decode()}]}
    raw = base64.b64decode(mod.handler(ev, None)["records"][0]["data"]).decode()
    assert raw.endswith("\n") and raw.count("\n") == 1

def test_unknown_fields_are_kept_in_extras(mod):
    (_, row), = run(mod, good(battery_v=12.6, fw_version="2.1.0", gps={"lat": 13.08, "lon": 80.27}))
    extras = json.loads(row["extras"])
    assert extras == {"battery_v": 12.6, "fw_version": "2.1.0", "gps": {"lat": 13.08, "lon": 80.27}}
    assert "battery_v" not in row

def test_numeric_strings_are_coerced(mod):
    (_, row), = run(mod, good(tyre_fl_psi="31.5", speed_kmph="60"))
    assert row["tyre_fl_psi"] == 31.5 and row["speed_kmph"] == 60.0 and row["dq_issues"] is None

def test_non_numeric_is_nulled_and_flagged(mod):
    (res, row), = run(mod, good(tyre_fl_psi="flat"))
    assert res == "Ok" and row["tyre_fl_psi"] is None and "not_numeric:tyre_fl_psi" in row["dq_issues"]

def test_out_of_range_is_nulled_and_flagged(mod):
    (_, row), = run(mod, good(tyre_rr_psi=-5, fuel_level_pct=140))
    assert row["tyre_rr_psi"] is None and row["fuel_level_pct"] is None
    assert "out_of_range:tyre_rr_psi" in row["dq_issues"] and "out_of_range:fuel_level_pct" in row["dq_issues"]

@pytest.mark.parametrize("bad", ["nan", float("inf")])
def test_nan_and_inf_are_rejected(mod, bad):
    payload = good()
    raw = json.dumps(payload).replace("33.1", json.dumps(bad) if isinstance(bad, str) else "Infinity")
    (_, row), = run(mod, raw.encode())
    assert row["tyre_fl_psi"] is None and "tyre_fl_psi" in row["dq_issues"]

def test_booleans_are_not_numbers(mod):
    (_, row), = run(mod, good(clutch_pct=True))
    assert row["clutch_pct"] is None and "not_numeric:clutch_pct" in row["dq_issues"]

def test_missing_sensor_is_null_without_issue(mod):
    p = good(); del p["fuel_efficiency_kmpl"]
    (_, row), = run(mod, p)
    assert row["fuel_efficiency_kmpl"] is None and row["dq_issues"] is None

def test_missing_event_time_flagged(mod):
    p = good(); del p["event_time"]
    (res, row), = run(mod, p)
    assert res == "Ok" and "missing_event_time" in row["dq_issues"]

def test_missing_vehicle_id_goes_to_error_prefix(mod):
    p = good(); del p["vehicle_id"]
    assert run(mod, p) == [("ProcessingFailed", None)]

@pytest.mark.parametrize("bad", [b"not json at all", b"[1,2,3]", b"123", b'"str"', b"{broken"])
def test_garbage_payloads_go_to_error_prefix(mod, bad):
    assert run(mod, bad) == [("ProcessingFailed", None)]

def test_topic_vehicle_id_is_trusted_over_payload(mod):
    (_, row), = run(mod, good(vehicle_id="CAR-999", topic_vehicle_id="CAR-001"))
    assert row["vehicle_id"] == "CAR-001" and "vehicle_id_mismatch" in row["dq_issues"]
    assert "topic_vehicle_id" not in row and row["extras"] is None

def test_topic_id_used_when_payload_has_none(mod):
    p = good(topic_vehicle_id="CAR-007"); del p["vehicle_id"]
    (res, row), = run(mod, p)
    assert res == "Ok" and row["vehicle_id"] == "CAR-007"

def test_one_bad_record_does_not_break_the_batch(mod):
    out = run(mod, good(), b"garbage", good(vehicle_id="CAR-002"))
    assert [r for r, _ in out] == ["Ok", "ProcessingFailed", "Ok"]

def test_record_ids_are_preserved(mod):
    ev = {"records": [{"recordId": "abc", "data": base64.b64encode(json.dumps(good()).encode()).decode()}]}
    assert mod.handler(ev, None)["records"][0]["recordId"] == "abc"

# ---- template-level safety checks ----
def test_every_iot_rule_has_error_action():
    res = load_template()["Resources"]
    for name, r in res.items():
        if r["Type"] == "AWS::IoT::TopicRule":
            assert "ErrorAction" in r["Properties"]["TopicRulePayload"], name

def test_glue_table_has_new_columns():
    cols = [c["Name"] for c in load_template()["Resources"]["TelemetryTable"]["Properties"]["TableInput"]["StorageDescriptor"]["Columns"]]
    assert {"schema_version", "extras", "dq_issues"} <= set(cols)

def test_bucket_denies_non_tls_and_blocks_public():
    t = load_template()["Resources"]
    stmt = t["DataBucketPolicy"]["Properties"]["PolicyDocument"]["Statement"][0]
    assert stmt["Effect"] == "Deny" and stmt["Condition"]["Bool"]["aws:SecureTransport"] == "false"
    pab = t["DataBucket"]["Properties"]["PublicAccessBlockConfiguration"]
    assert all(pab.values())

def test_device_policy_is_per_thing_scoped():
    doc = load_template()["Resources"]["DevicePolicy"]["Properties"]["PolicyDocument"]
    for s in doc["Statement"]:
        assert "ThingName" in s["Resource"], s
        assert "*" not in s["Resource"].replace("commands/*", "")


def test_event_time_in_other_iso_formats_is_accepted(mod):
    for t in ("2026-10-14T10:00:00Z", "2026-10-14T10:00:00.250+00:00", "2026-10-14T15:30:00+05:30"):
        (res, row), = run(mod, good(event_time=t))
        assert res == "Ok" and row["dq_issues"] is None, t


def test_unparseable_or_zoneless_event_time_is_flagged(mod):
    for t in ("yesterday", "2026-10-14 10:00", "2026-10-14T10:00:00"):
        (res, row), = run(mod, good(event_time=t))
        assert res == "Ok" and row["dq_issues"] == "bad_event_time", t


def test_device_clock_far_ahead_is_flagged(mod):
    (res, row), = run(mod, good(event_time="2026-10-14T11:00:00+00:00"))   # 1 h ahead of ingest_ts
    assert row["dq_issues"] == "future_event_time"
