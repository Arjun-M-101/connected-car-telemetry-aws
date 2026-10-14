"""Checks added in v4.1: identity in every IoT rule comes from the MQTT topic, monitoring and data protection
are wired as documented, and the device script prints a clear verdict for the spoof (security) test."""
import pathlib, sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))
import device_mqtt as d
from test_transform import load_template

T = load_template()
R = T["Resources"]


def rule_sql(name):
    return " ".join(R[name]["Properties"]["TopicRulePayload"]["Sql"].split())


def test_latest_state_rule_takes_vehicle_id_from_topic_not_payload():
    sql = rule_sql("LatestStateRule")
    assert "topic(2) AS vehicle_id" in sql
    assert "SELECT *" not in sql          # a payload field could otherwise override the key


def test_low_tyre_alert_takes_vehicle_id_from_topic():
    assert rule_sql("LowTyreRule").startswith("SELECT topic(2) AS vehicle_id")


def test_history_rule_passes_topic_id_to_the_validation_lambda():
    assert "topic(2) AS topic_vehicle_id" in rule_sql("TelemetryRule")


def test_latest_state_rule_keeps_every_sensor_field():
    sql = rule_sql("LatestStateRule")
    for f in ("event_time", "tyre_fl_psi", "tyre_fr_psi", "tyre_rl_psi", "tyre_rr_psi", "accelerator_pct",
              "clutch_pct", "speed_kmph", "fuel_level_pct", "fuel_efficiency_kmpl", "ingest_ts"):
        assert f in sql, f


def test_all_alarms_notify_the_operations_topic():
    alarms = [r for r in R.values() if r["Type"] == "AWS::CloudWatch::Alarm"]
    assert len(alarms) == 6
    for a in alarms:
        assert a["Properties"]["AlarmActions"] == ["OpsTopic"]


def test_latest_state_table_has_point_in_time_recovery():
    p = R["LatestStateTable"]["Properties"]["PointInTimeRecoverySpecification"]
    assert p["PointInTimeRecoveryEnabled"] is True


def test_spoof_test_verdict_when_rejected():
    line = d.result_line(sent=0, buffered=3, interruptions=4, spoof_topic="CAR-102")
    assert "REJECTED" in line and "EXPECTED" in line and "CAR-102" in line


def test_spoof_test_verdict_warns_if_accepted():
    assert "WARNING" in d.result_line(sent=2, buffered=0, interruptions=0, spoof_topic="CAR-102")


def test_normal_run_verdicts():
    assert d.result_line(10, 0, 0) == "RESULT: all 10 messages sent."
    assert "still buffered" in d.result_line(7, 3, 1)
