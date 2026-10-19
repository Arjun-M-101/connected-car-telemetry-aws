"""Checks added in v4.1 and v5.0: identity in every IoT rule comes from the MQTT topic, monitoring and data protection
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


def test_single_rule_takes_vehicle_id_from_topic_and_partitions_by_it():
    sql = rule_sql("TelemetryRule")
    assert "topic(2) AS topic_vehicle_id" in sql
    act = R["TelemetryRule"]["Properties"]["TopicRulePayload"]["Actions"]
    assert len(act) == 1 and act[0]["Kinesis"]["PartitionKey"] == "${topic(2)}"


def test_only_one_iot_rule_v5():
    # v5.0: one rule writes to the Kinesis stream; latest state and alerts are consumers of the stream
    rules = [n for n, r in R.items() if r["Type"] == "AWS::IoT::TopicRule"]
    assert rules == ["TelemetryRule"]


def test_firehose_reads_the_stream_and_writes_parquet():
    p = R["DeliveryStream"]["Properties"]
    assert p["DeliveryStreamType"] == "KinesisStreamAsSource"
    d = p["ExtendedS3DestinationConfiguration"]
    assert d["DataFormatConversionConfiguration"]["Enabled"] is True
    assert "ParquetSerDe" in d["DataFormatConversionConfiguration"]["OutputFormatConfiguration"]["Serializer"]
    assert d["BufferingHints"]["SizeInMBs"] >= 64          # Parquet conversion requires >= 64 MB
    assert d["S3BackupConfiguration"]["CompressionFormat"] == "GZIP"
    sd = R["TelemetryTable"]["Properties"]["TableInput"]["StorageDescriptor"]
    assert "parquet" in sd["SerdeInfo"]["SerializationLibrary"].lower()


def test_state_consumer_has_retries_bisect_and_dead_letter_queue():
    p = R["StateEventSource"]["Properties"]
    assert p["BisectBatchOnFunctionError"] is True
    assert p["FunctionResponseTypes"] == ["ReportBatchItemFailures"]
    assert p["DestinationConfig"]["OnFailure"]["Destination"] == ["StateDLQ", "Arn"] or "StateDLQ" in str(p["DestinationConfig"])
    assert p["MaximumRetryAttempts"] >= 1


def test_template_can_be_deployed_without_an_s3_bucket():
    # aws cloudformation deploy sends the template body directly only up to 51,200 bytes
    size = (ROOT / "template.yaml").stat().st_size
    assert size < 51200, f"template.yaml is {size} bytes: move code out or deploy with --s3-bucket"


def test_every_reference_in_the_template_exists():
    import re
    text = (ROOT / "template.yaml").read_text()
    known = set(R) | set(T.get("Parameters", {})) | {"AWS::Region", "AWS::AccountId", "AWS::NoValue",
                                                      "AWS::StackName", "AWS::Partition", "AWS::URLSuffix"}
    refs = re.findall(r"!Ref\s+([A-Za-z0-9:]+)", text) + re.findall(r"!GetAtt\s+([A-Za-z0-9]+)\.", text)
    for sub in re.findall(r"\$\{([A-Za-z0-9:]+)(?:\.[A-Za-z]+)?\}", text):
        refs.append(sub)
    missing = sorted({r for r in refs if r not in known})
    assert not missing, missing


def test_lambda_log_groups_and_names_belong_to_the_stack():
    for fn in ("TransformFunction", "StateFunction", "OfflineFunction"):
        name = R[fn]["Properties"]["FunctionName"]
        assert name.startswith("vehicle-telemetry-")            # tear-down finds it by this prefix
        groups = [r["Properties"]["LogGroupName"] for r in R.values() if r["Type"] == "AWS::Logs::LogGroup"]
        assert "/aws/lambda/" + name in groups                 # retention set, deleted with the stack


def test_stream_is_encrypted_and_kept_24h():
    p = R["TelemetryStream"]["Properties"]
    assert p["StreamEncryption"]["EncryptionType"] == "KMS"
    assert p["RetentionPeriodHours"] >= 24


def test_all_alarms_notify_the_operations_topic():
    alarms = [r for r in R.values() if r["Type"] == "AWS::CloudWatch::Alarm"]
    assert len(alarms) == 12
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
