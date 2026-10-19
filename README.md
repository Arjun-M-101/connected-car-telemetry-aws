# Connected-Car Telemetry on AWS

Sensor data from connected cars flows into AWS, gets validated, and lands in queryable tables. The project adds low-tyre alerts, a real-time "latest state" view per car, operations monitoring, per-device security and a small AI chatbot that answers questions about the fleet.

> **Status (v5.0, production upgrade):** the pipeline now runs on a production-style architecture: one IoT rule writes to an encrypted **Kinesis Data Stream**; Firehose reads the stream and lands **Parquet**; a **state Lambda** reads the same stream in batches and keeps the latest state per car (newest **valid** reading wins: late, duplicate, future-clock and impossible readings cannot corrupt it) and sends alerts with a cool-down, reminders and **hysteresis**; a scheduled check reports **offline** vehicles; failures retry, then land in a **dead-letter queue**; 12 alarms watch it. `load_test.py` measures throughput and latency, `resilience_test.py` proves the real-world cases on the live stack. See [docs/PERFORMANCE_SCALABILITY.md](docs/PERFORMANCE_SCALABILITY.md) for the current-vs-production analysis.
>
> Earlier status (v4.1): proof of concept with production-minded foundations. The sensors are **simulated** (`simulator.py`), and a real-device-style publisher (`device_mqtt.py`) uses X.509 certificates over MQTT. Everything on the AWS side is real and is created from one CloudFormation template. All data is synthetic.

### Architecture

![Architecture diagram](screenshots/AWS_Architecture_Diagram-Default.png)

*The architecture diagram was generated with AI (ChatGPT) from a written description of the deployed design, then checked by the author against `template.yaml` and corrected where the picture was wrong. `template.yaml` is the source of truth if it differs. The design follows published patterns; see [section 12](#12-design-basis-history-and-references).*

## Contents
1. [What it covers](#1-what-it-covers)
2. [How it works](#2-how-it-works)
3. [Repository layout](#3-repository-layout)
4. [Message and table schema](#4-message-and-table-schema)
5. [Design decisions](#5-design-decisions)
6. [Security](#6-security)
7. [Observability](#7-observability)
8. [Tests and CI](#8-tests-and-ci)
9. [Playbook: deploy, test, verify, tear down](#9-playbook-deploy-test-verify-tear-down)
10. [Troubleshooting](#10-troubleshooting)
11. [Limitations and roadmap](#11-limitations-and-roadmap)
12. [Design basis, history and references](#12-design-basis-history-and-references)
13. [Screenshots](#13-screenshots)

---

## 1. What it covers

| Requirement | How it is covered |
|---|---|
| Sensor -> AWS service -> table | device -> AWS IoT Core -> Amazon Kinesis Data Streams -> Amazon Data Firehose (+ validation Lambda, Parquet) -> Amazon S3 -> AWS Glue table -> Amazon Athena |
| Tyre pressure | 4 tyres in psi; e-mail alert below 28 psi (parameter `LowTyrePsi`) |
| Accelerator and clutch | pedal positions in percent; harsh-acceleration and clutch-usage reports |
| Fuel efficiency | km/l and fuel level; per-car averages |
| Report to driver / support team | SNS alert e-mail, DynamoDB latest state per car, SQL reports, chatbot |
| AI chatbot | `chatbot.py`: 7 fixed read-only Athena queries; an LLM (any OpenAI-compatible API such as Google Gemini, or Amazon Bedrock, or OpenAI) picks the query and phrases the answer; keyword fallback without an LLM |
| Different or bad data from real devices | validation Lambda: typed and range-checked fields, unknown fields kept in `extras`, problems flagged in `dq_issues`, original message kept in `raw/` |
| Operations | CloudWatch dashboard, 11 alarms (12 with the optional no-data alarm) sent to a separate operations SNS topic, custom metrics from the state Lambda, log groups with 30-day retention, dead-letter queue |
| Security | per-device IoT policy, identity taken from the MQTT topic, TLS-only encrypted private bucket, DynamoDB point-in-time recovery, no secrets in code |

## 2. How it works

1. **Device** publishes one JSON reading every 2 seconds to `vehicles/<vehicle_id>/telemetry` (MQTT, QoS 1). `simulator.py` uses AWS credentials; `device_mqtt.py` uses a per-vehicle X.509 certificate like real telematics hardware.
2. **AWS IoT Core** checks the device policy (a car may publish only to its own topic) and runs **one** topic rule, `vehicle_telemetry_ingest`. It adds `topic_vehicle_id` (from `topic(2)`, never from the payload) and `ingest_ts`, and writes the reading to the **Kinesis Data Stream** `vehicle-telemetry-stream`, using the vehicle id as the partition key (all readings of one car stay in order).
3. **Kinesis Data Stream** keeps every reading for 24 hours (replay), absorbs bursts, and feeds two independent consumers. Mode `PROVISIONED` with 1 shard (1,000 records/s) for the demo, `ON_DEMAND` for production (parameter `StreamMode`).
4. **History path:** **Amazon Data Firehose** reads the stream; the **validation Lambda** checks and normalises each record; Firehose converts it to **Parquet** (Snappy) and writes `telemetry/dt=YYYY-MM-DD/hr=HH/`. Untouched originals go GZIP-compressed to `raw/dt=YYYY-MM-DD/`, records that cannot be processed to `errors/`. Buffer: 64 MB or `BufferIntervalSeconds` (60 s demo, 300 to 900 s production).
5. **AWS Glue Data Catalog** defines the Parquet table `telemetry` with partition projection (no crawler), and **Amazon Athena** queries it in the workgroup `vehicle-telemetry` (1 GB scan limit per query).
6. **Real-time path:** the **state Lambda** `vehicle-telemetry-state` reads the stream in batches of up to 100. It updates one item per car in **DynamoDB** `vehicle-latest-state` only if the reading is newer than the stored one (a late or replayed reading never overwrites newer data). Times are parsed to epoch milliseconds (any ISO-8601 form with a zone); a reading more than 5 minutes ahead of AWS time, or with impossible values, cannot change the state. If a tyre is below 28 psi it sends **one** e-mail at once, a reminder every `AlertCooldownSeconds` (15 minutes) while still low, and one "back to normal" e-mail when every tyre is at least 29 psi (hysteresis: values around 28 do not flap).
7. **Offline detection:** every 5 minutes `vehicle-telemetry-offline-check` reports vehicles with no reading for `OfflineAfterMinutes` (10) in one summary e-mail; each vehicle once, until it sends again.
8. **Failures:** a failing record is retried (batch split in half, up to 3 retries); what still fails is recorded in the SQS **dead-letter queue** `vehicle-telemetry-state-dlq` and raises an alarm.
9. **CloudWatch** shows a dashboard (stream, lag, Firehose, Lambdas, alerts, offline vehicles) and sends 11 alarms to a separate operations SNS topic.
10. **Chatbot** maps a question to one of 7 fixed queries, runs it on Athena and, if an LLM is configured, asks the LLM to phrase the small result.

The history path (S3/Athena) is **near-real-time** (about 2 minutes with the demo buffer). The latest-state path (DynamoDB) is **real-time** (about 1 to 2 seconds; measure it with `load_test.py`).

## 3. Repository layout

```
.
├── README.md                     this file: design, playbook, troubleshooting
├── template.yaml                 CloudFormation: every AWS resource (45), incl. the code of the 3 Lambdas
├── simulator.py                  simulated cars (AWS credentials over HTTPS)
├── device_mqtt.py                real-device-style publisher (X.509 certificate, MQTT, store-and-forward outbox)
├── send_test_events.py           7 schema-drift / bad-data test events for vehicle CAR-900
├── chatbot.py                    question -> 1 of 7 fixed read-only queries -> optional LLM sentence
├── load_test.py                  N vehicles x 1 msg/s through IoT Core; throughput and end-to-end latency (v5.0)
├── resilience_test.py            late, duplicate, future-clock, impossible-value and hysteresis checks on the live stack (v5.0)
├── sql/queries.sql               report queries for the Athena console
├── scripts/
│   ├── provision_device.sh       create an IoT thing + certificate and attach the device policy (safe to re-run)
│   ├── deprovision_device.sh     remove one device and EVERY certificate attached to it (asks AWS)
│   └── teardown.sh               delete the whole project, fix what blocks the delete, prove nothing is left
├── tests/                        109 unit tests (no AWS account needed)
├── screenshots/                  architecture diagram and redacted evidence from real runs (section 13)
├── docs/
│   ├── PRODUCTION_READINESS.md   gap analysis and roadmap from proof of concept to production
│   └── PERFORMANCE_SCALABILITY.md  current (v4.1) vs production (v5.0) architecture: limits, numbers, improvements
├── requirements.txt
└── .github/workflows/ci.yml      runs the tests, cfn-lint and a shell syntax check on every push
```

## 4. Message and table schema

A reading from a device:
```json
{"vehicle_id": "CAR-001", "event_time": "2026-10-14T13:52:00+00:00",
 "tyre_fl_psi": 33.1, "tyre_fr_psi": 33.0, "tyre_rl_psi": 32.9, "tyre_rr_psi": 33.2,
 "accelerator_pct": 20.0, "clutch_pct": 0.0, "speed_kmph": 50.0,
 "fuel_level_pct": 70.0, "fuel_efficiency_kmpl": 15.0}
```

Table `vehicle_telemetry.telemetry` (JSON in S3):

| Column | Type | Notes |
|---|---|---|
| vehicle_id | string | always the id from the MQTT topic |
| event_time | string | ISO time from the device |
| tyre_fl_psi, tyre_fr_psi, tyre_rl_psi, tyre_rr_psi | double | range 0 to 100 |
| accelerator_pct, clutch_pct | double | range 0 to 100 |
| speed_kmph | double | range 0 to 400 |
| fuel_level_pct, fuel_efficiency_kmpl | double | range 0 to 100 |
| ingest_ts | bigint | server time in ms, added by IoT Core |
| schema_version | string | `1` unless the device sends another |
| extras | string | JSON with any fields the pipeline does not know yet |
| dq_issues | string | comma-separated problems found, empty when clean |
| dt, hr | partition | arrival date and hour (UTC) |

## 5. Design decisions

**Real devices send imperfect data.** The validation Lambda inside Firehose handles each case:

| Case | Result |
|---|---|
| Valid reading | typed and stored; original also kept in `raw/` |
| New field (for example `battery_v`) | kept in `extras`, queryable at once: `json_extract_scalar(extras,'$.battery_v')` |
| Missing field | stored as null |
| Number sent as text (`"31.5"`) | converted to a number |
| Non-numeric or out-of-range value | set to null and flagged in `dq_issues` (`not_numeric:<field>`, `out_of_range:<field>`) |
| Payload id differs from the topic id | the topic id wins; flagged `vehicle_id_mismatch` |
| Unparseable message or no vehicle id at all | original written to `errors/processing-failed/` |

A brand-new sensor does **not** become a column automatically. Automatic schema detection (for example a Glue crawler) lets one faulty device create wrong columns or change a type and break reports. A person promotes a field from `extras` to a column by updating the Lambda field list, the Glue columns and the tests.

**Identity comes from the topic.** The device policy binds a certificate to its own topic, so the topic is the trusted identity. The IoT rule and both Lambdas use the topic id; the payload id is only compared and flagged.

**CloudFormation** needs nothing installed in AWS CloudShell. The resources map one to one to Terraform if a team standard requires it.

**Kinesis Data Streams in front of everything (v5.0).** Up to v4.1 three IoT rules wrote directly to Firehose, SNS and DynamoDB: no replay, no ordering, one alert e-mail per message and a late reading could overwrite newer state. A stream gives one durable, ordered, replayable buffer and lets each consumer scale and fail on its own. Amazon MSK would be the choice for a Kafka-based organisation; it costs more to run.

**Parquet instead of JSON (v5.0).** Athena reads only the columns a query needs from compressed Parquet, so a typical fleet query scans several times less data than with JSON text (cost and speed both follow the bytes scanned).

**Newest valid reading wins (v5.0).** QoS 1 and the device outbox can deliver a reading twice or late, and device clocks drift. The state Lambda converts `event_time` to epoch milliseconds and writes with the condition `event_ms <= new event_ms`, so replays are harmless (idempotent) and never move the state backwards. A reading more than 5 minutes ahead of AWS time is rejected (it would otherwise block every later update), impossible values are never stored, and a field missing from the newest reading is removed so an old value never looks current. History keeps every message; reports can use `telemetry_dedup` in `sql/queries.sql`.

**Why Kinesis Data Streams and not MSK:** serverless, a direct IoT rule action, per-shard scaling and no VPC or 24x7 cluster. MSK fits an organisation that already runs Kafka. Details: docs/PERFORMANCE_SCALABILITY.md, section 4.

**Template size:** `aws cloudformation deploy` sends the template directly only up to 51,200 bytes; a test keeps `template.yaml` below that.

**Chatbot safety.** The LLM never writes SQL. It returns JSON naming one of 7 intents (`latest_status`, `lowest_tyre`, `fuel_efficiency`, `harsh_acceleration`, `clutch_usage`, `data_quality`, `vehicle_summary`) and optionally a vehicle id that must match `^CAR-\d{3}$`. Every query is a read-only `SELECT` limited to the last 2 days of partitions. If the LLM fails, keyword routing takes over and the table is printed.

## 6. Security

| Layer | In place | Not done yet |
|---|---|---|
| Device identity | one certificate and thing per vehicle; `vehicle-device-policy` allows connect as its own thing name, publish only to its own telemetry topic, subscribe/receive only its own commands topic | fleet provisioning, certificate rotation, AWS IoT Device Defender |
| Service access | least-privilege IAM roles for Firehose, the IoT rule and both Lambdas, scoped to this stack's resources | IAM Access Analyzer review |
| Data | bucket blocks public access, AES-256 encryption, versioning, policy denies non-TLS requests; lifecycle expiry and IA tiering; Kinesis stream KMS-encrypted; SQS queue encrypted; DynamoDB encryption and point-in-time recovery | customer-managed KMS keys |
| Detection | IoT rule error log, alarms | CloudTrail, GuardDuty, Security Hub |
| Secrets | none in code; `certs/`, `*.pem`, `*.key`, `.env` ignored by git; LLM key only in an environment variable | AWS Secrets Manager |
| Privacy | synthetic data only | consent, minimisation and retention for real location and driving data |

The free tier of an external LLM API may use prompts to improve the provider's products. Use it only with synthetic data.

## 7. Observability

* **Dashboard** `vehicle-telemetry`: messages from vehicles and rule success/failure, Kinesis records in and throttles, latest-state lag (iterator age), Firehose records and freshness, state Lambda counters (updated, stale, alerts sent, alerts suppressed, failed), Lambda invocations and errors, alert e-mails, alarm status.
* **Alarms** (to the operations SNS topic; in this demo it e-mails the same `AlertEmail` address as the driver/support alert, and in production it would go to the operations team):

| Alarm | Fires when |
|---|---|
| vehicle-telemetry-iot-rule-failures | the IoT rule cannot write to the stream |
| vehicle-telemetry-stream-throttled | the stream throttled writes (add shards or use ON_DEMAND) |
| vehicle-telemetry-firehose-delivery | Firehose cannot deliver to S3 |
| vehicle-telemetry-data-freshness | oldest record in Firehose is older than 20 minutes |
| vehicle-telemetry-transform-errors | the validation Lambda crashes |
| vehicle-telemetry-state-errors | the state Lambda fails (it retries) |
| vehicle-telemetry-state-lag | latest state is more than 60 s behind the stream for 3 minutes |
| vehicle-telemetry-state-dlq | a reading failed every retry and is in the dead-letter queue |
| vehicle-telemetry-parquet-conversion-failed | Firehose could not convert records to Parquet |
| vehicle-telemetry-offline-check-errors | the offline-detection Lambda failed |
| vehicle-telemetry-alert-delivery-failed | SNS cannot deliver a driver/support alert |
| vehicle-telemetry-no-data (optional) | no MQTT messages for 10 minutes; off by default (`EnableNoDataAlarm=true` to turn on) |

* **Logs** (30 days): `/aws/lambda/vehicle-telemetry-transform`, `/aws/lambda/vehicle-telemetry-state`, `/vehicle-telemetry/firehose`, `/vehicle-telemetry/iot-rule-errors`.

## 8. Tests and CI

```bash
pip install -r requirements.txt
python3 -m pytest -q          # 109 passed, no AWS account needed
cfn-lint template.yaml
```

| File | What it checks |
|---|---|
| tests/test_simulator.py | message fields, physical ranges, leak scenario |
| tests/test_chatbot.py | read-only queries, partition filter, SQL parses (sqlglot), injection blocked, routing incl. `data_quality`, LLM client retries and fallback, key never in errors |
| tests/test_transform.py | the validation Lambda taken from the template, rule error actions, Glue columns, bucket and device policy |
| tests/test_device_and_events.py | device outbox and the CAR-900 test events |
| tests/test_rules_and_device.py | one rule, id from the topic and used as partition key; Firehose reads the stream and writes Parquet; retries, bisect and DLQ; template under 51,200 bytes; every !Ref/!GetAtt/${} exists; Lambda names and log groups; stream encryption; 12 alarms to operations; PITR; spoof-test verdict |
| tests/test_state_processor.py | the state and offline Lambdas taken from the template, with a fake DynamoDB that evaluates the update and condition expressions: newest reading wins, late replays and duplicates, time formats, future clocks, bad times, impossible values, dropped sensors, cool-down and reminder, hysteresis and recovery, per-vehicle alerts, only the failed record retried, alert retried if SNS fails, offline reported once, metrics |
| tests/test_load_test.py | percentile and summary helpers of the load test |

`.github/workflows/ci.yml` runs the tests and `cfn-lint` on every push and pull request.

---

## 9. Playbook: deploy, test, verify, tear down

Run everything in **AWS CloudShell**, region **Asia Pacific (Mumbai) ap-south-1**. Allow about 2.5 hours for the full run. It costs a few rupees if you tear down the same day.
Every step lists **Expected** results and what to do **If not**. Take a screenshot wherever you see 📷. If a result differs from "Expected", record what you actually saw.

### 0. Set up every new CloudShell session
Upload the project zip with **Actions -> Upload file**, then:
```bash
cd ~ && unzip -q -o <project zip> && cd vehicle-telemetry
export AWS_REGION=ap-south-1 AWS_DEFAULT_REGION=ap-south-1
pip install -q -r requirements.txt awsiotsdk        # no --user flag: CloudShell uses a virtual environment
python3 -m pytest -q
```
**Expected:** `109 passed`. **If not:** fix the failing test before going on. If pip is slow because of cfn-lint, run `pip install -q pytest sqlglot boto3 pyyaml awsiotsdk` instead.

### 1. Deploy (new stack or update in place)
```bash
aws cloudformation deploy --template-file template.yaml --stack-name vehicle-telemetry \
  --capabilities CAPABILITY_IAM --parameter-overrides AlertEmail=you@example.com --region ap-south-1
```
Type your real e-mail address. A literal `<...>` placeholder breaks the shell.
**Expected:** `Successfully created/updated stack - vehicle-telemetry` after 3 to 6 minutes. An update that fails rolls back on its own.
**If not:**
```bash
aws cloudformation describe-stack-events --stack-name vehicle-telemetry --region ap-south-1 \
  --query "StackEvents[?contains(ResourceStatus,'FAILED')].[LogicalResourceId,ResourceStatusReason]" --output table
```
Confirm **both** subscription e-mails ("AWS Notification - Subscription Confirmation", check Spam): one for driver/support alerts and one for operations alarms.

**Set the bucket variable. Repeat this in every new shell or tab; later commands fail with "expected one argument" or "Invalid bucket name" when it is empty:**
```bash
BUCKET=$(aws cloudformation describe-stacks --stack-name vehicle-telemetry --region ap-south-1 \
  --query "Stacks[0].Outputs[?OutputKey=='Bucket'].OutputValue" --output text); echo "BUCKET=$BUCKET"
```
**Expected:** a name like `vehicle-telemetry-databucket-...`, not an empty value.

📷 Console checks:
* IoT Core -> Message routing -> Rules: 1 rule `vehicle_telemetry_ingest`, Active, action Kinesis.
* Kinesis -> Data streams -> `vehicle-telemetry-stream`: Active, encryption on.
* Lambda -> Functions -> `vehicle-telemetry-transform` and `vehicle-telemetry-state` (Configuration -> Triggers: Kinesis, Enabled).
* Lambda -> `vehicle-telemetry-offline-check` (Configuration -> Triggers: EventBridge, rate 5 minutes).
* SQS -> Queues -> `vehicle-telemetry-state-dlq`: 0 messages.
* CloudWatch -> Dashboards -> `vehicle-telemetry`, and CloudWatch -> Alarms -> All alarms (11 alarms, or 12 with the no-data alarm on).
* DynamoDB -> Tables -> `vehicle-latest-state`.

### 2. Data flow: sensor -> AWS -> table
```bash
python3 simulator.py --vehicles 3 --count 30 --interval 2
```
Wait 3 minutes (the validation step and Firehose each buffer up to 60 seconds), then 📷:
```bash
echo "${BUCKET:?run the BUCKET= line from step 1 first}"
aws s3 ls s3://$BUCKET/telemetry/ --recursive | tail -3     # clean, validated data
aws s3 ls s3://$BUCKET/raw/ --recursive | tail -3           # untouched originals
```
**Expected:** Parquet files under `telemetry/dt=.../hr=.../` and `.gz` files under `raw/dt=.../`. If you see bucket names instead of keys, `$BUCKET` is empty.
In Athena (workgroup `vehicle-telemetry`, database `vehicle_telemetry`) 📷:
```sql
SELECT vehicle_id, count(*) AS readings, min(schema_version) AS schema_version FROM telemetry GROUP BY vehicle_id ORDER BY 1;
```
**Expected:** CAR-001 to CAR-003 with at least 30 readings each and schema_version `1`.
**If 0 rows:** wait 2 more minutes, check the workgroup, and look for rejected records with `aws s3 ls s3://$BUCKET/errors/ --recursive` (a `format-conversion-failed/` folder means a record did not match the table columns).

### 3. Real-time latest state
In tab 1, run `python3 simulator.py --vehicles 3 --count 0 --interval 2` (it runs until Ctrl+C). In tab 2 (CloudShell "+"), run this twice, 10 seconds apart 📷:
```bash
aws dynamodb scan --table-name vehicle-latest-state --query "Items[].[vehicle_id.S,event_time.S,speed_kmph.N]" --output table
```
**Expected:** rows whose `event_time` moves forward. Stop tab 1 with Ctrl+C.

### 4. Low-tyre alert to driver / support
```bash
python3 simulator.py --vehicles 3 --count 40 --interval 2 --leak
```
CAR-001's front-left tyre crosses 28 psi after roughly 15 to 28 batches. Let it run to the end: since v5.0 you get **one** e-mail (then a reminder only if it is still low 15 minutes later), not one per reading.
**Expected:** e-mail with subject `Low tyre pressure: CAR-001` and a body like `{"vehicle_id": "CAR-001", ..., "low_tyres_psi": {"tyre_fl_psi": 27.8}, "threshold_psi": 28.0}`. 📷
Proof of de-duplication: CloudWatch -> Dashboards -> `vehicle-telemetry` -> widget "State Lambda": `AlertsSent` 1, `AlertsSuppressed` > 0.
About 10 minutes after the simulators stop you also get **one** e-mail `N vehicle(s) went offline` (offline detection); that is expected.
```sql
SELECT event_time, tyre_fl_psi FROM telemetry WHERE vehicle_id='CAR-001' ORDER BY event_time DESC LIMIT 30;
```
This proves the chain sensor -> IoT rule -> stream -> state Lambda -> SNS -> e-mail with de-duplication. Delivery to a phone app and escalation are not built.

### 5. Schema drift and data validation
```bash
python3 send_test_events.py      # add --alert to also send one low-tyre event (this e-mails you)
```
It sends 7 events for CAR-900: valid, new fields, numbers as strings, a non-numeric value, out-of-range values, a payload claiming CAR-777, and a payload with no vehicle id. **Wait at least 2 minutes** before querying, then 📷:
```sql
SELECT event_time, tyre_fl_psi, tyre_rr_psi, fuel_level_pct, dq_issues, extras FROM telemetry WHERE vehicle_id='CAR-900' ORDER BY ingest_ts;
```
**Expected (7 rows per run; running the script twice gives 14 rows with the same pattern):**

| Event | Expected result |
|---|---|
| valid | all numbers present; `dq_issues` and `extras` empty |
| new fields | `extras` holds `battery_v`, `fw_version`, `gps` |
| numbers as strings | `tyre_fl_psi` = 31.5 |
| non-numeric ("flat") | `tyre_fl_psi` empty; `dq_issues` = `not_numeric:tyre_fl_psi` |
| out of range | `tyre_rr_psi` and `fuel_level_pct` empty; `dq_issues` lists `out_of_range:...` |
| id differs from topic | stored as CAR-900; `dq_issues` = `vehicle_id_mismatch` |
| no id in payload | stored as CAR-900 |

**If 0 rows:** the Firehose batch has not landed yet. Wait 2 minutes and run again; the "Data scanned" figure grows when new files arrive.
```sql
SELECT count(*) FROM telemetry WHERE vehicle_id='CAR-777';    -- Expected 0: a claimed id is never stored
SELECT vehicle_id, json_extract_scalar(extras,'$.battery_v') AS battery_v FROM telemetry WHERE extras IS NOT NULL;   -- new field, no table change
```
Latest state and rule errors must also be clean 📷:
```bash
aws dynamodb scan --table-name vehicle-latest-state --query "Items[].vehicle_id.S" --output text   # includes CAR-900, never CAR-777
aws logs tail /vehicle-telemetry/iot-rule-errors --since 15m                                       # Expected: no output
```
If CAR-777 is listed, it is a stale item from a stack older than v4.1. Remove it with `aws dynamodb delete-item --table-name vehicle-latest-state --key '{"vehicle_id":{"S":"CAR-777"}}'` and run step 5 again.
The originals are still in `raw/` 📷:
```bash
echo "${BUCKET:?run the BUCKET= line from step 1 first}"
for K in $(aws s3 ls s3://$BUCKET/raw/ --recursive | tail -5 | awk '{print $4}'); do aws s3 cp s3://$BUCKET/$K - ; done | grep CAR-900 | grep -E '"flat"|-5' | head -3
```
**Expected:** the original `"flat"` text and `-5`, which were set to null in the clean table.

### 6. Observability
1. 📷 **Dashboard** while a simulator runs: CloudWatch -> Dashboards -> `vehicle-telemetry`. Metrics lag 1 to 5 minutes.
2. **Alarm e-mail path** (instant test; it proves the e-mail path, not a real fault):
   ```bash
   aws cloudwatch set-alarm-state --alarm-name vehicle-telemetry-transform-errors --state-value ALARM --state-reason "playbook test"
   ```
   **Expected:** within about a minute, an e-mail "ALARM: vehicle-telemetry-transform-errors" 📷. The alarm returns to OK at its next evaluation.
3. **Fleet went silent (real condition, optional):** deploy with `EnableNoDataAlarm=true`, stop all simulators and wait about 15 minutes:
   ```bash
   aws cloudformation deploy --template-file template.yaml --stack-name vehicle-telemetry \
     --capabilities CAPABILITY_IAM --parameter-overrides AlertEmail=you@example.com EnableNoDataAlarm=true --region ap-south-1
   ```
   **Expected:** "ALARM: vehicle-telemetry-no-data" 📷, and "OK" after the simulator runs again. Re-deploy with `EnableNoDataAlarm=false` afterwards, otherwise an idle system keeps e-mailing.
4. **Logs:**
   ```bash
   aws logs tail /aws/lambda/vehicle-telemetry-transform --since 15m | tail -20   # START / END / REPORT lines
   aws logs tail /vehicle-telemetry/firehose --since 1h | tail                     # normally empty
   aws logs tail /vehicle-telemetry/iot-rule-errors --since 1h | tail              # normally empty
   ```

### 7. Security checks and device certificates
```bash
echo "${BUCKET:?run the BUCKET= line from step 1 first}"
aws s3api get-public-access-block --bucket $BUCKET          # all four true
aws s3api get-bucket-encryption --bucket $BUCKET             # AES256
aws s3api get-bucket-policy --bucket $BUCKET --query Policy --output text | python3 -m json.tool   # DenyNonTLS
aws dynamodb describe-continuous-backups --table-name vehicle-latest-state \
  --query "ContinuousBackupsDescription.PointInTimeRecoveryDescription.PointInTimeRecoveryStatus"   # "ENABLED"
aws iot get-policy --policy-name vehicle-device-policy --query policyDocument --output text | python3 -m json.tool
```
📷 each output. Cover the 12-digit account ID before sharing a screenshot.

**Positive device test** (X.509 certificate over MQTT):
```bash
bash scripts/provision_device.sh CAR-101
python3 device_mqtt.py --thing CAR-101 --count 10 --interval 2
```
If it prints `CAR-101 already exists in AWS with a certificate whose private key is not in certs/CAR-101/`, a certificate from an earlier session is still attached: run `bash scripts/deprovision_device.sh CAR-101`, then provision again. If it prints `already provisioned`, nothing changes and you can go on.
**Expected:** `connected as CAR-101 ...`, `seq 1 sent` ... and `RESULT: all 10 messages sent.` After 2 minutes 📷:
```sql
SELECT vehicle_id, event_time, json_extract_scalar(extras,'$.seq') AS seq, json_extract_scalar(extras,'$.fw_version') AS fw FROM telemetry WHERE vehicle_id='CAR-101' ORDER BY ingest_ts;
```
**Negative test** (a stolen or faulty device must not act as another vehicle):
```bash
python3 device_mqtt.py --thing CAR-101 --count 3 --interval 2 --spoof-topic CAR-102
```
**Expected:** `connection interrupted ... UNEXPECTED_HANGUP` lines (AWS IoT closes the connection), `BUFFERED` lines, and finally `RESULT: publishing to vehicles/CAR-102/telemetry was REJECTED ... EXPECTED outcome` 📷. Then:
```sql
SELECT count(*) FROM telemetry WHERE vehicle_id='CAR-102' AND json_extract_scalar(extras,'$.seq') IS NOT NULL;   -- Expected 0
```
Secrets check before any `git push`: `git grep -n -E "AKIA[0-9A-Z]{16}|AIza[0-9A-Za-z_-]{35}|BEGIN [A-Z ]*PRIVATE KEY-----"` must print nothing, and `git status` must not list anything under `certs/`.

### 8. Chatbot
Keyword mode (no AI) 📷:
```bash
python3 chatbot.py --no-llm --ask "Which car has the lowest tyre pressure?"
python3 chatbot.py --no-llm --ask "Average fuel efficiency of each car"
python3 chatbot.py --no-llm --ask "How is CAR-001 doing?"
python3 chatbot.py --no-llm --ask "Any data quality problems?"
```
With an LLM: create a free Google AI Studio key with a personal account. Enter it without echoing it, and never save it in a file, a screenshot or a chat:
```bash
read -rs -p "Paste API key, then Enter: " LLM_API_KEY; echo; export LLM_API_KEY
python3 chatbot.py --list-llm-models          # pick a name containing flash-lite (fast, highest free limits)
export LLM_MODEL="<model name from the list>"
python3 chatbot.py --ask "Which car has the lowest tyre pressure and is it a problem?"
```
**Expected:** `[mode: OpenAI-compatible API ...]`, `[intent=lowest_tyre ...]`, then one sentence naming the car below 28 psi 📷.
Other providers: set `LLM_BASE_URL` for another OpenAI-compatible API, `OPENAI_API_KEY` and `OPENAI_MODEL` for OpenAI, or `BEDROCK_MODEL_ID` for Amazon Bedrock.
**Safety test:** `python3 chatbot.py --no-llm --ask "CAR-001'; DROP TABLE telemetry --"` must answer normally. **Wrong-key test:** `export LLM_API_KEY=wrong`; the bot prints a warning and still answers with the table.

### 8b. Load test and performance numbers (v5.0)
```bash
python3 load_test.py --vehicles 100 --seconds 60
```
Sends 6,000 readings (100 cars `LT-0001`... at 1 reading per second) and then measures how long the newest reading of 20 cars takes to reach DynamoDB.
**Expected (about 2 minutes):** `messages sent : 6000 of 6000 ... -> about 100 msg/s`, `publish errors : 0`, an `end-to-end (publish -> DynamoDB)` line with p50 and p95 in milliseconds (typically 1 to 3 seconds), and `VERDICT : PASS`. 📷
Then 📷 CloudWatch -> Dashboards -> `vehicle-telemetry`: the Kinesis widget shows about 6,000 records/minute with **0 throttled**, and the lag widget stays near 0. After 3 minutes in Athena: `SELECT count(*) FROM telemetry WHERE vehicle_id LIKE 'LT-%';` -> 6000.
The 100 `LT-` cars are reported offline about 10 minutes later in one e-mail (expected).
Larger runs: `--vehicles 500 --seconds 60` (about 500 msg/s, still under 1 shard's 1,000 records/s). Above that, redeploy with `StreamMode=ON_DEMAND` or `ShardCount=2` first, otherwise the `stream-throttled` alarm fires (this is the expected, visible limit).

### 8c. Resilience test: real-world data problems (v5.0)
```bash
python3 resilience_test.py --athena
```
Sends a late reading, a duplicate, a reading from a clock 2 hours ahead, an impossible -5 psi value, a `Z`-format time with milliseconds, then a low-tyre episode with a value inside the hysteresis band (cars `RT-001`, `RT-002`).
**Expected (about 5 minutes):** 11 `[PASS]` lines and `RESULT: ALL CHECKS PASSED`; two e-mails for RT-002 (`Low tyre pressure` and `Tyre pressure back to normal`). 📷
**If a check fails:** run it again once (the history check waits for Firehose); if it still fails, send the output.

### 9. Evidence checklist
1 rule, Kinesis stream, 3 Lambdas, DLQ, resilience-test result, dashboard, alarm list, S3 `telemetry/` and `raw/`, row counts, DynamoDB changing, alert e-mail, 7-row drift result, CAR-777 count 0, `extras` query, raw original, operations alarm e-mails, S3 security outputs, PITR and device policy, device test positive and negative, chatbot keyword and LLM, `93 passed`, load-test result and dashboard, teardown verification.

### 10. Tear down and verify nothing is left
One script does the whole tear-down. It needs nothing from earlier sessions: no `$BUCKET`, no `certs/` folder. If CloudShell times out half-way, open it again and run the same two lines; every step first checks what still exists.
```bash
cd ~/connected-car-telemetry-aws && export AWS_REGION=ap-south-1 AWS_DEFAULT_REGION=ap-south-1
bash scripts/teardown.sh
```
Type `DELETE` when it asks. It then, in order:
1. stops any `simulator.py` or `device_mqtt.py` still running in CloudShell;
2. switches off the three IoT rules, so no new data arrives while it cleans up;
3. asks AWS for **every** certificate attached to `vehicle-device-policy` or to a `CAR-NNN` thing (also old ones from earlier sessions), detaches, deactivates and deletes them, then deletes the things;
4. waits 75 seconds so Firehose writes its last buffer **before** the bucket is emptied;
5. finds the data bucket itself and empties it (objects, old versions, unfinished uploads);
6. deletes the stack and prints its status every 15 seconds; on `DELETE_FAILED` it prints the reason, fixes it (devices, device policy, bucket) and tries again, up to 3 times;
7. removes anything that can outlive the stack and the local `certs/` keys, then checks 15 resource types one by one.

**Expected (5 to 10 minutes):** `[OK] stack vehicle-telemetry deleted`, fifteen `[OK] ...: none` lines and `ALL CLEAN: nothing from vehicle-telemetry is left in ap-south-1.` 📷
**If not:** a `[LEFT]` line names what is still there. Run `bash scripts/teardown.sh` again after a few minutes (AWS can need up to 5 minutes after a certificate is detached before the policy can be deleted). To only check, without deleting anything: `bash scripts/teardown.sh --verify`.

Then, outside AWS: delete the LLM API key in Google AI Studio, run `unset LLM_API_KEY LLM_MODEL`, and check Billing the next day. DynamoDB keeps a free system backup of the deleted table for 35 days; it costs nothing and expires by itself. To prove repeatability, deploy again from step 1, run steps 2 and 4, and tear down again.

**Why the old four-command tear-down got stuck** (seen on 19 Oct 2026):
| What happened | Cause | Now |
|---|---|---|
| `DeleteThing ... Cannot delete. Thing CAR-101 is still attached to one or more principals` | `provision_device.sh` ran twice for CAR-101 (14 and 19 Oct). The second run overwrote `certs/CAR-101/cert_arn.txt`, so `deprovision_device.sh` removed only the new certificate; the old one stayed attached to the thing **and** to `vehicle-device-policy` | `teardown.sh` and `deprovision_device.sh` ask AWS which certificates are attached; `provision_device.sh` refuses to add a second certificate |
| `Invalid bucket name ""` on `aws s3 rm` | a new CloudShell session, so `$BUCKET` was empty and nothing was emptied | `teardown.sh` finds the bucket from the stack (or by name) |
| `wait stack-delete-complete` printed nothing and was stopped with Ctrl+C; the check command printed nothing | the stack could not delete the non-empty bucket or the still-attached policy; the check filter did not include `DELETE_IN_PROGRESS`, so a running or stuck delete looked finished | status printed every 15 s, `DELETE_FAILED` explained and fixed, 15 resource types checked |
| `aws s3 ls` still showed the bucket after the stack was "gone" | same cause: the stack was in `DELETE_IN_PROGRESS`/`DELETE_FAILED`, not deleted | the final check shows `[LEFT] CloudFormation stack ... DELETE_FAILED` instead of an empty line |

## 10. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `argument --bucket: expected one argument`, `Invalid bucket name ""`, or `aws s3 ls` lists buckets | `$BUCKET` is empty in this shell | run the `BUCKET=` line from step 1 |
| First deploy rolls back with `DeliveryStream` or `StateEventSource` "not authorized" / "cannot assume role" | a brand-new IAM role was not visible to the service yet (IAM is eventually consistent) | `bash scripts/teardown.sh`, then deploy again |
| Athena 0 rows and `errors/format-conversion-failed/` exists; alarm `parquet-conversion-failed` | a record did not match the table columns | read one file in that folder; fix the validation Lambda or the Glue column, deploy again |
| `vehicle-telemetry-state-lag` alarm | the stream has more data than the state Lambda processes | check `state-errors`; raise `ShardCount` or use `StreamMode=ON_DEMAND` |
| `vehicle-telemetry-state-dlq` alarm | a reading failed every retry | SQS -> the queue -> Send and receive messages -> Poll: the message names the shard and sequence numbers; fix the cause; the stream keeps the data 24 h |
| `N vehicle(s) went offline` e-mail after a demo | the simulators stopped; offline detection works | nothing; set `OfflineAfterMinutes` higher if you want fewer |
| Stack rolls back | a resource failed | `describe-stack-events` command in step 1 |
| `bash: your: No such file or directory` on deploy | a `<placeholder>` was typed literally | use the real value |
| `pip install --user` fails | CloudShell runs in a virtual environment | drop `--user` |
| Athena 0 rows | Firehose buffer not delivered yet, or wrong workgroup | wait 2 to 3 minutes; workgroup `vehicle-telemetry`; look in `errors/` |
| Files in `errors/processing-failed/` | record not JSON or without any vehicle id | open one file and read the original record |
| `iot-rule-errors` shows `Missing the key vehicle_id` | stack older than v4.1 | deploy this template (rules take the id from the topic) |
| No alert e-mail | subscription not confirmed | confirm both e-mails, check Spam |
| Too many alert e-mails | more than one car is leaking, or reminders every 15 minutes while a tyre stays low | expected; raise `AlertCooldownSeconds` for fewer reminders |
| Device cannot connect | wrong thing name, policy not attached, wrong endpoint | re-run `provision_device.sh`; use the same name in `--thing` |
| `provision_device.sh`: `already exists in AWS with a certificate whose private key is not in certs/` | an older certificate from an earlier session is still attached | `bash scripts/deprovision_device.sh CAR-101`, then provision again |
| `DeleteThing ... still attached to one or more principals` | the thing has a second (older) certificate | `bash scripts/deprovision_device.sh CAR-101` (it asks AWS for every certificate) or `bash scripts/teardown.sh` |
| Stack in `DELETE_FAILED` (`vehicle-device-policy` or `DataBucket`) | a certificate is still attached to the policy, or the bucket is not empty (often Firehose wrote its last buffer after `aws s3 rm`) | `bash scripts/teardown.sh`: it fixes both and deletes again |
| `wait stack-delete-complete` prints nothing for a long time | the delete is stuck or failed | Ctrl+C, then `bash scripts/teardown.sh` (shows the status and the reason) |
| Tear-down check prints nothing but the bucket still exists | the stack is still `DELETE_IN_PROGRESS` or `DELETE_FAILED` | `bash scripts/teardown.sh --verify` |
| `export` vs `set` | `set VAR=...` is Windows syntax | use `export VAR=...` in CloudShell |
| LLM `HTTP 400 API key not valid` / `403` / `404` / `429` | key pasted wrong / key or API blocked / wrong model name / free-tier limit | paste again / new key in a new project / copy the name from `--list-llm-models` / wait a minute |
| Bedrock `Error 002: Access to Bedrock models is not allowed for this account` | Bedrock blocked for the whole account | use the OpenAI-compatible option or keyword mode |
| Table instead of a sentence, no error line | `LLM_MODEL` not set in this shell | run both `export` lines again |

## 11. Limitations and roadmap

* Sensors are simulated; `device_mqtt.py` was tested from CloudShell, not on vehicle hardware. `load_test.py` tests hundreds of vehicles from one CloudShell, not millions.
* Alerts are e-mail only (de-duplicated per car since v5.0); no SMS, push or ticketing.
* No per-vehicle offline alarm, SLOs or tracing.
* CI only; no automatic deployment (CD) or separate dev/prod stacks.
* No user interface beyond the Athena console and the command-line chatbot.

`docs/PRODUCTION_READINESS.md` lists, area by area, what is built and what a production roll-out needs, the challenges of real hardware, and a phased plan.

## 12. Design basis, history and references

### Where the design comes from
The architecture combines patterns that AWS and the IoT community publish for device telemetry, sized for a low-cost proof of concept. Numbers in brackets refer to the reference list below.

| Decision | Choice in this project | Published pattern or reason |
|---|---|---|
| Device to cloud | MQTT over TLS into AWS IoT Core, one X.509 certificate per vehicle | lightweight publish/subscribe protocol for devices on unreliable networks; managed broker with device identity and rules [1][2][3] |
| Topic and identity | `vehicles/<id>/telemetry`, vehicle id read with `topic(2)` | put the thing name in every device topic; topic function and policy variables make the topic the trusted identity [4][5][6] |
| History (cold path) | IoT rule -> Firehose -> S3 -> Glue table -> Athena | published AWS IoT analytics pattern; the AWS Connected Vehicle Solution also landed raw vehicle data in S3 through Firehose [7][8][9] |
| Table | Glue table in the template with partition projection, no crawler | no crawler or partition maintenance; types cannot change by accident [10] |
| Latest state (hot path) | third IoT rule writes one item per car to DynamoDB | fast "latest" view next to the full history (batch and speed split); AWS connected-vehicle guidance keeps vehicle state in DynamoDB; Device Shadow considered [2][11][12] |
| Imperfect data | validation Lambda in Firehose, raw copy, `extras`, `dq_issues`, no automatic schema change | Firehose transformation; land raw data first and never edit it; reviewed schema evolution, schema registry later [13][14][15][16] |
| Sensor fields | tyre pressures, pedals, speed, fuel level, km/l | fuel level, accelerator and speed are standard OBD-II PIDs; tyre pressure comes from the TPMS; VSS is the standard signal catalogue [17][18][19] |
| Low-tyre alert | SNS e-mail below 28 psi (parameter `LowTyrePsi`) | TPMS regulations warn at 25% below the recommended pressure (US FMVSS 138; India AIS-154) [20][21][22] |
| AI chatbot | LLM picks one of 7 fixed read-only queries, never writes SQL | OWASP Top 10 for LLM applications: prompt injection and excessive agency [23] |
| Operations and security | dashboard, alarms, logs, least privilege, TLS-only encrypted bucket, PITR | Well-Architected IoT Lens and service security best practices [24][25] |
| Not used | AWS IoT FleetWise, Kinesis Data Streams, MSK, Flink | FleetWise is closed to new customers; AWS points to Connected Mobility guidance, which uses MSK and Flink for fleet scale [26][19] |

### How the project evolved
| Version | Trigger | What changed | Tests |
|---|---|---|---|
| v1 | the brief: sensor -> AWS service -> table, three sensor groups | IoT Core, Firehose, S3, Glue, Athena; low-tyre e-mail; simulator with a leak scenario | - |
| v2 | is a 60-second buffer "real time"? and a suggestion to try an AI chatbot | DynamoDB latest state; chatbot with fixed queries and keyword fallback; CI | 27 |
| v3 | Bedrock blocked on the account; no paid OpenAI balance | OpenAI-compatible LLM client with a free Gemini key | 39 |
| v4 | different data from real devices, real hardware, production grade, observability, security | validation Lambda and raw copy, dashboard and alarms, device policy and X.509 publisher, playbook | 70 |
| v4.1 | issues found in the live v4 run | every rule takes the vehicle id from the topic; clearer test output; verified on AWS | 79 |
| v4.1.1 | tear-down got stuck in a live run | `scripts/teardown.sh`: one re-runnable command that finds every certificate and the bucket itself and verifies the result | 79 |
| v5.0 | "make it production level" | Kinesis Data Stream in front; Parquet; state Lambda: newest valid reading wins, future clocks and impossible values rejected, alerts with cool-down, reminders and hysteresis; offline detection; retries, bisect and DLQ; 12 alarms; versioned bucket; load and resilience tests; performance and scalability analysis | 109 |

### References
All links were opened and checked on 15 October 2026.

1. OASIS, [MQTT Version 3.1.1](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html) (OASIS Standard, 2014)
2. AWS, [Guidance for Connected Vehicles on AWS](https://aws.amazon.com/solutions/guidance/connected-vehicles-on-aws/)
3. A. Al-Fuqaha et al., [Internet of Things: A Survey on Enabling Technologies, Protocols, and Applications](https://doi.org/10.1109/COMST.2015.2444095), IEEE Communications Surveys & Tutorials 17(4), 2015
4. AWS whitepaper, [Designing MQTT Topics for AWS IoT Core: MQTT design best practices](https://docs.aws.amazon.com/whitepapers/latest/designing-mqtt-topics-aws-iot-core/mqtt-design-best-practices.html)
5. AWS, [AWS IoT SQL functions (topic)](https://docs.aws.amazon.com/iot/latest/developerguide/iot-sql-functions.html)
6. AWS, [AWS IoT Core thing policy variables](https://docs.aws.amazon.com/iot/latest/developerguide/thing-policy-variables.html)
7. B. Snively, AWS Big Data Blog, [Derive Insights from IoT in Minutes using AWS IoT, Amazon Kinesis Firehose, Amazon Athena, and Amazon QuickSight](https://aws.amazon.com/blogs/big-data/derive-insights-from-iot-in-minutes-using-aws-iot-amazon-kinesis-firehose-amazon-athena-and-amazon-quicksight)
8. C. Rec, AWS IoT Blog, [Building Connected Vehicle Solutions on the AWS Cloud](https://aws.amazon.com/blogs/iot/building-connected-vehicle-solutions-on-the-aws-cloud/)
9. GitHub, [aws-solutions/aws-connected-vehicle-solution](https://github.com/aws-solutions/aws-connected-vehicle-solution) (archived reference implementation)
10. AWS, [Partition projection with Amazon Athena](https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html)
11. J. Kreps, [Questioning the Lambda Architecture](https://www.oreilly.com/ideas/questioning-the-lambda-architecture), O'Reilly, 2014
12. AWS, [AWS IoT Device Shadow service](https://docs.aws.amazon.com/iot/latest/developerguide/iot-device-shadows.html)
13. AWS, [Amazon Data Firehose: transform source data](https://docs.aws.amazon.com/firehose/latest/dev/data-transformation.html)
14. M. Armbrust et al., [Lakehouse: A New Generation of Open Platforms that Unify Data Warehousing and Advanced Analytics](https://www.cidrdb.org/cidr2021/papers/cidr2021_paper17.pdf), CIDR 2021
15. M. Kleppmann, *Designing Data-Intensive Applications*, O'Reilly, 2017 (chapter 4, schema evolution)
16. AWS, [AWS Glue Schema Registry](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html)
17. [OBD-II PIDs](https://en.wikipedia.org/wiki/OBD-II_PIDs) (SAE J1979 parameter list)
18. COVESA, [Vehicle Signal Specification](https://github.com/COVESA/vehicle_signal_specification)
19. AWS, [Guidance for Connected Mobility on AWS](https://docs.aws.amazon.com/guidance/latest/connected-mobility-on-aws/solution-overview.html)
20. US Federal Register, [FMVSS No. 138, Tire Pressure Monitoring Systems](https://www.federalregister.gov/documents/2005/04/08/05-6741/federal-motor-vehicle-safety-standards-tire-pressure-monitoring-systems-controls-and-displays) (70 FR 18136, 2005)
21. TUV Rheinland, [India AIS-154: TPMS type approval requirements](https://www.tuv.com/regulations-and-standards/en/india-ais-154-type-approval-requirements-for-vehicles-with-regard-to-their-tyre-pressure-monitoring-systems-tpms.html)
22. J. Guerrero-Ibanez, S. Zeadally, J. Contreras-Castillo, [Sensor Technologies for Intelligent Transportation Systems](https://www.mdpi.com/1424-8220/18/4/1212), Sensors 18(4), 2018
23. OWASP, [Top 10 for LLM Applications 2025](https://genai.owasp.org/llm-top-10/)
24. AWS, [Well-Architected IoT Lens](https://docs.aws.amazon.com/wellarchitected/latest/iot-lens/iot-lens.html)
25. AWS, [Security best practices for Amazon S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/security-best-practices.html) and [Security in AWS IoT Core](https://docs.aws.amazon.com/iot/latest/developerguide/security.html)
26. AWS, [AWS IoT FleetWise availability change](https://docs.aws.amazon.com/iot-fleetwise/latest/developerguide/iotfleetwise-availability-change.html)
27. AWS, [AWS IoT Device SDK v2 for Python](https://github.com/aws/aws-iot-device-sdk-python-v2) (used by `device_mqtt.py`)

The AWS service documentation used to build and run each part (IoT Core, Firehose, Glue, Athena, DynamoDB, SNS, Lambda, CloudWatch, CloudFormation) is listed in the project's implementation guide.

**AI use.** The code, template, tests and documentation were drafted with an AI assistant (Claude) and the architecture diagram with ChatGPT. I deployed and ran every version in my own AWS account, found and fixed the failures, and checked the design against the sources above.

## 13. Screenshots

Evidence from real runs in my own AWS account (region ap-south-1). All data is synthetic. Account ID, e-mail address, ARNs that contain the account ID, the IoT endpoint and source IP addresses are covered with black boxes. File names containing `v3` come from the first version and are kept as history; `v4` marks the v4 run and `v4.1` the v4.1 re-test that verified the fix. The step-by-step commands that produce each screenshot are in the [playbook](#9-playbook-deploy-test-verify-tear-down).

```
screenshots/
├── AWS_Architecture_Diagram-Default.png   architecture diagram (AI-generated, author-checked)
├── iam-roles-list.png                                         # IAM console: the three stack service roles (Firehose, IoT rule, Lambda)
├── s3-data-bucket-console.png                                 # S3 console: the single data bucket in ap-south-1
├── s3-telemetry-first-object-listing-v3.png                   # first telemetry object after the 5-batch smoke test (v3 run)
├── s3-telemetry-and-raw-listing.png                           # telemetry/ (clean) next to raw/ (untouched) plus the original "flat" and -5 values in raw/
├── s3-security-block-encryption-tls-policy.png                # public access block, AES256 encryption, DenyNonTLS bucket policy
├── iot-rules-list-two-rules-v3.png                            # IoT rules list after the first deployment, two rules (v3 run)
├── iot-rule-telemetry-to-firehose-v3.png                      # rule vehicle_telemetry_to_s3, Firehose action (v3 SQL)
├── iot-rule-low-tyre-alert-sns.png                            # rule vehicle_low_tyre_alert, SNS action (v3 SQL)
├── mqtt-test-client-subscribe.png                             # MQTT test client subscribed to vehicles/+/telemetry
├── mqtt-test-client-live-messages.png                         # live telemetry messages arriving in the MQTT test client
├── iot-rules-list-three-rules.png                             # IoT rules list, three active rules
├── iot-rule-latest-state-dynamodb-v3.png                      # rule vehicle_latest_state_to_dynamodb (v3 SQL)
├── iot-rule-latest-state-topic-vehicle-id-v4.1.png            # rule 3 SQL after v4.1: topic(2) AS vehicle_id
├── firehose-delivery-stream-active.png                        # Firehose delivery stream, Direct PUT to S3
├── firehose-stream-details-v3.png                             # Firehose stream details (v3 run, before error logging was enabled)
├── glue-database.png                                          # Glue database vehicle_telemetry
├── glue-table-overview.png                                    # Glue table telemetry, JSON, location in the data bucket
├── glue-table-schema-v3.png                                   # Glue table schema, 14 columns (v3 run)
├── athena-latest-20-rows.png                                  # Athena: latest 20 readings
├── athena-row-count-per-car-v3.png                            # Athena: 65 rows per car (v3 run)
├── athena-lowest-tyre-pressure-v3.png                         # Athena: lowest tyre pressure per car before the leak run
├── athena-fuel-efficiency.png                                 # Athena: average fuel efficiency and lowest fuel level per car
├── athena-clutch-usage.png                                    # Athena: share of readings with the clutch pressed
├── athena-harsh-acceleration-zero-rows.png                    # Athena: accelerator above 85 percent, 0 rows on the small data set
├── athena-leak-trace-car001.png                               # Athena: front-left tyre of CAR-001 falling during the leak run
├── athena-car001-latest-30-readings-v4.png                    # Athena: CAR-001 latest 30 readings through the validation Lambda
├── athena-row-count-and-schema-version-v4.png                 # Athena: 217 readings per car, schema_version 1
├── sns-low-tyre-alert-emails-v3.png                           # low-tyre alert e-mails from the leak run (v3 run)
├── sns-low-tyre-alert-emails-v4.png                           # low-tyre alert e-mails, front-left falling from 27.8 to 26.5 psi
├── sns-operations-subscription-confirmed.png                  # operations SNS topic subscription confirmed
├── ops-alarm-email-transform-errors-test.png                  # operations alarm e-mail (alarm state set by hand to test the path)
├── ops-alarm-email-no-data-alarm.png                          # operations e-mail: real no-data alarm in ALARM
├── ops-alarm-email-no-data-ok.png                             # operations e-mail: no-data alarm back to OK
├── dynamodb-latest-state-table-details.png                    # DynamoDB table vehicle-latest-state, key vehicle_id
├── dynamodb-items-first-scan.png                              # DynamoDB items, first scan, one per car
├── dynamodb-items-second-scan.png                             # DynamoDB items, second scan 16 seconds later (values changed)
├── dynamodb-cli-scans-v3.png                                  # DynamoDB CLI scans (v3 run)
├── dynamodb-cli-scans-v4.png                                  # DynamoDB CLI scans while the simulator ran (v4 run)
├── chatbot-keyword-mode.png                                   # chatbot keyword mode, tables straight from Athena
├── chatbot-gemini-model-list.png                              # silent key entry and the Gemini model list
├── chatbot-llm-mode-answers.png                               # chatbot LLM mode: intent plus a sentence
├── chatbot-llm-harsh-acceleration.png                         # chatbot LLM answer: harsh acceleration
├── chatbot-llm-clutch-usage.png                               # chatbot LLM answer: clutch usage
├── chatbot-interactive-latest-status.png                      # chatbot interactive mode: latest record for CAR-001
├── chatbot-keyword-clutch-usage.png                           # chatbot keyword mode: clutch usage
├── chatbot-v4-keyword-lowest-tyre.png                         # chatbot keyword mode: lowest tyre pressure per car
├── chatbot-v4-keyword-fuel-efficiency.png                     # chatbot keyword mode: fuel efficiency and fuel left per car
├── chatbot-v4-keyword-vehicle-summary.png                     # chatbot keyword mode: summary for CAR-001
├── chatbot-v4-data-quality-question.png                       # chatbot data_quality question: flagged readings
├── chatbot-v4-llm-lowest-tyre.png                             # chatbot LLM mode: sentence matches the table
├── cloudformation-deploy-v3.png                               # unzip, cloudformation deploy and reading the bucket name (v3 run)
├── simulator-smoke-test.png                                   # simulator smoke test, 5 batches of 3 messages
├── simulator-leak-run.png                                     # simulator leak scenario (--leak)
├── pytest-70-passed-and-stack-update-v4.png                   # 70 tests passed, then the v4 stack update (superseded by 79 tests)
├── cloudformation-deploy-no-data-alarm-on.png                 # stack update with EnableNoDataAlarm=true
├── pytest-79-passed-cfn-lint-clean.png                        # 79 tests passed and cfn-lint with no findings
├── cloudformation-deploy-v4.1.png                             # v4.1 deployed as an update of the same stack
├── cloudformation-stack-resources.png                         # stack resources (29 with the optional alarm off)
├── github-actions-ci-green.png                                # GitHub Actions: ci passes on the v4.1 commits
├── send-test-events-output.png                                # send_test_events.py: the seven CAR-900 test events
├── lambda-log-and-iot-rule-error-log-before-fix.png           # Lambda log and the IoT rule error log before the v4.1 fix
├── athena-claimed-id-car777-count-zero.png                    # Athena: the payload claiming CAR-777 was not stored under that ID
├── athena-extras-new-field-battery-v.png                      # Athena: new field battery_v kept in extras and readable at once
├── athena-car900-drift-result.png                             # Athena: the seven CAR-900 events, run twice (14 rows) with dq_issues and extras
├── dynamodb-scan-no-car777-rule-errors-empty.png              # v4.1 fix verified: no CAR-777 item, rule error log empty
├── lambda-functions-list.png                                  # Lambda functions list: vehicle-telemetry-transform, Python 3.12
├── lambda-transform-function-page.png                         # Lambda function page with the validation code
├── cloudwatch-alarms-overview.png                             # CloudWatch overview with the stack alarms
├── cloudwatch-dashboards-list.png                             # CloudWatch dashboards list
├── cloudwatch-dashboard-live-data.png                         # dashboard vehicle-telemetry with live data
├── cloudwatch-dashboard-alarm-detail.png                      # dashboard with the transform-errors alarm detail
├── dynamodb-pitr-and-device-policy.png                        # point-in-time recovery enabled and the per-device IoT policy
├── device-cert-positive-test-car101.png                       # device test: ten readings from CAR-101 with its certificate
├── device-cert-spoof-test-v4.png                              # spoof test, v4 script (ended with a traceback, fixed in v4.1)
├── athena-car102-count-zero-after-spoof.png                   # Athena: nothing accepted on CAR-102 after the spoof attempt
├── device-cert-spoof-test-verdict-v4.1.png                    # spoof test, v4.1 script: REJECTED, the expected outcome
├── cloudshell-errors-user-flag-and-email-placeholder.png      # early CloudShell errors (--user flag, placeholder e-mail)
└── bedrock-validation-exception.png                           # Bedrock ValidationException (why the LLM option is OpenAI-compatible)
```

### Deployment, tests and CI

The v4.1 package passes its 79 tests and cfn-lint, is deployed as an update of the same stack, and CI is green on the v4.1 commits.

![79 tests passed and cfn-lint with no findings](screenshots/pytest-79-passed-cfn-lint-clean.png)

![V4.1 deployed as an update of the same stack](screenshots/cloudformation-deploy-v4.1.png)

![Stack resources (29 with the optional alarm off)](screenshots/cloudformation-stack-resources.png)

![GitHub Actions: ci passes on the v4.1 commits](screenshots/github-actions-ci-green.png)

### Ingestion: IoT Core rules and Firehose

Three active topic rules on `vehicles/+/telemetry`; every rule takes the vehicle ID from the topic (`topic(2) AS vehicle_id`, v4.1).

![MQTT test client subscribed to vehicles/+/telemetry](screenshots/mqtt-test-client-subscribe.png)

![Live telemetry messages arriving in the MQTT test client](screenshots/mqtt-test-client-live-messages.png)

![IoT rules list, three active rules](screenshots/iot-rules-list-three-rules.png)

![Rule 3 SQL after v4.1: topic(2) AS vehicle_id](screenshots/iot-rule-latest-state-topic-vehicle-id-v4.1.png)

![Firehose delivery stream, Direct PUT to S3](screenshots/firehose-delivery-stream-active.png)

### Storage and query: S3, Glue and Athena

Clean data under `telemetry/`, untouched originals under `raw/`, and the Athena table.

![S3 console: the single data bucket in ap-south-1](screenshots/s3-data-bucket-console.png)

![Telemetry/ (clean) next to raw/ (untouched) plus the original "flat" and -5 values in raw/](screenshots/s3-telemetry-and-raw-listing.png)

![Glue database vehicle_telemetry](screenshots/glue-database.png)

![Glue table telemetry, JSON, location in the data bucket](screenshots/glue-table-overview.png)

![Athena: CAR-001 latest 30 readings through the validation Lambda](screenshots/athena-car001-latest-30-readings-v4.png)

![Athena: 217 readings per car, schema_version 1](screenshots/athena-row-count-and-schema-version-v4.png)

### Validation, schema drift and the v4.1 fix

The seven CAR-900 test events show new fields kept in `extras`, bad values flagged in `dq_issues`, and a claimed vehicle ID never stored. After the v4.1 fix the latest-state table has no CAR-777 item and the rule error log is empty.

![Send_test_events.py: the seven CAR-900 test events](screenshots/send-test-events-output.png)

![Athena: the payload claiming CAR-777 was not stored under that ID](screenshots/athena-claimed-id-car777-count-zero.png)

![Athena: new field battery_v kept in extras and readable at once](screenshots/athena-extras-new-field-battery-v.png)

![Athena: the seven CAR-900 events, run twice (14 rows) with dq_issues and extras](screenshots/athena-car900-drift-result.png)

![V4.1 fix verified: no CAR-777 item, rule error log empty](screenshots/dynamodb-scan-no-car777-rule-errors-empty.png)

![Lambda functions list: vehicle-telemetry-transform, Python 3.12](screenshots/lambda-functions-list.png)

![Lambda function page with the validation code](screenshots/lambda-transform-function-page.png)

### Alerts: driver/support and operations e-mail

Low-tyre alert e-mails, the second SNS topic for operations alarms, a real no-data alarm and its recovery.

![Low-tyre alert e-mails, front-left falling from 27.8 to 26.5 psi](screenshots/sns-low-tyre-alert-emails-v4.png)

![Operations SNS topic subscription confirmed](screenshots/sns-operations-subscription-confirmed.png)

![Operations alarm e-mail (alarm state set by hand to test the path)](screenshots/ops-alarm-email-transform-errors-test.png)

![Operations e-mail: real no-data alarm in ALARM](screenshots/ops-alarm-email-no-data-alarm.png)

![Operations e-mail: no-data alarm back to OK](screenshots/ops-alarm-email-no-data-ok.png)

### Real-time latest state in DynamoDB

One item per car, overwritten on every reading.

![DynamoDB items, first scan, one per car](screenshots/dynamodb-items-first-scan.png)

![DynamoDB items, second scan 16 seconds later (values changed)](screenshots/dynamodb-items-second-scan.png)

![DynamoDB CLI scans while the simulator ran (v4 run)](screenshots/dynamodb-cli-scans-v4.png)

### Observability: CloudWatch

Alarm overview and the dashboard with live data.

![CloudWatch overview with the stack alarms](screenshots/cloudwatch-alarms-overview.png)

![CloudWatch dashboards list](screenshots/cloudwatch-dashboards-list.png)

![Dashboard vehicle-telemetry with live data](screenshots/cloudwatch-dashboard-live-data.png)

![Dashboard with the transform-errors alarm detail](screenshots/cloudwatch-dashboard-alarm-detail.png)

### Security and device identity

Least-privilege roles, a private encrypted TLS-only bucket, point-in-time recovery, and the per-device policy with a positive test and a spoof test.

![IAM console: the three stack service roles (Firehose, IoT rule, Lambda)](screenshots/iam-roles-list.png)

![Public access block, AES256 encryption, DenyNonTLS bucket policy](screenshots/s3-security-block-encryption-tls-policy.png)

![Point-in-time recovery enabled and the per-device IoT policy](screenshots/dynamodb-pitr-and-device-policy.png)

![Device test: ten readings from CAR-101 with its certificate](screenshots/device-cert-positive-test-car101.png)

![Athena: nothing accepted on CAR-102 after the spoof attempt](screenshots/athena-car102-count-zero-after-spoof.png)

![Spoof test, v4.1 script: REJECTED, the expected outcome](screenshots/device-cert-spoof-test-verdict-v4.1.png)

### Chatbot

Keyword mode, the data-quality question, and LLM mode (sentence matches the table).

![Chatbot keyword mode: lowest tyre pressure per car](screenshots/chatbot-v4-keyword-lowest-tyre.png)

![Chatbot keyword mode: fuel efficiency and fuel left per car](screenshots/chatbot-v4-keyword-fuel-efficiency.png)

![Chatbot keyword mode: summary for CAR-001](screenshots/chatbot-v4-keyword-vehicle-summary.png)

![Chatbot data_quality question: flagged readings](screenshots/chatbot-v4-data-quality-question.png)

![Chatbot LLM mode: sentence matches the table](screenshots/chatbot-v4-llm-lowest-tyre.png)

