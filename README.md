# Connected-Car Telemetry on AWS

Sensor data from connected cars flows into AWS, gets validated, and lands in queryable tables. The project adds low-tyre alerts, a real-time "latest state" view per car, operations monitoring, per-device security and a small AI chatbot that answers questions about the fleet.

> **Status:** proof of concept with production-minded foundations. The sensors are **simulated** (`simulator.py`), and a real-device-style publisher (`device_mqtt.py`) uses X.509 certificates over MQTT. Everything on the AWS side is real and is created from one CloudFormation template. All data is synthetic.

![architecture](docs/architecture.png)

*Architecture diagram, generated from `docs/architecture.dot` with Graphviz (the script was written with AI assistance).*

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

---

## 1. What it covers

| Requirement | How it is covered |
|---|---|
| Sensor -> AWS service -> table | device -> AWS IoT Core -> Amazon Data Firehose (+ validation Lambda) -> Amazon S3 -> AWS Glue table -> Amazon Athena |
| Tyre pressure | 4 tyres in psi; e-mail alert below 28 psi (parameter `LowTyrePsi`) |
| Accelerator and clutch | pedal positions in percent; harsh-acceleration and clutch-usage reports |
| Fuel efficiency | km/l and fuel level; per-car averages |
| Report to driver / support team | SNS alert e-mail, DynamoDB latest state per car, SQL reports, chatbot |
| AI chatbot | `chatbot.py`: 7 fixed read-only Athena queries; an LLM (any OpenAI-compatible API such as Google Gemini, or Amazon Bedrock, or OpenAI) picks the query and phrases the answer; keyword fallback without an LLM |
| Different or bad data from real devices | validation Lambda: typed and range-checked fields, unknown fields kept in `extras`, problems flagged in `dq_issues`, original message kept in `raw/` |
| Operations | CloudWatch dashboard, 6 alarms to a separate operations e-mail, log groups with 30-day retention |
| Security | per-device IoT policy, identity taken from the MQTT topic, TLS-only encrypted private bucket, DynamoDB point-in-time recovery, no secrets in code |

## 2. How it works

1. **Device** publishes one JSON reading every 2 seconds to `vehicles/<vehicle_id>/telemetry` (MQTT, QoS 1). `simulator.py` uses AWS credentials; `device_mqtt.py` uses a per-vehicle X.509 certificate like real telematics hardware.
2. **AWS IoT Core** checks the device policy (a car may publish only to its own topic) and runs three topic rules. Every rule takes the vehicle id from the topic with `topic(2)`, never from the payload.
3. **Rule 1, history:** sends every reading to **Amazon Data Firehose**, which buffers for 60 seconds or 1 MB. On the way, the **validation Lambda** checks and normalises each record. Clean records go to `telemetry/dt=YYYY-MM-DD/hr=HH/`, untouched originals to `raw/dt=YYYY-MM-DD/`, and records that cannot be processed to `errors/`.
4. **AWS Glue Data Catalog** defines the table `telemetry` with partition projection (no crawler), and **Amazon Athena** queries it in the workgroup `vehicle-telemetry` (1 GB scan limit per query).
5. **Rule 2, alert:** if any tyre is below 28 psi, it publishes the reading to an **SNS** topic that e-mails the driver/support subscriber.
6. **Rule 3, latest state:** overwrites one item per car in **DynamoDB** table `vehicle-latest-state`, so the newest reading is available in about a second.
7. **CloudWatch** shows a dashboard and sends alarms to a separate operations SNS topic.
8. **Chatbot** maps a question to one of 7 fixed queries, runs it on Athena and, if an LLM is configured, asks the LLM to phrase the small result.

The history path (S3/Athena) is **near-real-time** (about 1 to 2 minutes, because Firehose batches). The latest-state path (DynamoDB) is **real-time** (about a second).

## 3. Repository layout

```
.
├── README.md                     this file: design, playbook, troubleshooting
├── template.yaml                 CloudFormation: every AWS resource (30), incl. the validation Lambda code
├── simulator.py                  simulated cars (AWS credentials over HTTPS)
├── device_mqtt.py                real-device-style publisher (X.509 certificate, MQTT, store-and-forward outbox)
├── send_test_events.py           7 schema-drift / bad-data test events for vehicle CAR-900
├── chatbot.py                    question -> 1 of 7 fixed read-only queries -> optional LLM sentence
├── sql/queries.sql               report queries for the Athena console
├── scripts/
│   ├── provision_device.sh       create an IoT thing + certificate and attach the device policy
│   └── deprovision_device.sh     remove a device completely (run before deleting the stack)
├── tests/                        79 unit tests (no AWS account needed)
├── docs/
│   ├── architecture.dot / .png   architecture diagram
│   └── PRODUCTION_READINESS.md   gap analysis and roadmap from proof of concept to production
├── requirements.txt
└── .github/workflows/ci.yml      runs the tests and cfn-lint on every push
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

**Identity comes from the topic.** The device policy binds a certificate to its own topic, so the topic is the trusted identity. All three IoT rules and the Lambda use the topic id; the payload id is only compared and flagged.

**CloudFormation** needs nothing installed in AWS CloudShell. The resources map one to one to Terraform if a team standard requires it.

**Firehose instead of Kinesis Data Streams** keeps the moving parts to a minimum for landing data in S3. High volume, several consumers or replay would call for Kinesis Data Streams or Amazon MSK.

**Chatbot safety.** The LLM never writes SQL. It returns JSON naming one of 7 intents (`latest_status`, `lowest_tyre`, `fuel_efficiency`, `harsh_acceleration`, `clutch_usage`, `data_quality`, `vehicle_summary`) and optionally a vehicle id that must match `^CAR-\d{3}$`. Every query is a read-only `SELECT` limited to the last 2 days of partitions. If the LLM fails, keyword routing takes over and the table is printed.

## 6. Security

| Layer | In place | Not done yet |
|---|---|---|
| Device identity | one certificate and thing per vehicle; `vehicle-device-policy` allows connect as its own thing name, publish only to its own telemetry topic, subscribe/receive only its own commands topic | fleet provisioning, certificate rotation, AWS IoT Device Defender |
| Service access | least-privilege IAM roles for Firehose, IoT rules and the Lambda, scoped to this stack's resources | IAM Access Analyzer review |
| Data | bucket blocks public access, AES-256 encryption, policy denies non-TLS requests; lifecycle expiry; DynamoDB point-in-time recovery | customer-managed KMS keys, versioning |
| Detection | IoT rule error log, alarms | CloudTrail, GuardDuty, Security Hub |
| Secrets | none in code; `certs/`, `*.pem`, `*.key`, `.env` ignored by git; LLM key only in an environment variable | AWS Secrets Manager |
| Privacy | synthetic data only | consent, minimisation and retention for real location and driving data |

The free tier of an external LLM API may use prompts to improve the provider's products. Use it only with synthetic data.

## 7. Observability

* **Dashboard** `vehicle-telemetry`: messages from vehicles, IoT rule success and failure, Firehose records and delivery, data freshness, validation Lambda invocations and errors, alert e-mails, alarm status.
* **Alarms** (to the operations e-mail topic):

| Alarm | Fires when |
|---|---|
| vehicle-telemetry-iot-rule-failures | any IoT rule fails its action |
| vehicle-telemetry-firehose-delivery | Firehose cannot deliver to S3 |
| vehicle-telemetry-data-freshness | oldest record in Firehose is older than 15 minutes |
| vehicle-telemetry-transform-errors | the validation Lambda crashes |
| vehicle-telemetry-alert-delivery-failed | SNS cannot deliver a driver/support alert |
| vehicle-telemetry-no-data (optional) | no MQTT messages for 10 minutes; off by default (`EnableNoDataAlarm=true` to turn on) |

* **Logs** (30 days): `/aws/lambda/vehicle-telemetry-transform`, `/vehicle-telemetry/firehose`, `/vehicle-telemetry/iot-rule-errors`.

## 8. Tests and CI

```bash
pip install -r requirements.txt
python3 -m pytest -q          # 79 passed, no AWS account needed
cfn-lint template.yaml
```

| File | What it checks |
|---|---|
| tests/test_simulator.py | message fields, physical ranges, leak scenario |
| tests/test_chatbot.py | read-only queries, partition filter, SQL parses (sqlglot), injection blocked, routing incl. `data_quality`, LLM client retries and fallback, key never in errors |
| tests/test_transform.py | the validation Lambda taken from the template, rule error actions, Glue columns, bucket and device policy |
| tests/test_device_and_events.py | device outbox and the CAR-900 test events |
| tests/test_rules_and_device.py | every rule takes the id from the topic, alarms go to operations, PITR on, spoof-test verdict |

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
**Expected:** `79 passed`. **If not:** fix the failing test before going on. If pip is slow because of cfn-lint, run `pip install -q pytest sqlglot boto3 pyyaml awsiotsdk` instead.

### 1. Deploy (new stack or update in place)
```bash
aws cloudformation deploy --template-file template.yaml --stack-name vehicle-telemetry \
  --capabilities CAPABILITY_IAM --parameter-overrides AlertEmail=you@example.com --region ap-south-1
```
Type your real e-mail address. A literal `<...>` placeholder breaks the shell.
**Expected:** `Successfully created/updated stack - vehicle-telemetry` after 2 to 5 minutes. An update that fails rolls back on its own.
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
* IoT Core -> Message routing -> Rules: 3 rules (`vehicle_telemetry_to_s3`, `vehicle_low_tyre_alert`, `vehicle_latest_state_to_dynamodb`), all Active.
* Lambda -> Functions -> `vehicle-telemetry-transform`.
* CloudWatch -> Dashboards -> `vehicle-telemetry`, and CloudWatch -> Alarms -> All alarms (5 alarms, or 6 with the no-data alarm on).
* DynamoDB -> Tables -> `vehicle-latest-state`.

### 2. Data flow: sensor -> AWS -> table
```bash
python3 simulator.py --vehicles 3 --count 30 --interval 2
```
Wait 2 minutes (Firehose buffers 60 seconds), then 📷:
```bash
echo "${BUCKET:?run the BUCKET= line from step 1 first}"
aws s3 ls s3://$BUCKET/telemetry/ --recursive | tail -3     # clean, validated data
aws s3 ls s3://$BUCKET/raw/ --recursive | tail -3           # untouched originals
```
**Expected:** keys under `telemetry/dt=.../hr=.../` and `raw/dt=.../`. If you see bucket names instead of keys, `$BUCKET` is empty.
In Athena (workgroup `vehicle-telemetry`, database `vehicle_telemetry`) 📷:
```sql
SELECT vehicle_id, count(*) AS readings, min(schema_version) AS schema_version FROM telemetry GROUP BY vehicle_id ORDER BY 1;
```
**Expected:** CAR-001 to CAR-003 with at least 30 readings each and schema_version `1`.
**If 0 rows:** wait 2 more minutes, check the workgroup, and look for rejected records with `aws s3 ls s3://$BUCKET/errors/ --recursive`.

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
CAR-001's front-left tyre crosses 28 psi after roughly 15 to 28 batches. Press Ctrl+C after the first alert e-mail, because every reading below 28 psi sends one e-mail.
**Expected:** e-mail "AWS Notification Message" with `"vehicle_id":"CAR-001"` and `tyre_fl_psi` below 28. 📷
```sql
SELECT event_time, tyre_fl_psi FROM telemetry WHERE vehicle_id='CAR-001' ORDER BY event_time DESC LIMIT 30;
```
This proves the chain sensor -> IoT rule -> SNS -> e-mail. It does not prove delivery to a phone app, de-duplication or escalation (not built).

### 5. Schema drift and data validation
```bash
python3 send_test_events.py      # add --alert to also send one low-tyre event (this e-mails you)
```
It sends 7 events for CAR-900: valid, new fields, numbers as strings, a non-numeric value, out-of-range values, a payload claiming CAR-777, and a payload with no vehicle id. **Wait at least 2 minutes** before querying, then 📷:
```sql
SELECT event_time, tyre_fl_psi, tyre_rr_psi, fuel_level_pct, dq_issues, extras FROM telemetry WHERE vehicle_id='CAR-900' ORDER BY ingest_ts;
```
**Expected (7 rows):**

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

### 9. Evidence checklist
3 rules, Lambda, dashboard, alarm list, S3 `telemetry/` and `raw/`, row counts, DynamoDB changing, alert e-mail, 7-row drift result, CAR-777 count 0, `extras` query, raw original, operations alarm e-mails, S3 security outputs, PITR and device policy, device test positive and negative, chatbot keyword and LLM, `79 passed`, teardown verification.

### 10. Tear down and verify nothing is left
Stop all simulators first (Ctrl+C), set `$BUCKET` (step 1), then:
```bash
for T in $(ls certs 2>/dev/null); do bash scripts/deprovision_device.sh $T; done   # required before deleting the stack
aws s3 rm s3://$BUCKET --recursive
aws cloudformation delete-stack --stack-name vehicle-telemetry --region ap-south-1
aws cloudformation wait stack-delete-complete --stack-name vehicle-telemetry --region ap-south-1 && echo STACK_DELETED
```
Verify (each line should print nothing, or "no bucket left"):
```bash
aws cloudformation list-stacks --stack-status-filter DELETE_FAILED CREATE_COMPLETE UPDATE_COMPLETE --query "StackSummaries[?StackName=='vehicle-telemetry'].StackName" --output text
aws dynamodb list-tables --query "TableNames[?contains(@,'vehicle')]" --output text
aws iot list-topic-rules --query "rules[?contains(ruleName,'vehicle')].ruleName" --output text
aws iot list-things --query "things[].thingName" --output text
aws logs describe-log-groups --log-group-name-prefix /vehicle-telemetry --query "logGroups[].logGroupName" --output text
aws s3 ls | grep -i vehicle-telemetry || echo "no bucket left"
```
Then delete the LLM API key in the provider console, run `rm -rf certs __pycache__`, and check Billing the next day. To prove repeatability, deploy again from step 1, run steps 2 and 4, and tear down again.

## 10. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `argument --bucket: expected one argument`, `Invalid bucket name ""`, or `aws s3 ls` lists buckets | `$BUCKET` is empty in this shell | run the `BUCKET=` line from step 1 |
| Stack rolls back | a resource failed | `describe-stack-events` command in step 1 |
| `bash: your: No such file or directory` on deploy | a `<placeholder>` was typed literally | use the real value |
| `pip install --user` fails | CloudShell runs in a virtual environment | drop `--user` |
| Athena 0 rows | Firehose buffer not delivered yet, or wrong workgroup | wait 2 to 3 minutes; workgroup `vehicle-telemetry`; look in `errors/` |
| Files in `errors/processing-failed/` | record not JSON or without any vehicle id | open one file and read the original record |
| `iot-rule-errors` shows `Missing the key vehicle_id` | stack older than v4.1 | deploy this template (rules take the id from the topic) |
| No alert e-mail | subscription not confirmed | confirm both e-mails, check Spam |
| Too many alert e-mails | leak run left going | Ctrl+C; alerts are not de-duplicated (known limitation) |
| Device cannot connect | wrong thing name, policy not attached, wrong endpoint | re-run `provision_device.sh`; use the same name in `--thing` |
| Stack delete fails on `vehicle-device-policy` | a certificate is still attached | run `deprovision_device.sh` for every thing |
| Bucket delete fails | bucket not empty | `aws s3 rm s3://$BUCKET --recursive`, delete again |
| `export` vs `set` | `set VAR=...` is Windows syntax | use `export VAR=...` in CloudShell |
| LLM `HTTP 400 API key not valid` / `403` / `404` / `429` | key pasted wrong / key or API blocked / wrong model name / free-tier limit | paste again / new key in a new project / copy the name from `--list-llm-models` / wait a minute |
| Bedrock `Error 002: Access to Bedrock models is not allowed for this account` | Bedrock blocked for the whole account | use the OpenAI-compatible option or keyword mode |
| Table instead of a sentence, no error line | `LLM_MODEL` not set in this shell | run both `export` lines again |

## 11. Limitations and roadmap

* Sensors are simulated; `device_mqtt.py` was tested from CloudShell, not on vehicle hardware, and nothing was load tested.
* Alerts are e-mail only and not de-duplicated; no SMS, push or ticketing.
* No per-vehicle offline alarm, SLOs or tracing.
* CI only; no automatic deployment (CD) or separate dev/prod stacks.
* No user interface beyond the Athena console and the command-line chatbot.

`docs/PRODUCTION_READINESS.md` lists, area by area, what is built and what a production roll-out needs, the challenges of real hardware, and a phased plan.
