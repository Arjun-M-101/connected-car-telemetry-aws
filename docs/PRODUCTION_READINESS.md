# From proof of concept to production: status, gaps and roadmap (v5.0)

**Status (19 Oct 2026, project v5.0):** the data path is built to a production architecture: one IoT rule into an encrypted **Amazon Kinesis Data Stream**, Firehose to **Parquet**, a state Lambda that keeps the newest valid reading per car and sends alerts with hysteresis, offline detection, retries with a dead-letter queue and 12 alarms. What is **not** production yet is mostly outside the code: a second environment and a deployment pipeline, organisation-level security services, real hardware and a fleet-scale test, and a user interface. Each gap is listed below with the reason it is not built here.

The performance and scalability comparison of v4.1 and v5.0, with numbers, is in [PERFORMANCE_SCALABILITY.md](PERFORMANCE_SCALABILITY.md).

## 1. Status at a glance
| Area | Built in v5.0 | Still needed for a real roll-out | Why not built here |
|---|---|---|---|
| Ingestion | IoT Core MQTT/HTTPS, **1 rule -> Kinesis Data Stream** (KMS-encrypted, 24 h replay, ordered per vehicle, PROVISIONED or ON_DEMAND) | Device-side batching, IoT Basic Ingest, quota increases above 20,000 msg/s | needs real devices and a traffic forecast |
| Why Kinesis, not MSK | Kinesis is serverless, has a direct IoT rule action, scales per shard, pay per hour | MSK only if the organisation already runs Kafka (Kafka Connect, MirrorMaker, existing consumers) | MSK needs a VPC, brokers or a Serverless cluster and costs far more for this volume (see PERFORMANCE_SCALABILITY.md, section 4) |
| History | Firehose reads the stream, validation Lambda, **Parquet (Snappy)**, raw copy GZIP, partition projection | Apache Iceberg tables with compaction; a curated de-duplicated table | worthwhile at TB scale; the dedupe query covers reports now |
| Real-time state | **Newest valid reading wins** (conditional write on event time in ms), impossible values never stored, future clocks rejected, idempotent retries | Global table in a second region | single-region project |
| Alerts | **Per-vehicle cool-down, reminders, hysteresis, recovery message**, SNS message attributes for routing | SMS/push to a driver app, tickets for support, escalation, per-customer preferences | needs a driver app and a ticketing system |
| Fleet health | **Offline detection** every 5 minutes, one summary e-mail, metric `VehiclesOffline` | IoT Fleet Indexing connectivity status for MQTT devices; GSI instead of a scan above ~100,000 vehicles | the scan is cheap and simple up to that size |
| Schema / format change | Validation Lambda: typed known fields, unknown fields in `extras`, bad values and bad or future times flagged, raw copy kept, `schema_version` | Glue Schema Registry and versioned contracts per device model | needs several real device types |
| Failure handling | Retries, batch bisection, per-record failure reporting, **SQS dead-letter queue**, 24 h stream replay | Replay runbook automation from the DLQ | the stream keeps 24 h, so a manual replay is possible |
| Observability | Dashboard; **12 alarms** (11 + optional no-data): rule failures, stream throttling, consumer lag, DLQ, Parquet conversion, Lambda errors, freshness, delivery, alert delivery; custom metrics | SLOs and error budgets, on-call routing (PagerDuty/Opsgenie), runbook per alarm, tracing | needs an operations team |
| Infrastructure as code | One CloudFormation template (45 resources), tested by 109 unit tests and cfn-lint, a reference-integrity test and a size test | Same template deployed to dev and prod **accounts** (AWS multi-account best practice; fixed names are safe per account) | one personal account |
| CI/CD | CI: pytest, cfn-lint, shell syntax on every push | CD: GitHub Actions with an AWS OIDC role, change sets reviewed, dev -> prod promotion | needs a second account and approvals |
| Security | Least-privilege roles per function, per-device certificate and policy, topic-trusted identity, TLS-only private bucket with versioning, encrypted stream, queue and table, PITR | Customer-managed KMS keys, CloudTrail + GuardDuty + Security Hub + Config, IoT Device Defender, certificate rotation, pen test, privacy impact assessment (DPDP Act / GDPR) | organisation-level services and reviews |
| Performance proof | `load_test.py` (throughput, p50/p95 latency), `resilience_test.py` (late, duplicate, future clock, bad value, hysteresis) | 10,000+ vehicle test with several load generators | needs quota increases and budget |
| User interface | CloudShell, Athena, dashboard, chatbot CLI | QuickSight/Grafana, support web app, driver app, chatbot behind an authenticated API | separate product work |
| Data governance | Raw/clean separation, lifecycle, IA tiering, versioning | Catalog ownership, retention per regulation, lineage, Lake Formation | organisation policy |

## 2. Different fields or formats from real devices
**Today the pipeline copes with: extra fields, missing fields, numbers sent as text, out-of-range or broken values, and a wrong vehicle id.** (README, Playbook step 5 tests each one.)
* Known sensor fields are typed and range-checked in a Lambda inside Firehose. Bad values become empty and the row gets a `dq_issues` flag; the original message is always kept in `raw/`.
* Unknown fields are not lost: they are stored as JSON text in `extras` and can be queried immediately with `json_extract_scalar(extras, '$.field')`.
* Only unparseable messages or messages without any vehicle id go to `errors/processing-failed`.

**What it does NOT do automatically:** turn a brand-new sensor into a proper column. That is a deliberate choice: a person decides the name, unit and valid range, then adds it in three places (Lambda field list, Glue table column, tests). Fully automatic schema discovery (Glue crawler) is possible but dangerous in production: a typo in one device can silently create a wrong column or change a type and break dashboards.

**Production strategy (levels):**
1. Contract per device model and firmware: `{device_type, schema_version}` in every message. (`schema_version` is already carried.)
2. Raw layer immutable (done), curated layer typed (done), analytics layer (aggregates) built from curated.
3. Schema registry (AWS Glue Schema Registry) and compatibility rules (add optional fields only).
4. Dead-letter handling with alarms (built in v5.0 for the real-time path: SQS queue + alarm) and a replay job that re-processes `raw/` after a fix (not built).
5. Different formats (binary CBOR/Protobuf, CAN bus signals) are decoded at the edge or in a decoding Lambda; the rest of the pipeline stays JSON.
6. Units: always send SI/agreed units and a unit field if mixed (psi vs kPa vs bar is a classic bug).

## 3. Moving to real hardware
**Real data path:** sensors -> ECU / CAN bus -> telematics control unit (TCU) or an OBD-II dongle -> cellular/Wi-Fi -> MQTT over TLS with an X.509 certificate -> AWS IoT Core -> same pipeline.
`device_mqtt.py` already speaks the same protocol a real device would (certificate, per-vehicle topic, QoS 1, reconnect, store-and-forward). `simulator.py` uses AWS credentials instead, which a real car must never hold.

**Cheap ways to move to real hardware (in order):**
1. Run `device_mqtt.py` with a real certificate from a laptop or CloudShell (done: README, Playbook step 7).
2. Run it on a Raspberry Pi or ESP32 reading real sensors (tyre pressure BLE sensors, a potentiometer as the pedal, a fuel-flow sensor).
3. Read a real car through an ELM327 OBD-II adapter with the python-OBD library (speed, throttle, fuel trim; tyre pressure and clutch are often not exposed on generic OBD-II).
4. Production: a certified telematics unit from a supplier, with the AWS IoT Device SDK or AWS IoT Greengrass.

**Real-world challenges and what handles them:**
| Challenge | Why it happens | Mitigation |
|---|---|---|
| Connectivity gaps | tunnels, parking garages, roaming | store-and-forward outbox (built), QoS 1, persistent session, back-off; offline detection after 10 minutes (built v5.0) |
| Duplicates and out-of-order data | QoS 1 is at-least-once, buffered replays | latest state: newest `event_time` wins, idempotent (built v5.0); history keeps every copy, reports use `telemetry_dedup` (sql/queries.sql); a curated dedupe job for large tables is next |
| Wrong clocks | device RTC drift | server `ingest_ts` (built); a reading more than 5 minutes ahead of AWS is flagged `future_event_time` and cannot change the latest state (built v5.0); time zones and formats parsed to epoch ms (built v5.0) |
| Sensor noise and faults | stuck, spiking, disconnected sensors | range checks (built), plus stuck-value and rate-of-change rules |
| Different devices and firmware | many suppliers, formats | schema_version, contracts, decoding layer |
| Message volume and cost | 1 Hz x millions of vehicles | Kinesis Data Stream in front (built v5.0), Parquet (built v5.0); batch several readings per message on the device, IoT Basic Ingest, Managed Flink for windowed aggregates (next) |
| Identity | stolen or cloned device | per-device certificate (built), policy limits topics (built), Device Defender, revoke certs |
| Updates | bugs in firmware | IoT Jobs OTA with staged roll-out |
| Privacy | GPS and driving behaviour are personal data | consent, minimisation, encryption, retention, access logging (India DPDP Act, GDPR for EU customers) |
| Alert fatigue | flapping values near threshold | per-vehicle cool-down, reminder, hysteresis band and a 'back to normal' message (built v5.0) |

**Will it work on real live data?** The architecture is the standard AWS pattern for it, and v5.0 handles the problems real devices cause (duplicates, late and out-of-order data, wrong clocks, impossible values, connection gaps, flapping values). It has not been run with physical hardware or at fleet scale. The accurate claim is "validated end to end with simulated sensors and real-device-style authentication, with a resilience test and a load test on the live stack; hardware and 10,000+ vehicle tests are the next phase".

## 4. Roadmap
| Phase | Deliverable | Status |
|---|---|---|
| 1 | Simulated end-to-end, alerts, DynamoDB, chatbot, tests | done |
| 2 (v4 / v4.1) | Validation and schema flexibility, observability, security hardening, device identity, playbook | done |
| 2b (v4.1.1) | Re-runnable, verifying tear-down | done |
| 3 (v5.0) | Kinesis Data Stream, Parquet, newest-valid-wins state, alert hysteresis, offline detection, DLQ, 12 alarms, load and resilience tests, scalability analysis | done |
| 4 | CD pipeline with OIDC; dev and prod accounts | 2 to 3 days |
| 5 | Real hardware proof (Raspberry Pi, ESP32 or OBD-II) | 1 to 2 weeks |
| 6 | UI: dashboards on Athena; support web app; chatbot behind API Gateway + Cognito | 1 to 2 weeks |
| 7 | Driver/support channels: SMS or push, tickets, escalation | 1 week |
| 8 | 10,000+ vehicle test; Iceberg and compaction; Managed Flink aggregates if needed | 1 to 2 weeks |
| 9 | Security review: KMS, CloudTrail, GuardDuty, Device Defender, pen test, privacy impact assessment | 1 to 2 weeks |

**Terraform vs CloudFormation:** CloudFormation needs nothing installed in CloudShell. Porting to Terraform is mechanical if the team standard requires it.

## 5. Observability model
* **Metrics:** messages in, rule success/failure, Kinesis records and throttles, consumer lag (iterator age), Firehose records, freshness and Parquet conversion, Lambda errors, state counters (Updated, Stale, BadTime, FutureSkew, AlertsSent, AlertsSuppressed, Recovered, Failed), vehicles offline, alerts sent.
* **Logs:** three Lambdas, Firehose delivery, IoT rule errors; 30-day retention.
* **Alarms -> operations topic:** see the README table (12 with the optional no-data alarm).
* **Suggested SLOs:** 99% of readings in the latest state within 5 s and queryable in Athena within the buffer interval + 3 minutes; first alert e-mail within 1 minute of the first low reading; offline reported within OfflineAfterMinutes + 5 minutes.

## 6. Security model
Layers: identity (per-device certificate and policy, one IAM role per function scoped to its resources) -> data (TLS in transit; encryption at rest in S3, Kinesis, SQS and DynamoDB; public access blocked; versioning; PITR) -> detection (error logs, alarms) -> process (no secrets in code, `.gitignore` for certs and keys, CI checks).
**Not done:** customer-managed KMS keys, CloudTrail/GuardDuty/Security Hub/Config, IoT Device Defender, certificate rotation, VPC endpoints (no VPC resources are used), Cognito for user access, threat model, privacy impact assessment.
**LLM data note:** the chatbot sends the question and a small result table to the LLM provider. With synthetic data that is fine; with real vehicle data it can be personal data and needs approval, a data-processing agreement, or an in-region model (Bedrock).
