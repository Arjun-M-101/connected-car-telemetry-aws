# From proof of concept to production: gap analysis and roadmap

This project is a **working proof of concept with production-minded foundations**, not a finished production system.
It proves the data path, alerting, validation, and the chatbot idea. This document lists exactly what is done, what is designed but not built, and what a real roll-out needs.

## 1. Status at a glance
| Area | In the project now (v4) | Not yet (needed for production) |
|---|---|---|
| Ingestion | IoT Core MQTT, Firehose, S3 | Kinesis Data Streams or MSK for high volume and replay; batching/compression on device |
| Schema / format change | Validation Lambda: typed known fields, unknown fields kept in `extras`, bad values flagged, raw copy kept, `schema_version` field | Schema registry and versioned contracts per device model; automated promotion of new fields into columns; Iceberg tables with schema evolution |
| Real hardware | Device policy per vehicle, X.509 provisioning script, MQTT sender with store-and-forward outbox | Fleet provisioning at scale, OTA updates (IoT Jobs), tested on a real telematics unit / OBD-II dongle |
| Infrastructure as code | CloudFormation (one template, validated by cfn-lint and 79 automated tests) | Environments (dev/test/prod), remote state not needed (CFN), or port to Terraform if the team standard requires it |
| CI/CD | CI: pytest + cfn-lint on every push | CD: deploy via GitHub Actions with an AWS OIDC role, change sets reviewed, dev -> prod promotion |
| Observability | Dashboard, 5 alarms plus an optional sixth, log groups with retention, IoT rule error logging, optional "fleet silent" alarm | Per-vehicle offline detection, SLOs and error budgets, tracing, on-call rota (PagerDuty/Opsgenie), runbooks per alarm |
| Security | Least-privilege roles, per-device certificates and policy, TLS-only encrypted private bucket, topic-trusted identity, PITR on DynamoDB, secrets never in code | Customer-managed KMS keys, CloudTrail + GuardDuty + Security Hub + Config, VPC endpoints, IoT Device Defender, certificate rotation, data-privacy controls (location is personal data), pen test |
| Alerts | Email via SNS for tyre pressure | De-duplication and hysteresis, escalation, SMS/push to driver app, ticket creation for support, per-customer preferences |
| User interface | CloudShell + Athena + chatbot CLI | QuickSight/Grafana dashboards, support-agent web app, driver mobile app, chatbot behind authenticated API |
| Scale | 3 simulated cars | Load test (1 msg/s x N vehicles), cost model, partitioning and compaction strategy |
| Data governance | Raw/clean separation, lifecycle expiry | Catalog ownership, retention policy per regulation, lineage, access control per team (Lake Formation) |

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
4. Dead-letter handling with alarms and a replay job that re-processes `raw/` after a fix.
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
| Connectivity gaps | tunnels, parking garages, roaming | store-and-forward outbox (built), QoS 1, persistent session, back-off |
| Duplicates and out-of-order data | QoS 1 is at-least-once, buffered replays | `seq` + `event_time` + vehicle id for de-duplication; curated layer dedupe job |
| Wrong clocks | device RTC drift | server `ingest_ts` added (built); compare with `event_time`; flag large skew |
| Sensor noise and faults | stuck, spiking, disconnected sensors | range checks (built), plus stuck-value and rate-of-change rules |
| Different devices and firmware | many suppliers, formats | schema_version, contracts, decoding layer |
| Message volume and cost | 1 Hz x millions of vehicles | batch several readings per message, compress, sample, move to Kinesis/MSK + Flink |
| Identity | stolen or cloned device | per-device certificate (built), policy limits topics (built), Device Defender, revoke certs |
| Updates | bugs in firmware | IoT Jobs OTA with staged roll-out |
| Privacy | GPS and driving behaviour are personal data | consent, minimisation, encryption, retention, access logging (India DPDP Act, GDPR for EU customers) |
| Alert fatigue | flapping values near threshold | hysteresis, cool-down, per-vehicle de-duplication |

**Will it work on real live data?** The architecture is the standard AWS pattern for it and the protocol path is realistic, but it has not been run with physical hardware or at fleet scale. The accurate claim is "validated end to end with simulated sensors and real-device-style authentication; hardware and load tests are the next phase".

## 4. From proof of concept to production
It is a prototype by scope, on purpose: prove the path first, then harden. It does use more than the minimum (IoT Core, Lambda, Firehose, S3, Glue, Athena, DynamoDB, SNS, CloudWatch, IAM, CloudFormation), but a production system also needs the items in the table in section 1.

**Phased plan (realistic effort for one engineer, part time):**
| Phase | Deliverable | Effort |
|---|---|---|
| 1 (done) | Simulated end-to-end, alerts, DynamoDB, chatbot, tests | done |
| 2 (v4 / v4.1, done) | Validation + schema flexibility, observability, security hardening, device identity, playbook; v4.1 fixes rule identity | done |
| 3 | CD pipeline with OIDC, dev/prod stacks, change-set review | 2 to 3 days |
| 4 | Real hardware proof (Pi/ESP32/OBD-II), decoding of real payloads | 1 to 2 weeks |
| 5 | UI: QuickSight dashboard on Athena; support web app; chatbot behind an authenticated API (API Gateway + Lambda + Cognito) | 1 to 2 weeks |
| 6 | Alert engine: de-dup, hysteresis, escalation, SMS/push, ticketing | 1 week |
| 7 | Scale and cost test; Kinesis/MSK if volume demands; Iceberg and compaction | 1 to 2 weeks |
| 8 | Security review: KMS, CloudTrail, GuardDuty, Device Defender, pen test, privacy impact assessment | 1 to 2 weeks |

**Terraform vs CloudFormation:** this project uses CloudFormation because it needs nothing installed in CloudShell. The lakehouse project uses Terraform; porting is mechanical (resources map one to one) and can be done if the team standard is Terraform.

## 5. Observability model
* **Metrics (what is happening):** messages in, rule success/failure, Firehose records and freshness, Lambda errors, alerts sent (dashboard `vehicle-telemetry`).
* **Logs (why):** Lambda, Firehose delivery and IoT rule errors, 30-day retention.
* **Alarms (who gets woken):** rule failures, delivery failure, stale data (>15 min), Lambda crash, alert delivery failure, fleet silent (optional) -> operations email topic.
* **Known gaps:** no per-vehicle "offline" alarm (needs a scheduled check on DynamoDB `event_time` or IoT Fleet Indexing connectivity), no distributed tracing, no SLOs, no on-call routing, no data-quality trend alarm.
* **Suggested SLOs:** 99% of readings queryable within 3 minutes (history path) and 5 seconds (latest state); alert email within 1 minute of a threshold breach.

## 6. Security model
Layers: identity (per-device certificate and policy, IAM roles scoped per service) -> data (TLS in transit, encryption at rest, public access blocked, TLS-only bucket policy, point-in-time recovery) -> detection (error logs, alarms) -> process (no secrets in code, `.gitignore` for certs and keys, CI checks).
**Not done:** KMS customer keys, CloudTrail/GuardDuty/Security Hub/Config, IoT Device Defender, certificate rotation, network isolation (VPC endpoints), Cognito for user access, threat model review, privacy impact assessment.
**LLM data note:** the chatbot sends the question and a small result table to the LLM provider. With synthetic data that is fine; with real vehicle data it can be personal data and needs approval, a data-processing agreement, or an in-region model (Bedrock).
