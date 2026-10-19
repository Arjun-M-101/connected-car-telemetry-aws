# Performance and scalability: current (v4.1) vs production (v5.0) architecture

This document explains where the v4.1 design stops scaling, why, and what v5.0 changes. The numbers come from three sources, and each one is labelled:
**[measured]** in a live run of this project; **[quota]** an AWS default service quota (most can be raised on request, so they are planning limits, not hard walls); **[estimate]** arithmetic from the measured message size, to be confirmed with `load_test.py`.

## 1. The two architectures

| | v4.1 (proof of concept) | v5.0 (production upgrade) |
|---|---|---|
| Ingestion | IoT Core, **3 topic rules**, each runs on every message | IoT Core, **1 topic rule** -> **Kinesis Data Stream** (encrypted, 24 h retention, partition key = vehicle id) |
| History path | rule -> Firehose **Direct PUT** (one `PutRecord` per message) -> Lambda -> **JSON** in S3, 60 s / 1 MB buffer | Firehose **reads the stream** -> same validation Lambda -> **Parquet (Snappy)**, 64 MB / 60-900 s buffer; raw copy **GZIP** |
| Latest state | rule -> DynamoDB `PutItem` (**last message wins**, even if older) | state Lambda, batches of up to 100, **conditional write: newest `event_time` wins**, idempotent on retry |
| Alerts | rule -> SNS on **every** reading below 28 psi | state Lambda -> SNS **once per car per 15 min** (conditional `alert_until`), no alerts from late or replayed readings |
| Failure handling | rule error log only | retries, batch bisection, per-record failure reporting, **SQS dead-letter queue** + alarm |
| Observability | 5 alarms (+1 optional) | 9 alarms (+1 optional) incl. stream throttling, consumer lag, DLQ; custom metrics (updated, stale, alerts sent/suppressed) |
| Data protection | SSE-S3, TLS-only bucket, PITR | + bucket versioning, stream KMS encryption, encrypted DLQ, DynamoDB SSE, IA tiering after 30 days |

## 2. Limitations and drawbacks of v4.1

### 2.1 Throughput ceilings
Message size **[measured]**: the Firehose files of the 18 Oct run hold about **320 bytes per validated reading** (28,852 bytes for 90 readings in one minute).

| Fleet at 1 reading/s per car | Messages/s | MB/s | Readings/day | JSON/day **[estimate]** |
|---|---|---|---|---|
| 3 cars (demo, 0.5 Hz) | 1.5 | 0.0005 | 130 thousand | 41 MB |
| 1,000 cars | 1,000 | 0.32 | 86 million | 28 GB |
| 10,000 cars | 10,000 | 3.2 | 864 million | 276 GB |
| 100,000 cars | 100,000 | 32 | 8.6 billion | 2.8 TB |

Where v4.1 stops:
1. **Firehose Direct PUT in ap-south-1: 1,000 requests/s and 1 MiB/s per delivery stream [quota].** The IoT Firehose action sends one `PutRecord` per message, so the history path tops out at **about 1,000 messages/s, i.e. about 1,000 cars at 1 Hz**. Above that, the rule action is throttled and messages go to the error log, not to S3.
2. **Every message is evaluated by 3 rules.** IoT Core's default is **20,000 rule evaluations/s and 20,000 actions/s per account [quota]**, so 3 rules use the budget 3 times faster (ceiling about 6,700 messages/s, before any quota increase).
3. **Alert e-mail storm.** A car with a slow leak sends one e-mail per reading: 30 e-mails per minute at 0.5 Hz **[measured: the 19 Oct run produced one e-mail every 2 s]**. SNS e-mail delivery is rate-limited (**10 messages/s per account by default [quota]**), so 100 leaking cars at 1 Hz would already exceed it, and nobody reads that many e-mails.

### 2.2 Correctness under real-device behaviour
4. **Out-of-order state.** QoS 1 is at-least-once and `device_mqtt.py` replays its outbox after a connection drop. The DynamoDB rule overwrites with whatever arrives last, so a replayed 10:00:01 reading can overwrite a 10:00:05 reading: the "latest" state goes backwards.
5. **No replay.** Messages go straight from IoT Core to their destinations. If the Lambda or Firehose is misconfigured for an hour, the data of that hour is only in the error log (or lost). There is no buffer to re-read from.
6. **No ordering guarantee** per vehicle on any path.

### 2.3 Cost and query performance
7. **JSON in S3.** Athena charges per byte scanned (**US$5 per TB [quota/pricing, us-east-1 list price]**) and must read every column of every JSON row. A report that needs 2 of 17 columns still reads all of them.
8. **Small files.** A 60 s / 1 MB buffer writes up to 1,440 files per day per prefix. At fleet scale many small files slow Athena down (per-file overhead) and add S3 request cost.
9. **Uncompressed raw copy** doubles storage for no query benefit.

### 2.4 Operability
10. No signal for **consumer lag**, **throttling** or **records that failed every retry**: a slow or broken path is only visible when someone notices missing data.

## 3. What v5.0 changes, and the effect

| Limitation | v5.0 change | Effect |
|---|---|---|
| 1. Firehose Direct PUT ceiling | Firehose reads from the Kinesis stream instead of receiving `PutRecord` calls | history path scales with the stream: **1,000 records/s or 1 MB/s per shard [quota]**; `ON_DEMAND` mode scales shards automatically (default up to **200,000 records/s / 200 MiB/s [quota]**) |
| 2. 3 rule evaluations per message | 1 rule, 1 action | **3x** more messages per second within the same IoT quota |
| 3. E-mail storm | one e-mail per car per `AlertCooldownSeconds` (15 min) | a leaking car at 1 Hz: **900 e-mails -> 1** per 15 minutes; SNS rate limit no longer a factor |
| 4. Out-of-order state | conditional `UpdateItem`: only if `event_time` is newer or equal | latest state never goes backwards; retries are idempotent |
| 5. No replay | stream keeps 24 h | consumers can be fixed and re-read the last 24 h (`StartingPosition`, or a new consumer) |
| 6. No ordering | partition key = vehicle id | all readings of a car are in order on one shard; `ParallelizationFactor` keeps per-key order |
| 7. JSON scans | Parquet + Snappy | Athena reads only the needed columns, compressed: typically **several times (often 5-10x) fewer bytes scanned** for column-subset reports **[estimate; check "Data scanned" in Athena]** |
| 8. Small files | 64 MB buffer, interval a parameter (300-900 s in production) | fewer, larger files; next step compaction or Apache Iceberg |
| 9. Raw copy uncompressed | GZIP | raw storage roughly 5-10x smaller for JSON text **[estimate]** |
| 10. No lag/failure signals | alarms: stream throttled, consumer lag > 60 s, DLQ not empty, state errors; custom metrics | problems page operations within minutes |

### 3.1 Sizing the stream **[estimate]**
Shards needed (PROVISIONED) = max(messages/s / 1,000, MB/s / 1). Our 320-byte readings hit the record limit first:

| Fleet at 1 Hz | Shards | v4.1 history path | v5.0 history and state path |
|---|---|---|---|
| 1,000 | 1 | at its ceiling | 1 shard |
| 10,000 | 10 | throttled (needs quota increases and batching) | 10 shards or ON_DEMAND |
| 100,000 | 100 | not possible without redesign | ON_DEMAND (or 100 shards); raise IoT quotas; batch readings on the device |

### 3.2 The state Lambda keeps up
One conditional `UpdateItem` takes roughly 5-10 ms inside the region, so one batch at a time would process about 100-200 records/s, below what a shard accepts. v5.0 sets `ParallelizationFactor: 10` (up to 10 concurrent batches per shard, order per vehicle kept), giving roughly 1,000-2,000 records/s per shard. Lambda concurrency = shards x 10, well within the default 1,000 per account for up to about 100 shards **[quota]**. Watch the `state-lag` alarm (iterator age).

### 3.3 Latency

| Path | v4.1 | v5.0 |
|---|---|---|
| Latest state (DynamoDB) | about 1 s | about 1-3 s (stream + up to 1 s batching window); measure with `load_test.py` |
| Alert e-mail | seconds, but once per reading | seconds after the first low reading, then de-duplicated |
| History (Athena) | 1-2 min (60 s buffers) | 2-3 min with the demo buffer; 5-15 min with a production buffer (fewer, larger files) |

The trade-off is deliberate: real-time needs use DynamoDB; Athena is for reports, where a few minutes do not matter and bigger files cost less.

## 4. What v5.0 still does not do
* **Multi-region / disaster recovery**: one region. Production: a second region with IoT Core custom domain fail-over, or at least cross-region replication of S3 and DynamoDB global tables.
* **Per-vehicle offline detection**, SLOs and an on-call rota.
* **Compaction / Iceberg** for very large tables; **Glue Schema Registry** for versioned contracts.
* **CD pipeline** (GitHub Actions with OIDC, dev and prod stacks). CI runs tests and cfn-lint.
* **Customer-managed KMS keys**, CloudTrail, GuardDuty, IoT Device Defender, certificate rotation.
* **Hardware and fleet-scale tests**: `load_test.py` runs hundreds of cars from one CloudShell; a 10,000-car test needs several load generators and quota increases.

## 5. How to verify the numbers
1. `python3 load_test.py --vehicles 100 --seconds 60`: messages/s, publish latency, end-to-end p50/p95 to DynamoDB.
2. CloudWatch dashboard `vehicle-telemetry`: Kinesis `IncomingRecords` and `WriteProvisionedThroughputExceeded`, state lag, Firehose freshness.
3. Athena: run the same report on the v5.0 Parquet table and note **Data scanned**; compare with a JSON table if you keep one from v4.1.
4. `aws s3 ls s3://$BUCKET/telemetry/ --recursive --summarize`: file count and average size.

## References
* AWS IoT Core quotas: https://docs.aws.amazon.com/general/latest/gr/iot-core.html
* Amazon Data Firehose quotas: https://docs.aws.amazon.com/firehose/latest/dev/limits.html
* Kinesis Data Streams quotas and on-demand mode: https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html
* Lambda with Kinesis (batching, parallelization, bisect, failure destinations): https://docs.aws.amazon.com/lambda/latest/dg/with-kinesis.html
* Firehose record format conversion to Parquet: https://docs.aws.amazon.com/firehose/latest/dev/record-format-conversion.html
* Amazon SNS quotas: https://docs.aws.amazon.com/general/latest/gr/sns.html
* Athena pricing and columnar formats: https://aws.amazon.com/athena/pricing/ and https://docs.aws.amazon.com/athena/latest/ug/columnar-storage.html
