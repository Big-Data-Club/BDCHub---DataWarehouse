# ADR-001: Serverless warehouse and processing stack

- Status: Accepted for Phase 0; implementation is planned
- Original decision date: 2026-08-31
- Documentation revision: 2026-10-21
- Owners: BDC Hub platform/data team
- Scope: analytical storage, ingestion, transformation, orchestration, catalog, and cost controls

The project owner's later [billing-disabled decision](../../docs/adr/2026-10-21-bigquery-billing-disabled-foundation.md)
supersedes this ADR's billing-enabled assumption for `bdc-web-487000`. The active
initial slice uses real-event batch loads and logical views; the larger SCD/CDC
architecture below remains planned. See the [current setup](05-bigquery-setup.md).

## 1. Context and constraints

BDC Hub's K3s manifests define a single Kafka KRaft broker with a
`512 MiB–1 GiB` JVM heap, `0.5 CPU/1 GiB` request, `2 CPU/2 GiB` limit,
`10 GiB` PVC, and internal replication factor `1`. See
[the Kafka manifest](../../k3s/base/kafka-statefulset.yaml). These are repository
settings, not a live capacity measurement. Historical disk pressure makes the
node unsuitable for a permanent warehouse database, a full Kafka Connect cluster,
or a long-lived object lake.

The original operational inventory uses Neon PostgreSQL through pooled endpoints
for Auth, LMS, Lab, Chat, and AI. Revalidate the deployment inventory before
provisioning connectors. Personalize already has a local DuckDB/Parquet pipeline;
Recommender publishes Kafka events. The warehouse must isolate analytical load
from application infrastructure while supporting SCD history, semi-structured
events, partition pruning, and SQL.

Near-zero cost means operating within measured allowances where possible, not a
pricing guarantee. Recheck current pricing, currency, egress, and quotas before
rollout. Configure alerts and stop controls.

## 2. Decision drivers

In priority order:

1. Avoid permanent warehouse storage/compute on K3s.
2. Support dimensional modelling, window functions, `MERGE`, partitions, and clustering.
3. Grow from a few GB to hundreds of GB without redesigning the platform.
4. Keep initial cost low and observable, with explicit operating limits.
5. Minimize database administration and infrastructure maintenance.
6. Support batch and near-real-time ingestion with idempotency and replay.
7. Provide IAM, audit, row/column controls, and PII separation.
8. Use SQL, Git, and CI for development and review.

## 3. Alternatives considered

These are architecture trade-offs, not current vendor price quotations.

| Option | Strengths | Limitations in this project | Decision |
|---|---|---|---|
| BigQuery | Managed columnar analytics, SQL, partitioning/clustering, IAM, batch loading | Billing and cost controls needed for the intended DML/streaming pipeline | Selected |
| BigQuery Sandbox | Useful for SQL demonstrations without a billing account | Tables expire after 60 days; no DML, streaming, or Data Transfer Service | Demo only |
| ClickHouse Cloud | Fast analytical queries and incremental aggregation | Different operating/model choices; no assumption of permanent free hosting | Not the initial target |
| Neon/PostgreSQL | Familiar relational model and SQL | Analytical scans compete with OLTP compute, storage, and network budgets | Operational source only |
| DuckDB on K3s | Simple local analytical SQL | Single-host storage, locks, backup, and disk pressure | Keep existing Personalize use; no new canonical DW here |
| MotherDuck | Managed DuckDB experience | Requires separate evaluation of governance, integration, limits, and cost | Prototype candidate |

## 4. Decision

### 4.1 Warehouse and naming

Use BigQuery in `asia-southeast1` (Singapore). Keep related intermediate storage
in the same region to simplify residency and avoid unnecessary transfer costs.
Follow [the setup guide](05-bigquery-setup.md) for development provisioning.

```text
GCP project: bdc-hub-analytics-<env>

bdc_raw_<env>         append-only event/CDC landing, short retention
bdc_staging_<env>     typed, deduplicated, quarantined records
bdc_core_<env>        conformed dimensions, facts, bridges, snapshots
bdc_mart_<env>        dashboard aggregates
bdc_governance_<env>  metadata, watermarks, quality results, deletion ledger
bdc_pii_<env>         restricted identity mapping, only when necessary
```

Datasets represent lifecycle and access boundaries. Dimensions and facts are
kinds of tables within `bdc_core`, not separate `dim` and `fact` datasets.

### 4.2 Ingestion

Phase 1 uses one lightweight sink replica on K3s:

- Consume implemented domain topics with a dedicated warehouse consumer group.
- Validate and redact records before landing them; preserve source identity.
- Buffer in bounded memory and flush by size or after 1–5 minutes.
- Choose batch load or the Storage Write API according to freshness, measured
  volume, load-job quotas, and recovery requirements.
- Commit only acknowledged, contiguous offsets in each Kafka partition.
- Expect duplicates after a crash between BigQuery acknowledgement and offset commit.

Do not install a full Connect cluster and multiple Debezium connectors by default.
Selective CDC starts in Phase 3 after capacity measurement and WAL/slot safeguards.

Shared-pool batch loading has no ingestion charge by default, while storage,
queries, intermediate storage, and transfers can still be billed. The Storage
Write API has separate allowances and pricing. Verify both against
[BigQuery pricing](https://cloud.google.com/bigquery/pricing) before rollout.

### 4.3 Transformation

Prefer versioned SQL and a thin runner in Phase 1; Dataform Core SQLX can be
introduced when dependency management justifies it. The transformation DAG is:

```text
raw -> staging normalization/deduplication -> dimensions -> facts -> snapshots -> marts
```

Each model declares a unique key, partition scope, owner, description, freshness,
and quality checks. Use deterministic surrogate keys rather than Redis counters.

### 4.4 Orchestration

- The consumer loop manages ingestion continuously.
- Initial transformation targets are every five minutes for facts and every
  fifteen minutes for dimensions, subject to dependency/late-binding handling.
- Daily marts remain provisional during the 48-hour allowed-lateness window.
- Use a scheduled runner for bounded transformation jobs. Cloud Scheduler is a
  trigger and needs a runnable target; it does not execute SQL on its own.
- Use GitHub Actions for schema validation and deployment, not a daemon sink.

### 4.5 Observability

Track records read/written/rejected/deduplicated, input/output bytes, batch latency,
consumer lag by partition, event-time freshness, BigQuery processed bytes and
slot/load errors, CDC retained WAL, quality results, and quarantine counts.
Separate ingestion health from transformation and dashboard freshness.

## 5. Cost controls

Budget alerts are not hard spending caps. Combine:

1. Budget alerts at 25/50/75/90/100%, with notification delivery tested.
2. Project/user query quotas and `maximum_bytes_billed` on query jobs.
3. `require_partition_filter = true` on large partitioned tables.
4. Raw expiration of 30–90 days and staging expiration of 7–30 days, subject to policy.
5. Dashboard access to curated marts rather than unrestricted raw scans.
6. Separate ingestion, transformation, deployment, and analyst identities.
7. Daily cost/volume forecasts and a stop control for analytics workloads.
8. Privacy filtering to avoid storing unnecessary large or sensitive payloads.

Batch loading needs `bigquery.jobs.create`, which can also permit query job
submission. Do not claim a batch loader cannot submit queries merely because
identities are separate. Restrict readable datasets, use quotas, and audit jobs;
a Storage Write API-only identity can omit job creation when it is not needed.

Stopping a sink consumes the remaining Kafka retention window. Alert on that
recovery deadline and preserve a source replay path; stopping analytics must not
stop the application. Notification delays and already-running work mean these
controls cannot promise an exact zero-cost ceiling.

## 6. Security and privacy

| Class | Examples | Warehouse policy |
|---|---|---|
| Public | Published course title, public feature catalog | Allowed in core/marts |
| Internal | Event type, latency, score, rank | Internal IAM access |
| Confidential/PII | Email, display name, organization membership | Restricted dataset/columns; only for an explicit purpose |
| Restricted | Password hashes, bearer/API keys, raw IP, chat bodies, prompts/responses, learner code | Excluded by default |

Where explicitly approved, use rotating HMAC pseudonyms rather than raw IP, and
retain coarse location only for the agreed purpose. Parse user agents into
browser/device/OS categories; minimize retention of raw strings. Chat analytics
keeps counts and flags. MCP keeps tool, actor, scope, target, result class, and
latency, without sensitive arguments or tokens.

Service-local numeric user IDs remain identifying data; moving them into a
warehouse does not anonymize them. Use a stable analytics pseudonym where the
contract requires one and isolate any identity mapping. A deletion ledger must
cover removal from raw, core, marts, replay inputs, and later backfills under the
agreed policy. Normal append-only processing does not override privacy erasure.

## 7. Consequences

Benefits include a small ingestion footprint on K3s, independently scalable
analytics, reproducible SQL history, and per-user/feature/model cost attribution
without collecting content bodies.

Trade-offs include a billing-enabled project, eventual consistency, mandatory
deduplication, application-enforced BigQuery key integrity, and additional CDC
network/compute/WAL costs. BigQuery does not enforce declared primary/foreign keys.

## 8. Modelling pitfalls to avoid

- `GENERATE_UUID()` returns a string, not an `INT64` surrogate key.
- `FARM_FINGERPRINT` is a 64-bit hash with collision risk. Use canonicalized
  SHA-256 string keys for this design and retain natural keys for reconciliation.
- Choose partitions from throughput and parallelism measurements, not a universal count.
- Keep one canonical role-history representation; use membership bridges only
  for actual many-to-many relationships.
- Historical model costs require effective-dated pricing or a call-time snapshot.
- Compare retained bytes with storage allowances; monthly incoming bytes alone
  do not describe a year's retained storage.

## 9. References

- [BigQuery pricing](https://cloud.google.com/bigquery/pricing)
- [BigQuery Sandbox](https://docs.cloud.google.com/bigquery/docs/sandbox)
- [Batch loading](https://docs.cloud.google.com/bigquery/docs/batch-loading-data)
- [Storage Write API](https://docs.cloud.google.com/bigquery/docs/write-api)
- [BigQuery IAM](https://docs.cloud.google.com/bigquery/docs/access-control)
- [Neon logical replication](https://neon.com/docs/guides/logical-replication-neon)
- [Neon network transfer](https://neon.com/docs/introduction/network-transfer)
- [Debezium PostgreSQL connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
