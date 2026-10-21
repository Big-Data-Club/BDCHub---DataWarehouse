# Data engineering optimization playbook

Owner: BDC Hub platform/data team with source owners and DevOps.
Status: design and tuning guidance; parameter values are starting hypotheses.

Use this playbook for pipeline throughput, freshness, warehouse scan cost, storage,
and recovery. Apply a technique only after identifying a bottleneck and preserving
the relevant event, history, metric, and privacy contracts.

## 1. Establish the baseline

Capture a representative input window with event rate, peak/burst shape, encoded
size percentiles, compression, consumer fan-out, key skew, and late-arrival
percentiles. Record end-to-end freshness separately from batch duration and Kafka lag.
For SQL, retain job statistics, execution stages, bytes processed/billed, slot-ms,
shuffle/spill indicators, wall time, and output cardinality. For storage, separate
logical/physical billing, retained layers, time travel, and intermediate objects.

Change one material variable at a time on the same sanitized snapshot. Record
cold/warm cache conditions and concurrency; result-cache hits are not evidence
of a faster execution plan. Use bounded test workloads outside production nodes.

A useful result states the baseline, proposed change, measured improvement,
correctness comparison, new operational cost, and the condition for reverting it.

## 2. Select the right capture and ingestion method

| Technique | Use when | Trade-off or failure mode to check |
|---|---|---|
| Domain event/outbox | Business meaning or reliable mutation publication matters | Outbox growth, relay ordering, publish/mark duplicate window |
| Source-owned incremental export | A durable row and reliable cursor exist; minutes/hours are acceptable | Equal timestamps, hard deletes, snapshot consistency, DB load |
| Selective CDC | Historical row updates/deletes are needed without adequate events | Slots/WAL, schema changes, source compute/network, resnapshot |
| Batch load | Freshness permits bounded batches and load-job quotas fit | Per-table job counts, small files, acknowledgement recovery |
| Storage Write API | Lower latency or sustained load favors streaming | Stream semantics, retries, offsets, ingestion costs |
| Client aggregation/sampling | High-volume noncritical telemetry dominates | Bias, weights, changed metric definitions, lost rare events |

Prefer the initial lightweight sink over a new processing cluster unless an
observed requirement justifies the extra operational footprint. Never sample
billing, committed business mutations, or security/audit evidence merely to meet
a throughput target. Any reduced-fidelity telemetry needs an explicit metric contract.

## 3. Kafka and consumer efficiency

### Batching, compression, and small files

Flush by record count, encoded bytes, or maximum age, whichever occurs first.
Bound in-flight batches independently from fetch size; a small mean event size
can hide very large events. Measure CPU versus network savings with LZ4/Zstd.
Batching amortizes RPC overhead but increases latency and the amount replayed
after failure. Group compatible small input files into fewer load jobs and avoid
one file/job per event. Select file size from throughput and retry measurements,
not a universal target, and verify active load-job/partition-operation quotas.

### Parallelism and skew

Consumer concurrency cannot exceed useful topic partitions. Increase partition
counts only after checking key distribution, ordering, broker disk, and consumer
bottlenecks. A hot user/course key can dominate a partition even with many total
partitions. Splitting or salting a key is valid only if the ordering contract
survives and downstream recombination is defined.

Scale processing across independent partitions before parallelizing ordered work
inside a partition. Keep one acknowledgement frontier per partition; a fast later
batch must never advance a commit past an earlier failed batch.

### Backpressure and recovery

Pause assigned partitions or bound fetch/in-flight work when the destination slows.
Continue required heartbeats/polling according to the client library's rebalance
contract. Use bounded exponential backoff with jitter and distinguish transient
quota/server errors from permanent schema/auth failures. Shutdown/rebalance must
drain or safely abandon work without committing unacknowledged records.

If backlog is `B` events, arrival rate is `a`, and measured processing capacity is
`c`, approximate drain time is `B / (c - a)` when `c > a`. If `c <= a`, the pipeline
never catches up. Compare drain time with the oldest event's remaining retention,
not the topic's nominal retention alone. Stopping analytics to control cost must
include this replay deadline.

## 4. Incremental transformations

Use two clocks: ingestion time for discovering newly landed data, and event time
for historical facts/reporting. An event received today can affect a fact from
last month. Discover it through today's ingestion partition, then update the
old event partition or enqueue a controlled correction.

Track successful transform boundaries and source versions in governance. Persist
run/batch identity and affected keys/partitions. Advance a watermark after durable
output, never when a job merely starts. Coordinate concurrent runs for the same
keys/partitions so retries do not race or overwrite newer results.

Deduplicate the incoming source before `MERGE`. For append-only event facts,
conflicting content under one event identity is a contract error. For mutable
state, choose the authoritative source version and make an older update unable
to overwrite a newer state. A timestamp-only `MAX(updated_at)` cursor can skip
equal-time rows; use a composite cursor with overlap/deduplication and an explicit
delete channel.

Prune both source and target partitions for `MERGE`, while retaining all possible
matches. Filtering only the source does not prove a cheap target scan. A too-narrow
target predicate can turn an existing key outside that range into an insert;
use a partition-stable fact key or an explicit relocation strategy. Do not use a
recent-event-time predicate as a substitute for complete late-event discovery.

Replace a complete affected partition only when the replacement input is complete
and the write is atomic. Otherwise use key-level mutations with deterministic
ordering. Backfills declare their range, resource budget, checkpoint, ownership,
and reconciliation, using the same canonical transformations as live processing.

## 5. BigQuery query and physical design

The following choices follow Google's
[query performance guidance](https://docs.cloud.google.com/bigquery/docs/best-practices-performance-compute).
Validate each on actual job statistics rather than assuming it improves every table.

| Technique | Apply when | Verify |
|---|---|---|
| Partition pruning | Queries constrain event/reporting/ingestion time | Source and target scans cover only required partitions |
| Ordered clustering | Large enough tables have selective recurring filters | Filter prefix, selectivity, measured bytes, reclustering effects |
| Column projection | Wide tables/raw JSON dominate scans | Only required typed columns are read; `LIMIT` is not a scan cap |
| Typed staging columns | Frequently queried fields require repeated JSON parsing/casting | Parse once, retain rejects, preserve null/invalid distinctions |
| Filter/preaggregate before joins | A stage shuffles many rows unnecessarily | Grain and join cardinality remain correct |
| Materialized/intermediate aggregates | Repeated expensive computation dominates | Supported query shape, freshness, refresh/storage costs |
| Nested/repeated detail | Bounded one-to-many detail is usually read with its parent | `UNNEST` fan-out, array size, update/query patterns |
| Approximate aggregates/sketches | Exploratory distinct counts/quantiles permit error | Explicit accuracy contract and compatible sketch merging |
| Materialized temporary/intermediate results | An expensive expression is reused | CTEs alone do not guarantee single evaluation |

Do not join raw one-to-many facts and then sum measures from both sides. Aggregate
each to the shared business grain first. Avoid Cartesian joins and unbounded
window sorts. Inspect skew and spill before increasing compute. Use exact
reconciliation for billing/compliance-style totals even if dashboards use approved
approximate metrics. Do not add daily distinct counts or average percentiles.

A partition-friendly fact query is explicit about projection and range:

```sql
SELECT
  course_id,
  COUNT(*) AS interaction_count,
  COUNT(DISTINCT user_id) AS active_users
FROM `example-project.bdc_core_dev.fact_content_interaction`
WHERE occurred_at >= @window_start
  AND occurred_at < @window_end
GROUP BY course_id;
```

This is an illustrative target query; the fact table is not implemented yet.
Bind timestamps with an agreed reporting timezone. `DATE(timestamp)` defaults to
UTC; Vietnamese reporting days require an explicit timezone conversion. For DML,
see [partitioned-table mutation guidance](https://docs.cloud.google.com/bigquery/docs/using-dml-with-partitioned-tables).

## 6. Model and storage efficiency

Choose a fact grain that answers the question without retaining restricted content.
Use Type 1 for corrections where past values have no analytical meaning, Type 2
for meaningful history, and periodic snapshots for state-over-time questions.
Hash only Type 2 attributes so cosmetic updates do not generate history churn.
Rebuild only affected natural-key timelines; late changes may still require
historical fact rebinding. Do not replace SCD2 with a current-state dimension
just to reduce row counts if historical queries rely on it.

Use expiration/lifecycle rules per layer and avoid indefinite staging, quarantine,
and temporary exports. Compact eligible small Parquet files for replay/archives;
retain immutable manifests so a retry refers to the same bytes. Native BigQuery
tables do not need a user-managed Parquet compaction job. Aggregate old data only
when drill-down requirements and deletion/reconciliation policies permit it.

Evaluate cold storage by total retained cost, retrieval latency, request/transfer
charges, and the ability to honor erasure during replay. Measure storage under
the selected BigQuery billing model; compressed input file bytes are not logical
warehouse bytes.

## 7. Source-system protection

Source owners implement approved exports/CDC. Page exports by indexed composite
keys with bounded ranges rather than increasing `OFFSET`, and use a documented
snapshot boundary when consistency matters. Keep exports and outbox cleanup in
small batches, monitor query latency/locks, and coordinate major backfills.

CDC is not free change detection: monitor retained WAL, slot acknowledgement,
connector lag, source uptime, and network costs. Compacted topics can lose
intermediate history. Pausing a connector can worsen WAL growth. Use the
[source-owned CDC runbook](02-etl-cdc-pipeline.md), not automatic slot deletion.

Analytical notebooks read approved Parquet or read-only DuckDB snapshots. They
must not open or mutate the live Personalize writer path.

## 8. Cost, observability, and regression gates

Use dry runs, `maximum_bytes_billed`, project/user quotas, job labels, retention,
and storage/query forecasts. Review the
[official cost guidance](https://docs.cloud.google.com/bigquery/docs/best-practices-costs).
Budget alerts and cost dashboards detect spend; they do not guarantee a hard cap.
Show storage, ingestion, queries, transfers, and intermediate infrastructure as
separate cost components.

Before accepting an optimization, check:

- Equivalent facts and metric outputs on normal and skewed input.
- Duplicate replay, conflicting identities, and partial acknowledgement recovery.
- Late/out-of-order updates, source deletes, and facts preceding dimensions.
- Rebalance/shutdown, exhausted quotas, malformed records, and quarantine replay.
- Retention expiry, bounded backfill recovery, and deletion followed by replay.
- Latency/throughput/cost improvement under the same workload and concurrency.

For a documentation-only design, state the required experiments and unresolved
parameters rather than claiming that proposed values were benchmarked. For code
changes, use scenario tests that validate these invariants and keep the measured
before/after evidence with the change.
