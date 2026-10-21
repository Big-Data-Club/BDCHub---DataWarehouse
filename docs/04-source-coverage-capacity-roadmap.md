# Source coverage, capacity, and implementation roadmap

Owner: BDC Hub platform/data team with service owners and DevOps.
Status: planning inventory and rollout gates, not production deployment evidence.

The initial project foundation and bounded live-source commands are now
implemented; use [the active setup](05-bigquery-setup.md) for observed deployment
status. The phases below describe the broader target, not completed milestones.

## 1. Inventory and evidence

The original inventory date is **2026-08-31**. Documentation was revised on
**2026-10-21**. Treat historical observations separately from checked-in settings;
measure the live system before choosing production capacity.

### Infrastructure

The [Kafka manifest](../../k3s/base/kafka-statefulset.yaml) defines Kafka 3.7 KRaft,
one broker, topic auto-creation, three default partitions, a `512 MiB–1 GiB`
heap, `0.5 CPU/1 GiB` request, `2 CPU/2 GiB` limit, and `10 GiB` PVC. Internal
replication factor is one; broker/PVC loss is not tolerated by this topology.
The original deployment inventory describes one K3s node and Neon PostgreSQL
with pooled application endpoints. Revalidate both before rollout.

Personalize owns DuckDB/Parquet analytics and profiles. It is not a replacement
for a durable, governed warehouse. Consult the implementation-backed
[Data Platform guide](../../docs/DATA_PLATFORM.md) before assuming a topic exists.

### Existing signals and gaps

| Domain | Source evidence/surface | Remaining work |
|---|---|---|
| LMS | Learning events, micro-interactions, document/AI/graph/maintenance commands | Standard enrollment/content lifecycle events and consistent context |
| AI | Document/job/graph status and node merge events | Per-user LLM context, durable usage, embedding/Studio telemetry |
| Lab | Job/session constants and consumers | Verify actual runtime publishers; add submission/task completion contracts |
| Personalize | Analytics/recommendation consumers, profile/notification producers | Some accepted topics have no confirmed producer |
| Recommender | `recommender.interactions.v1` | Standardized impression/outcome attribution in warehouse |
| Auth | Notification consumption | Interactive login/session/security telemetry |
| Chat | Database/Redis messaging | Metadata analytics publication |
| MCP | Audit table and tool adapter | Durable event publication, correlation, latency, client/tool version |

For the initial slice, `lms.analytics.interactions` and
`recommender.interactions.v1` have implemented producer/consumer contracts.
`user.login.events`, `lms.analytics.telemetry`, and `lms.course.interactions`
remain consumer-only gaps until a producer and end-to-end test are demonstrated.
The separate learning worker consumes `learning-events`; verify producer semantics
and compatibility before adding it. Commands/status signals are not automatically
complete analytical facts, and declared topic constants are not publication evidence.

## 2. Source-to-target plan

Priority expresses analytical importance, not implementation order. The existing
LMS/recommendation slice comes first; selective CDC waits until Phase 3.

| Source | Capture | Warehouse target | Priority |
|---|---|---|---|
| Auth users, roles, organizations | Source-owned snapshot, then selective CDC | User/org dimensions and membership bridge | P0 |
| Interactive login attempt | Durable domain event/outbox as required | Login attempt fact | P0 |
| Session lifecycle | Events and state snapshots | Session fact | P1 |
| Courses, sections, content | Snapshot, publish events, later CDC | Content/course dimensions | P0 |
| Enrollment mutation | Outbox | Enrollment lifecycle fact | P0 |
| Learning/micro-interaction | Existing event adapter | Content/feature facts | P0 |
| Quiz, question, response | Outbox or selected CDC | Dimensions and attempt/response facts | P1 |
| Labs, versions, tasks | Snapshot, later selected CDC | Lab dimensions | P0 |
| Lab session, submission, test results | Verified runtime events/outbox | Lab facts | P0 |
| HPC resource/job | Runtime event | HPC fact | P1 |
| Chat/channel membership metadata | Outbox | Chat facts/bridge | P1 |
| Recommendation impression/outcome | Existing contract | Recommendation facts | P1 |
| AI models and tasks | Snapshot, later selected CDC | AI dimensions | P0 |
| LLM attempt | Context/durability migration, then outbox/selected CDC | Attempt fact | P0 |
| Agent turn/session/feedback | Outbox/selected CDC | Agent facts | P1 |
| Indexing, embeddings, VLM, reranking | Instrument job pipeline | Index/embedding facts | P0 |
| Studio generation/export | Instrument job pipeline | Studio fact | P1 |
| MCP key lifecycle/tool calls | Owning-service outbox | MCP facts | P0 |
| Skill/profile state | Events and approved daily exports | Skill evidence/snapshots | P1 |

## 3. Kafka capacity model

Use observed encoded sizes, compression, and fan-out rather than a fixed forecast:

```text
ingress_bytes_day = events_day * mean_encoded_event_bytes * framing_multiplier
retained_bytes    = ingress_bytes_day * retention_days * replication_factor
                    * stored_compression_fraction * safety_factor
network_bytes_sec ~= event_rate * encoded_bytes * producer_and_consumer_copies
```

The stored compression fraction is compressed/uncompressed size, for example
0.3–0.7. Use an initial safety factor of at least 1.5, then include internal topics,
segment/index overhead, retries, and burst headroom explicitly. Consumer groups
increase network reads; partition count alone does not describe load.

### Illustrative scenarios

Assume 1.5 KiB encoded payloads, framing multiplier 1, compression fraction 0.5,
safety factor 1.5, replication factor 1, and three days of retention:

| Events/day | Mean rate | 20× peak rate | Estimated retained analytics data |
|---:|---:|---:|---:|
| 100,000 | 1.2/s | 23/s | 0.32 GiB |
| 1,000,000 | 11.6/s | 232/s | 3.22 GiB |
| 5,000,000 | 57.9/s | 1,158/s | 16.09 GiB |

These are sizing examples, not measured traffic or total PVC usage. One million
events/day already consumes a material share of the 10 GiB PVC before existing
and internal topics. Disk and replay durability may fail before CPU throughput.
Keep steady-state Kafka PVC utilization below the proposed 70% target.

### Phase 1 tuning candidates

- Collect bytes/messages in/out, partition lag age, request latency, log size,
  offline partitions, PVC usage, and disk growth for at least seven days.
- Set explicit topic partitions/retention rather than relying on auto-create defaults.
- Start analytics retention at 24–72 hours and DLQ at seven days with byte caps.
- Benchmark LZ4/Zstd producer compression.
- Start the sink near `100m CPU/128 MiB` request and `500m CPU/512 MiB` limit;
  these are test candidates, not proven resource requirements.
- Try 500–5,000 events per batch or a 1–5 minute flush, with an independent byte
  limit. Check per-table load-job quotas before choosing batch-load cadence.
- Alert at proposed PVC thresholds of 60/70/80%, plus lag age and growth slope.
- Preserve 25–30% free space; image/log cleanup must not delete Kafka data.

If throughput or recovery requirements exceed this topology, evaluate funded
managed transport or additional brokers. Extending retention on a full disk is
not a scaling strategy. Any telemetry sampling must preserve metric definitions
and must never silently remove core business/cost events.

## 4. Warehouse storage and query budget

Estimate storage using retained logical bytes under the selected billing model:

```text
retained_logical_bytes = rows_per_day * mean_logical_bytes_per_row * retention_days
query_workload_bytes  = sum(bytes_scanned_per_run * runs_per_period)
```

Include every retained layer, snapshots, replay artifacts, quarantine, and the
selected storage model's time-travel/fail-safe behavior. Compressed Parquet file
size is not the same as BigQuery logical storage. For example, 3.7 GiB of new data
per month retained for twelve months is about 44.4 GiB before deletion/aggregation,
not 3.7 GiB. Recheck current allowances rather than embedding them as guarantees.

Initial policy proposals are raw for 30 days, staging for 7–30 days, core facts
for thirteen months, and small daily marts for up to twenty-five months. SCD and
identity retention need domain/privacy review. Keep Chat/MCP/AI analytics to
approved metadata. Forecast table storage and query spend for 30/90 days; use
daily checks initially and a monthly policy review.

When forecast exceeds budget, consider shorter raw retention, approved telemetry
aggregation, cold exports with lifecycle rules, or an explicit paid budget. Cold
storage has its own access/transfer/operation costs and deletion obligations.

## 5. Implementation phases

### Phase 0: contracts and foundation

1. Agree on architecture, grains, metric definitions, privacy, and retention.
2. Measure source traffic and Kafka/DB capacity over seven days.
3. Prepare a development GCP project, identities, cost controls, and datasets.
4. Define source/target catalogs and executable contract test requirements.

Exit: architecture review completed and unresolved P0 ownership/privacy questions
identified and resolved for the initial slice. See [setup](05-bigquery-setup.md).

### Phase 1: one complete slice, without CDC

1. Implement raw/governance DDL and the loader.
2. Consume existing LMS micro-interaction and recommendation topics using a new group.
3. Normalize event identity and prove deduplication/replay behavior.
4. Add date/feature dimensions and source-owned minimal user/course snapshots.
5. Deliver one daily learning-activity mart and reconciliation report.

Exit: duplicate, replay, quarantine, rebalance, quota-failure, and source-count
checks pass; freshness and cost are measured. Missing historical context remains
explicit rather than being fabricated.

### Phase 2: actor/context telemetry and outbox

Add Auth login/session events, LMS enrollment/content outbox, verified Lab events,
Chat metadata outbox, AI usage context/durability, and MCP audit correlation.
Build AI/indexing/MCP facts and daily marts after source contracts are ready.

Exit: a user action can be traced to its tool/job and every attributable provider
attempt, with a documented loss/reconciliation policy.

### Phase 3: selective CDC and SCD2

Start with one source-owned dimension snapshot/connector, schedule Neon changes
with its owner, monitor compute/network/WAL impact, and validate late/out-of-order
history. Expand the allowlist only after capacity/privacy review.

Exit: slot loss, restore, source delete, resnapshot, and timeline repair runbooks
have been exercised without assuming an always-complete Kafka history.

### Phase 4: broader marts and measured optimization

Add user/course/content/Lab/Chat/AI/MCP marts, cohorts, funnels, retention, feature
adoption, and cost attribution. Tune aggregation/expiration from actual workloads
and add the instructor/organization/admin access policies each use case needs.
Use the [optimization playbook](06-data-engineering-optimization.md).

## 6. Acceptance criteria

- Trace IDs connect domain actions, tools/jobs, and provider attempts where promised.
- Exact login time and reporting-day login definitions are independently testable.
- Historical fact joins resolve the correct SCD version at event time.
- Duplicate replay produces equivalent core results.
- Analytics failures do not fail application requests.
- Recovery throughput clears lag before source retention expires.
- Restricted payloads and secrets are excluded before persistence.
- Each dashboard metric has a versioned definition and owner.
- Reconciliation differences remain within an explicit, explained tolerance.
- Query limits, retention, alerts, and analytics stop/resume behavior are tested;
  nobody treats budget notification as an exact monetary cap.

## 7. Inputs required before production

Development and metadata/contract verification can proceed while these are being
resolved. Continuous production ingestion needs a project owner, the selected
billing mode, organization IAM constraints,
retention/consent policy, measured volume, confirmed region/residency, freshness
SLO, analytics RPO/RTO, single-broker loss tolerance, and historical provider price
and currency-conversion sources.
