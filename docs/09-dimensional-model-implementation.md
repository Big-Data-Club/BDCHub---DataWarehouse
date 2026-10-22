# Implemented dimensional model

Owner: platform/data team. Scope: raw, staging and core; new marts are deferred.
Canonical implementation: [raw envelope DDL](../ddl/003_platform_events.sql),
[core SQL](../transforms/002_platform_core.sql),
[source contracts](../worker/contracts.py), and
[quality analysis](../scripts/analyze_foundation.py).
Source definitions and gaps: [source register](../contracts/sources.json).

## 1. Design decisions

This is a dimensional constellation: separate stars for learning activity,
recommendation events and AI provider attempts. Shared user/course/date dimensions
make their meanings consistent. The core objects are BigQuery logical views over
retained evidence because the selected billing-disabled environment does not
support the intended persistent incremental/SCD2 update path.

Logical dimensions/facts define analytical relationships; physical materialization
is a separate storage/performance choice. The current design does not require an
OLTP-style snowflake for course hierarchies: it would introduce additional joins
before the necessary authoritative source snapshots exist. Lesson/node IDs remain
explicit natural/degenerate identifiers until those snapshots are onboarded.

Design order for every source is business process -> grain -> dimensions ->
facts/measures. Never add another process to a fact just because it shares a user
ID. An AI turn can invoke several tools and several provider attempts; joining
those rows directly to learning events would multiply counts and cost.

## 2. Bus matrix

| Business process / fact | User | Course | Feature | Date | Time | AI model | Degenerate/context keys |
|---|---|---|---|---|---|---|---|
| `fact_learning_activity` | yes | yes | yes | yes | yes | no | interaction, lesson, node |
| `fact_recommendation_event` | yes | nullable | source surface/action | yes | exact timestamp | no | recommendation, set, entity, rank |
| `fact_llm_attempt` | unavailable in current source | unavailable in current source | task code | yes | exact timestamp | nullable | usage, request, attempt number |

Login, sessions, enrollments, formal quiz submissions, Lab, Chat, agent turns,
tool execution and skill snapshots are target processes in the
[modelling catalog](03-data-warehouse-modelling.md), not facts asserted to exist
by this implementation. The machine source register records why each is gated.

## 3. Raw and staging semantics

`bdc_raw_prod.platform_events` is a landing envelope, not a single mixed-grain
business fact. A row is one captured source delivery. Its contract contains:

| Columns | Meaning |
|---|---|
| `event_id`, `source_event_id`, `source_name` | Stable namespaced identity and original source key |
| `record_source`, `schema_version` | Owning producer and normalization contract version |
| `occurred_at`, `ingested_at` | Actual source instant and actual collection instant |
| `user_id`, `course_id`, `request_id`, `session_id` | Only supplied, attributable context; otherwise null |
| `capture_method`, `capture_id` | Kafka delivery or consistent owner snapshot and capture correlation |
| `kafka_topic`, `kafka_partition`, `kafka_offset` | Exact broker coordinates; null for owner snapshots |
| `payload_sha256` | Hash of canonical allowlisted source fields, excluding delivery metadata |
| `payload` | Native JSON with source-specific, strictly allowlisted fields |

Raw is partitioned by UTC ingestion date and clustered by source, course, user,
event identity. Every read has a bounded ingestion partition filter. Original event
time remains available even for arrivals older than the normal business lookback.
Restricted identifiers still require dataset IAM; hashing keys is not anonymization.

`staging.platform_events` retains one delivery only if every retained delivery
under an event ID has the same payload hash. Conflicts go to
`governance.platform_identity_conflicts`. `staging.learning_events` reconciles both
the original LMS raw table and the new envelope, checks cross-path hashes, and
exposes one original interaction. Its conflicts are independently queryable in
`governance.learning_identity_conflicts`.

LMS event IDs keep the existing `lms.analytics.interactions:<interaction_id>`
namespace across the cutover. Equivalent deliveries must preserve the existing
UTC timestamp normalization and hash. Recommendation and AI identities use their
own source namespaces. Snapshot capture IDs are not business-event keys.

## 4. Facts and measure contracts

Canonical hashing is versioned behaviour. Adding fields or changing normalization
must not silently change hashes of historical v1 events. Introduce a compatible
contract/topic version and conformed migration, then reconcile representations
before switching writers. Genuine source mutations remain conflicting immutable
identities; schema evolution must not be disguised as a source mutation.

### Learning activity

Grain: one original LMS micro-interaction, not a browser session or request.
Primary analytical identity: `event_id`; original `interaction_id` remains.
Foreign keys: `user_sk`, `course_sk`, `feature_sk`, `date_sk`, `time_sk`.
Exact `occurred_at` is retained in addition to its Vietnamese reporting date.

`event_count=1` is additive across compatible slices. `score` preserves the source
value; its scale/version is not known uniformly, so a cross-feature mean score is
not a governed metric. Explicit `quick_check_correct`/`quick_check_incorrect`
actions may support later correctness measures with a declared attempt grain.
Views are not completion, `ask_ai` is not a provider call, and activity counts
cannot establish study duration or platform login counts.

### Recommendation events

Grain: one original impression, click, accept, reject, dismiss, started or completed
event. `event_id` is the event key, while `recommendation_id` and
`recommendation_set_id` are attribution/context keys and may occur in many rows.
Rank, surface and entity identity preserve the publisher's known context.

`event_count=1` is additive by event type. Click-through and completion rates are
derived measures requiring eligible, observed denominators and an attribution
window. A conversational turn can expose several items; one turn is not one
recommendation impression. Conversational preference/candidate-set provenance
is a declared producer gap rather than a fabricated field.

### LLM attempts

Grain: one `llm_usage_log` row exported by the AI owner. `usage_id` and `event_id`
are keys; `request_id` can correlate multiple attempts and is not unique.
`attempt_count=1` counts recorded attempts, not user queries, turns or unique jobs.
Provider/model/task, success, retry number, fallback flag and a controlled failure
class retain their source meaning. Raw provider errors and key identifiers are
excluded before transport.

Reported prompt/completion tokens are additive; unknown usage stays null with
`usage_status='unknown'`. Legacy failed/unmeasured zero-token records do not become
known-zero usage. Zero/unavailable latency is exported as null. Percentiles need
underlying observations; averages are weighted by their observation counts.

The source has no historical price snapshot or reliable actor/course context.
Consequently `cost_amount`/`currency` and actor/course remain null, with explicit
availability status. Applying today's model catalog price to historical attempts
would fabricate cost history. Local/free-provider API spend is also different
from infrastructure cost. Versioned pricing and attribution must precede those
future measures.

## 5. Dimensions and keys

| Dimension | Natural key / scope | Surrogate key / attributes | Current history policy |
|---|---|---|---|
| `dim_user` | Global BDC user ID observed in backed events | SHA-256 of `bdc:user:<id>`; first/last observed times | Inferred member; names/roles unknown; no asserted SCD timeline |
| `dim_course` | LMS course ID observed in backed events | SHA-256 of `bdc:course:<id>` | Inferred member; title/publication state unknown |
| `dim_feature` | `lms:<action_type>` | SHA-256 of that controlled source feature code | Observed taxonomy; no invented catalog rows |
| `dim_date` | Vietnam reporting date | Integer `YYYYMMDD`, calendar/ISO year/week/month/quarter | Deterministic calendar attributes, observed dates only |
| `dim_time` | Local minute of day | Integer `hour*60+minute`, hour/minute | Deterministic minute attributes; facts retain exact timestamps |
| `dim_ai_model` | Provider + model name including explicit null handling | SHA-256 of canonical named JSON structure | Observed identity only; no price/model-version history inferred |

Null relationships are meaningful where context is absent. They are not replaced
with a fabricated user/course or arbitrary sentinel business record. Mandatory
learning relationships have explicit orphan checks. Nullable recommendation
course/model relationships are checked only when the source supplies the key.

Natural keys remain in facts so a later dimension enrichment can bind them.
First observed activity is not a signup, enrollment, publication or role-effective
date. Inferred members cannot answer historical role/category questions until
source-owned versions or snapshots exist.

## 6. SCD and future source snapshots

The target SCD2 policy applies only to attributes whose history changes analytical
interpretation: roles/status, course publication/owner/category and governed
model/pricing policy. Cosmetic corrections may use Type 1; calendar is Type 0.
Concurrent roles/organizations need effective-dated bridges, not one arbitrary
primary role copied into every fact.

An SCD2 version has scoped natural key, deterministic version key, authoritative
`effective_from`, exclusive `effective_to`, source version, attribute hash and
deletion/purpose status. Intervals may not overlap and there is at most one current
version per scoped key. A late source update requires affected historical joins
to be reconsidered; arrival time does not establish business effective time.
BigQuery does not enforce those integrity relationships for this model.

No current inferred dimension is labelled SCD2. Before implementing that upgrade,
onboard approved owner snapshots/outboxes, define historical consistency and erasure
rules, and settle persistent retention/materialization under the selected budget.

## 7. Quality and reproducibility

Run `python scripts/analyze_foundation.py --gcloud-bin <installed-gcloud>` for
aggregate source coverage and integrity checks. It does not emit user IDs or
learner records. Inspect counts for distinct fact keys, source hash conflicts,
mandatory relationship orphans, dimension key uniqueness and future event times.
An empty/unavailable source must not be presented as a business zero.

Unit tests isolate source contracts, allowlist privacy, multi-topic overfetch,
load acknowledgement, journal restart, snapshot hash/count validation, confirmed
snapshot dedupe and target-incarnation changes. Real deployment acceptance also
requires acknowledged real source batches and aggregate reconciliation; unit
fixtures are never loaded into the live warehouse.

Historical correction operates on source evidence and compatible forward models.
Do not manually edit an aggregate or overwrite source truth. Preserve model Git
revision, query/load job IDs, source coordinates/window and capture hash in the
receipt so another operator can reproduce and audit the result.

## 8. Deferred consumers

Data marts, OLAP/dashboard APIs, teacher/student interventions, recommender and
conversational recommender changes, training manifests and learnable-harness
adaptation are deliberately deferred. They will consume governed core models and
separately authorized content artifacts. No generated answer is a verified label
without explicit feedback/verifier provenance. See the
[foundation design](08-foundation-design.md) for the business and future-use contract.
