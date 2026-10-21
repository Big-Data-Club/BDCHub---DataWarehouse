# Data warehouse modelling

Owner: BDC Hub platform/data team, with domain owners defining business semantics.
Status: target model; tables below are planned unless implementation is linked.

The initial implementation is a typed raw LMS table and logical
[staging/fact/mart/quality views](../transforms/001_lms_views.sql). It preserves
source keys and does not create the planned dimensions or persisted SCD2 history.
The [current setup](05-bigquery-setup.md) describes the billing-disabled constraints.

## 1. Modelling method

Use Kimball-style dimensional modelling with conformed dimensions and approved raw
history for replay/audit. Define each fact's grain before its columns. Keep distinct
business processes in separate facts and serve dashboards from curated models.

| Model | Purpose | Example |
|---|---|---|
| Dimension | Descriptive context with an explicit history policy | User, course, model price |
| Transaction fact | One immutable business event | Login attempt, quiz submission, LLM attempt |
| Accumulating snapshot | One lifecycle updated at milestones | Enrollment, AI job |
| Periodic snapshot | State at a declared time/grain | User-course daily progress |
| Factless fact | Relationship or event without numeric measures | Membership, feature exposure |
| Bridge | Many-to-many or hierarchical relationships | Concurrent user-role membership |
| Junk dimension | Related low-cardinality flags | Controlled status flags |
| Degenerate dimension | Business identifier retained in a fact | Request, job, session ID |
| Mart | Rebuildable aggregate for a specific use case | Daily course activity |

Declare measure additivity: counts and costs may sum across compatible grains;
progress/mastery snapshots do not sum across time; distinct users and percentiles
cannot be combined by adding daily results. Aggregate facts to a compatible grain
before joining them to avoid fan-out and duplicate costs.

## 2. Conformed dimensions

### SCD conventions

Each SCD2 dimension has:

```text
<entity>_sk STRING             deterministic SHA-256 surrogate key
<entity>_id STRING/INT64       natural source key
effective_from TIMESTAMP      inclusive
effective_to TIMESTAMP        exclusive; NULL means open
is_current BOOL
source_updated_at TIMESTAMP
record_source STRING
attribute_hash STRING
dw_created_at TIMESTAMP
dw_updated_at TIMESTAMP
```

There is at most one current row per scoped natural key, and effective intervals
must not overlap. A business delete normally creates a tombstone version using
`is_deleted`; privacy erasure follows the separate deletion policy. Include
source/tenant scope in keys where natural identifiers can collide.

### Dimension catalog

| Dimension | SCD | Historical attributes | Type 1/exclusion policy |
|---|---|---|---|
| `dim_user` | Hybrid 2/1 | Account role/status, organization, tier, onboarding level/goal | Cosmetic name/avatar fixes are Type 1; isolate email mapping |
| `dim_organization` | 2 | Plan, status, type | Cosmetic branding is Type 1 |
| `dim_course` | 2 | Owner, status, visibility, category, difficulty, language, major content version | Cosmetic title corrections may be Type 1 |
| `dim_section` | 2 | Course, order, publication state | Exclude content bodies |
| `dim_content` | 2 | Section, type, publish/index state, version, source type | Exclude raw text/files |
| `dim_knowledge_node` | 2 | Label, type, status, version | Keep merge/split lineage in a bridge |
| `dim_quiz` | 2 | Settings, chapter, availability | Attempts belong in facts |
| `dim_question` | 2 | Type, difficulty, Bloom level, version | Answer text requires restricted policy |
| `dim_lab` | 2 | Version, runtime, difficulty, resource/time limits, active state | Exclude hidden test cases |
| `dim_feature` | 2 | Feature/tool code, surface, owner, billable class | Shared taxonomy across services |
| `dim_ai_task` | 2 | Category, billable actor, async flag, policy | Preserve semantic changes |
| `dim_ai_model_price` | 2 | Provider, model, modality, price, currency, validity | Resolve prices at call time |
| `dim_mcp_tool` | 2 | Tool, version, risk, read/write, scopes | Cosmetic renames may be Type 1 |
| `dim_device` | 1 | Normalized browser, OS, device | Minimize raw user-agent retention |
| `dim_geo` | 1 | Coarse country/region | No precise location by default |
| `dim_date` | 0 | Calendar, ISO week, governed Vietnamese holidays | Generated and calendar-governed |
| `dim_time` | 0 | Minute/hour/daypart | Facts still retain exact timestamps |
| `dim_event_status` | Junk | Controlled success/failure/retry/fallback flags | Low-cardinality combinations |

Do not duplicate the same role timeline in both a separate history table and
`dim_user`. Concurrent roles/organizations require an effective-dated
`bridge_user_role_membership`; account-level status stays in the user dimension.

## 3. Fact catalog and exact grains

### Identity and activity

| Fact | Type | Grain | Measures |
|---|---|---|---|
| `fact_login_attempt` | Transaction | One authentication attempt, successful or failed | Latency, success, failure class |
| `fact_session` | Accumulating | One authenticated application session | Start/end, duration, refresh count, last activity |
| `fact_feature_interaction` | Transaction | One semantic action on a feature/surface | Duration, count, result class |
| `fact_page_view` | Transaction | One meaningful page/screen view | Finalized dwell time |
| `fact_user_feature_daily` | Periodic | User × feature × reporting date | Uses, successes/failures, active seconds |

A token refresh is not a new interactive login. Retain exact login `occurred_at`
and date/time keys; derive daily login counts using the agreed reporting timezone.
Unknown/failed authentication can have no resolved user; do not fabricate one.

### LMS, content, and courses

| Fact | Type | Grain | Measures |
|---|---|---|---|
| `fact_enrollment_lifecycle` | Accumulating | User × enrollment | Enrollment/start/completion/drop times, progress |
| `fact_content_interaction` | Transaction | One action on content/lesson/node | Duration, progress delta, completion, score |
| `fact_quiz_attempt` | Transaction | One submitted quiz attempt | Score, duration, pass flag, question counts |
| `fact_question_response` | Transaction | Attempt × question | Correctness, score, response time, hint use |
| `fact_flashcard_review` | Transaction | One card review | Rating, interval/ease changes |
| `fact_forum_interaction` | Transaction | One post/comment/vote event | Count, character count, no body |
| `fact_course_daily_snapshot` | Periodic | Course × reporting date | Enrollments, active users, completions, content count |
| `fact_user_course_daily` | Periodic | User × course × reporting date | Time, interactions, progress, mastery delta |

### Labs, HPC, and terminal sessions

| Fact | Type | Grain | Measures |
|---|---|---|---|
| `fact_lab_session` | Accumulating | One user lab session | Start/end/idle/runtime, checkpoints |
| `fact_lab_submission` | Transaction | One submission | Score, passed/total cases, CPU/memory/wall time |
| `fact_lab_test_result` | Transaction | Submission × test case | Pass flag, runtime, memory, no hidden expected output |
| `fact_lab_task_attempt` | Transaction | Runtime task × attempt | Completion, score contribution, verifier type |
| `fact_hpc_job` | Accumulating | One scheduler/container job | Queue/start/end, exit, CPU/GPU/memory-seconds |
| `fact_terminal_command_summary` | Periodic | Session × minute/task | Command count, verified task evidence |

Completion requires domain verifiers/checkpoints, not elapsed terminal-open time.
Raw shell commands and learner source code are restricted and excluded by default.

### Chat and social activity

| Fact | Type | Grain | Measures |
|---|---|---|---|
| `fact_chat_message` | Transaction | One message metadata event | Character count, attachment count, code flag |
| `fact_chat_reaction` | Transaction | One user-message-reaction mutation | Count, add/remove flag |
| `fact_channel_membership` | Factless/accumulating | One user-channel membership interval | Join/leave times |

Exclude message content and attachment URLs from core analytics. Content-based
research needs a separately scoped dataset/project and explicit consent/retention.

### Recommendation and personalization

| Fact | Type | Grain | Measures |
|---|---|---|---|
| `fact_recommendation_impression` | Transaction | User × recommendation item × impression | Rank, score, count |
| `fact_recommendation_outcome` | Transaction | One click/dismiss/enrollment outcome | Delay from attributed impression |
| `fact_skill_evidence` | Transaction | One evidence event for a user-skill pair | Weight, score, mastery delta |
| `fact_user_skill_daily` | Periodic | User × skill × reporting date | Mastery, confidence, evidence counts |

Keep recommendation/set IDs for attribution. Outcomes can repeat or arrive late;
do not overwrite an impression with a single assumed outcome.

### AI, LLMs, indexing, and Studio

| Fact | Type | Grain | Measures |
|---|---|---|---|
| `fact_llm_call_attempt` | Transaction | One provider API attempt | Input/output/cached tokens, latency, cost, result/fallback |
| `fact_agent_turn` | Transaction | One user turn in an agent session | Tool/LLM counts, tokens, latency |
| `fact_agent_session` | Accumulating | One teacher/mentor session | Turns, duration, tokens, feedback |
| `fact_ai_job` | Accumulating | One asynchronous generation job | Requested/generated/rejected/published items, duration, tokens |
| `fact_embedding_batch` | Transaction | One provider/local embedding batch | Items, input tokens, dimensions, duration/cost |
| `fact_index_job` | Accumulating | One indexing/reindexing job | Chunks, images, embedding batches, retries, cancellation |
| `fact_studio_generation` | Accumulating | One Studio generation/export job | Slides/assets, LLM/image calls, duration/cost |
| `fact_ai_feedback` | Transaction | One feedback event on AI output | Like/dislike/rating, no free text by default |

Retain provider/model/pricing snapshots and the effective price key on each LLM
attempt. Each retry is a separate row. Derive request-level fallback from the
attempt chain or mark `is_fallback_attempt`. Count all attempts for cost, but
count a logical user action once. Nullable usage is preferable to a fabricated
zero when a failed provider call has no usage report.

Local embeddings can have estimated token counts. Retain `input_chars`,
`estimated_tokens`, `token_count_method`, compute time, and `is_estimate`.

### MCP

| Fact | Type | Grain | Measures |
|---|---|---|---|
| `fact_mcp_tool_call` | Transaction | One invocation | Latency, bytes/items, result, read/write class |
| `fact_mcp_session` | Accumulating | One client connection/session | Calls, tools, duration, failures |
| `fact_mcp_resource_access` | Transaction | One resource read/list | Results/bytes, success |

Link actor, key surrogate, client, resource scope, trace/request, and downstream
job/LLM IDs. Domain facts count mutations; MCP facts describe their channel.
Joining these must not multiply business counts or provider costs.

## 4. SCD2 processing

### Deterministic keys

For a single-source numeric user ID, an illustrative key is:

```sql
TO_HEX(SHA256(CONCAT(
  'user|', CAST(user_id AS STRING), '|',
  FORMAT_TIMESTAMP('%FT%H:%M:%E6SZ', effective_from, 'UTC')
))) AS user_sk
```

For multi-source/string keys, include source/tenant and canonicalize components
with unambiguous encoding, explicit null handling, and stable timestamp precision.
Keep natural keys even when hashing. Do not maintain surrogate sequences in Redis.

### Timeline reconstruction

A matched `MERGE` source row cannot both close its old version and insert a new
version by relying on two matched actions. More importantly, late changes can
split a historical interval rather than simply close the current row.

1. Deduplicate changes by scoped natural key and source version/LSN.
2. Read the existing historical timeline for affected keys plus incoming changes.
3. Resolve equal effective timestamps using authoritative source ordering.
4. Apply the declared Type 1 correction policy; hash only Type 2 attributes.
5. Collapse consecutive identical Type 2 states.
6. Compute exclusive `effective_to` with `LEAD(effective_from)`.
7. Atomically replace/merge the affected key timelines with a concurrency policy.
8. Validate one current row, positive intervals, and no overlap.
9. Rebind facts whose historical surrogate key changed and refresh affected marts.

Resolve fact keys using both the scoped natural key and the event-time interval:

```sql
event.user_id = dim.user_id
AND event.occurred_at >= dim.effective_from
AND event.occurred_at < COALESCE(dim.effective_to, TIMESTAMP '9999-12-31')
```

When a dimension arrives later, retain the fact's natural key and a durable unknown
surrogate. Distinguish `unknown` from `not_applicable`, track unresolved keys,
and rerun late binding when context becomes available.

## 5. Illustrative LLM attempt table

```sql
CREATE TABLE bdc_core_prod.fact_llm_call_attempt (
  llm_call_sk STRING NOT NULL,
  source_log_id INT64,
  request_id STRING,
  parent_request_id STRING,
  attempt_no INT64 NOT NULL,
  user_sk STRING,
  user_id INT64,
  course_sk STRING,
  course_id INT64,
  ai_task_sk STRING,
  task_code STRING NOT NULL,
  model_price_sk STRING,
  session_id STRING,
  job_id STRING,
  tool_name STRING,
  trigger_type STRING NOT NULL,
  provider_code STRING,
  model_name STRING,
  input_tokens INT64,
  cached_input_tokens INT64,
  output_tokens INT64,
  total_tokens INT64,
  usage_status STRING NOT NULL,
  latency_ms INT64,
  estimated_cost NUMERIC,
  cost_currency STRING,
  cost_is_estimate BOOL,
  success BOOL NOT NULL,
  is_fallback_attempt BOOL NOT NULL,
  error_class STRING,
  called_at TIMESTAMP NOT NULL,
  ingested_at TIMESTAMP NOT NULL,
  record_source STRING NOT NULL
)
PARTITION BY DATE(called_at)
CLUSTER BY user_id, task_code, model_name
OPTIONS (require_partition_filter = TRUE);
```

Suggested `usage_status` values are `REPORTED`, `ESTIMATED`, and `UNKNOWN`.
Validate their semantics in the event contract. Raw provider errors may echo keys
or prompts; core keeps only a controlled `error_class`. Sanitize before landing.

## 6. Marts

| Mart | Grain/purpose |
|---|---|
| `mart_user_daily_activity` | User × date; login/session/LMS/Lab/Chat/AI/MCP measures |
| `mart_user_ai_daily` | User × date × task/provider where defined |
| `mart_course_daily` | Course × date; acquisition, engagement, completion, quality |
| `mart_content_funnel_daily` | Content × date; impression/start/completion/assessment |
| `mart_lab_daily` | Lab × date; sessions, submissions, pass rate, resources |
| `mart_ai_platform_daily` | Task × model × provider × date; usage, cost, latency, failure |
| `mart_mcp_daily` | Tool × client × date; calls, results, latency |
| `mart_data_quality_daily` | Source × date; freshness, rejection, reconciliation |

Marts are rebuilt from core; they do not rebuild source dimensions/facts. Task
codes are row dimensions, not a new physical column per task. Define the timezone
and grain of each date and avoid joining several marts without aligning grains.

## 7. Physical design and retention

These are proposed defaults, subject to volume, policy, and budget review:

| Layer | Partition | Clustering candidates | Retention |
|---|---|---|---|
| Raw events | Ingestion date | Event type, source | 30–90 days |
| Staging | Event/source update date | Natural key, type | 7–30 days |
| Transaction facts | Event date | Common selective filters | 13–25 months |
| SCD dimensions | Usually unpartitioned when small | Scoped natural key, current flag | Business/privacy policy |
| Daily snapshots/marts | Reporting date | User, course, feature | 13–25 months |
| Restricted identity map | Update date if useful | Analytics user key | Minimum required |

Partition only when scale and access patterns justify it. Choose ordered clustering
columns from actual filters; avoid duplicating an already adequate date partition
key without evidence. Nested/repeated fields can help bounded one-to-many detail,
but reusable conformed entities stay in dimensions. See
[optimization guidance](06-data-engineering-optimization.md) for measurement criteria.

## 8. Metric contracts

Each metric declares owner, grain, numerator/denominator, qualifying filters,
timezone, allowed lateness, attribution window, and version. Examples:

- DAU counts distinct authenticated users with qualifying activity, excluding
  refresh/background polling. It is not a sum of feature-level distinct users.
- Daily login counts successful interactive authentication, excluding refresh.
- Feature use follows a semantic attempt/success event, not an HTTP 200 alone.
- User AI cost includes attributable provider attempts; platform/system cost is separate.
- Completion follows a verified domain outcome, not time spent with a terminal open.
- Course progress is an end-of-day snapshot, not a sum of percentages.

## 9. Data tests

Check unique current dimension keys, non-overlapping/positive intervals, fact
identity uniqueness, nonnegative reported usage/duration/cost, provider-specific
token arithmetic, dimension resolution SLOs, attempt identity/order, enrollment
milestones, source-to-staging/core reconciliation, and restricted-data exclusion.

Use scenario-based cases: a duplicate replay, a late dimension split, facts before
dimensions, repeated outcomes, several LLM attempts for one request, unknown usage,
and deletion followed by replay. Check complete result equivalence, not only row
counts, when validating an optimization or backfill.
