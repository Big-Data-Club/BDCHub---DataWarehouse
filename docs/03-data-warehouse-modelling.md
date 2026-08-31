# Data Warehouse modelling

## 1. Modelling method

BDC Hub dùng **Kimball-style dimensional model** với conformed dimensions, đồng thời giữ raw immutable để replay/audit. Mỗi fact xác định grain trước khi liệt kê cột. Không tạo một “fact_everything” khổng lồ và không join các operational schema trực tiếp từ dashboard.

Các loại bảng:

- **Dimension**: context mô tả who/what/where; có SCD policy rõ ràng.
- **Transaction fact**: một event bất biến, ví dụ login attempt, quiz submission, MCP call.
- **Accumulating snapshot fact**: một lifecycle được cập nhật qua nhiều milestone, ví dụ enrollment hoặc AI job.
- **Periodic snapshot fact**: trạng thái chụp theo ngày, ví dụ user-course daily activity/mastery.
- **Factless fact**: quan hệ/sự kiện không có measure, ví dụ feature exposure hoặc membership.
- **Bridge**: many-to-many, hierarchy hoặc multi-valued relationship.
- **Junk dimension**: gom các cờ/low-cardinality status dùng chung, tránh hàng chục tiny dimensions.
- **Degenerate dimension**: ID nghiệp vụ giữ ngay trên fact như request_id/job_id/session_id.
- **Mart**: aggregate phục vụ use case, có thể rebuild từ core.

## 2. Conformed dimensions

### SCD conventions

Mọi SCD2 dimension có:

```text
<entity>_sk STRING            deterministic SHA-256 surrogate key
<entity>_id STRING/INT64      natural key từ source
effective_from TIMESTAMP      inclusive
effective_to TIMESTAMP        exclusive, NULL = open
is_current BOOL
source_updated_at TIMESTAMP
record_source STRING
attribute_hash STRING
dw_created_at TIMESTAMP
dw_updated_at TIMESTAMP
```

Invariant: tối đa một current row/natural key; intervals không overlap. Delete tạo tombstone/version với `is_deleted`, không xóa lịch sử ngay trừ privacy erasure.

### Danh mục dimensions

| Dimension | SCD | Tracked history | Type 1 / excluded rationale |
|---|---|---|---|
| `dim_user` | Hybrid 2/1 | role, account status, organization, tier, onboarding level/goal | display name/avatar typo Type 1; email nằm PII map, không nhân bản mọi version |
| `dim_organization` | 2 | plan/status/type | cosmetic branding Type 1 |
| `dim_course` | 2 | owner, status, visibility, category, difficulty, language, major content version | typo title có thể Type 1 nếu product chốt cosmetic |
| `dim_section` | 2 | course, order, publish state | — |
| `dim_content` | 2 | section, type, publish/index state, version, source type | raw text/file excluded |
| `dim_knowledge_node` | 2 | label/type/status/version | merge/split lineage ở bridge riêng |
| `dim_quiz` | 2 | settings, publish chapter, availability | attempts không nằm dimension |
| `dim_question` | 2 | type, difficulty, Bloom level, version | answer text có policy restricted |
| `dim_lab` | 2 | version, runtime, difficulty, resource/time limits, active state | hidden testcase excluded |
| `dim_feature` | 2 | canonical feature/tool code, surface, owner, billable class | đảm bảo LMS/Lab/AI/MCP dùng chung taxonomy |
| `dim_ai_task` | 2 | category, billable actor, async flag, policy | task semantics thay đổi phải có history |
| `dim_ai_model_price` | 2 | provider/model/modality/price/currency/effective period | cần historical cost đúng thời điểm |
| `dim_mcp_tool` | 2 | tool/version/risk/read-write/scopes | rename cosmetic Type 1 nếu identity không đổi |
| `dim_device` | 1 | normalized browser/os/device | raw user-agent hết hạn sớm |
| `dim_geo` | 1 | country/region coarse | không lưu precise location |
| `dim_date` | 0 static | calendar, ISO week, VN holiday | generated/calendar-governed |
| `dim_time` | 0 static | minute/hour/daypart | event vẫn giữ timestamp chính xác |
| `dim_event_status` | junk | success/failure/retry/fallback flags | low-cardinality flags |

Không dùng `bridge_user_role_history` song song với full SCD2 `dim_user` nếu chỉ lặp cùng lịch sử. Nếu một user đồng thời có nhiều role/org, dùng factless `bridge_user_role_membership` với effective interval; account-level role/status vẫn ở `dim_user`.

## 3. Fact catalogue và grain

### Identity và activity

| Fact | Type | Exact grain | Measures |
|---|---|---|---|
| `fact_login_attempt` | Transaction | một authentication attempt, thành công hoặc thất bại | latency, success, failure class |
| `fact_session` | Accumulating | một authenticated application session | start/end, duration, refresh count, last activity |
| `fact_feature_interaction` | Transaction | một semantic user action trên feature/surface | duration, count=1, result class |
| `fact_page_view` | Transaction | một meaningful page/screen view | dwell time when finalized |
| `fact_user_feature_daily` | Periodic | user × feature × date | uses, success/failure, active seconds |

`JWT_REFRESH` không được tính là login mới. Login fact giữ exact `occurred_at`, `date_sk`, `time_sk`; daily count là mart từ successful interactive login theo policy.

### LMS/content/course

| Fact | Type | Exact grain | Measures |
|---|---|---|---|
| `fact_enrollment_lifecycle` | Accumulating | user × enrollment | enrolled/started/completed/dropped times, progress |
| `fact_content_interaction` | Transaction | một action trên content/lesson/node | duration, progress delta, completion, score |
| `fact_quiz_attempt` | Transaction | một submitted quiz attempt | score, duration, passed, question counts |
| `fact_question_response` | Transaction | attempt × question | correct, score, response time, hint use |
| `fact_flashcard_review` | Transaction | một card review | quality/rating, interval/ease changes |
| `fact_forum_interaction` | Transaction | post/comment/vote event | count, char count; no raw body |
| `fact_course_daily_snapshot` | Periodic | course × date | enrollments, active users, completions, content count |
| `fact_user_course_daily` | Periodic | user × course × date | time, interactions, progress, mastery delta |

### Lab/HPC/terminal

| Fact | Type | Exact grain | Measures |
|---|---|---|---|
| `fact_lab_session` | Accumulating | một user lab session | start/end/idle/runtime, checkpoint count |
| `fact_lab_submission` | Transaction | một submission | score, passed cases, total cases, CPU/memory/wall time |
| `fact_lab_test_result` | Transaction | submission × testcase | pass/fail, runtime, memory; no hidden expected output |
| `fact_lab_task_attempt` | Transaction | runtime task × attempt | completion, score contribution, verifier type |
| `fact_hpc_job` | Accumulating | một scheduler/container job | queue/start/end, exit, CPU/GPU/memory seconds |
| `fact_terminal_command_summary` | Periodic | session × minute/task | command count, task evidence; no raw command by default |

Terminal completion dựa trên explicit runtime tasks/checkpoints/verifier, không dựa vào việc terminal mở đủ lâu. Raw shell command là restricted và không vào warehouse mặc định.

### Chat/social

| Fact | Type | Exact grain | Measures |
|---|---|---|---|
| `fact_chat_message` | Transaction | một message metadata event | char count, attachment count, code flag |
| `fact_chat_reaction` | Transaction | user × message × reaction mutation | count=1, add/remove |
| `fact_channel_membership` | Factless/accumulating | user × channel membership interval | joined/left time |

Content/body/attachment URL không vào core analytics. Nếu research cần NLP trên chat, tạo project/dataset riêng, consent và retention riêng.

### Recommendation/personalization

| Fact | Type | Exact grain | Measures |
|---|---|---|---|
| `fact_recommendation_impression` | Transaction | user × recommendation item × impression | rank, score, count=1 |
| `fact_recommendation_outcome` | Transaction | click/dismiss/enroll outcome | delay from impression |
| `fact_skill_evidence` | Transaction | một evidence event cho user × skill | evidence weight, score, mastery delta |
| `fact_user_skill_daily` | Periodic | user × skill × date | mastery, confidence, evidence counts |

Impression set ID và recommendation ID là degenerate dimensions để attribution. Không gộp click vào cùng row impression nếu outcome có thể nhiều lần/đến trễ.

### AI/LLM/indexing/studio

| Fact | Type | Exact grain | Measures |
|---|---|---|---|
| `fact_llm_call_attempt` | Transaction | một provider API attempt | input/output/cached tokens, latency, cost, success/fallback |
| `fact_agent_turn` | Transaction | một user turn trong agent session | tool count, LLM calls, tokens, latency |
| `fact_agent_session` | Accumulating | một teacher/mentor session | turns, duration, tokens, feedback |
| `fact_ai_job` | Accumulating | một async generation job | requested/generated/rejected/published items, duration, tokens |
| `fact_embedding_batch` | Transaction | một embedding provider/local batch | items, input tokens, vector dimensions, duration/cost |
| `fact_index_job` | Accumulating | một content indexing/reindex job | chunks, images, embedding batches, retries, cancellation |
| `fact_studio_generation` | Accumulating | một studio project generation/export | slides/assets, LLM/image calls, duration/cost |
| `fact_ai_feedback` | Transaction | one user feedback on AI output | like/dislike/rating; no text by default |

`fact_llm_call_attempt` chứa snapshot provider/model/price/currency để audit, đồng thời có SK tới `dim_ai_model_price`. Retry là row riêng. `fallback_used` nên được suy ra từ attempt chain hoặc ghi cả `is_fallback_attempt`; không cộng một request nhiều lần khi tính user actions nhưng phải cộng mọi attempt khi tính cost.

Embedding local không nhất thiết có tokenizer usage chính xác: giữ `input_chars`, `estimated_tokens`, `token_count_method`, CPU/GPU duration và `is_estimate`.

### MCP

| Fact | Type | Exact grain | Measures |
|---|---|---|---|
| `fact_mcp_tool_call` | Transaction | một MCP tool invocation | latency, bytes/items, success, risk/write flag |
| `fact_mcp_session` | Accumulating | một client connection/session | calls, tools, duration, failures |
| `fact_mcp_resource_access` | Transaction | một resource read/list | result count/bytes, success |

MCP call link đến actor user, API key surrogate (không token), client, course/target, trace/request và downstream LLM/job IDs. Write mutation vẫn được tính ở domain fact; MCP fact cho biết channel/automation attribution, tránh duplicate business count.

## 4. SCD2 processing

### Deterministic key

```sql
TO_HEX(SHA256(CONCAT(
  'user|', CAST(user_id AS STRING), '|',
  FORMAT_TIMESTAMP('%FT%H:%M:%E6SZ', effective_from)
))) AS user_sk
```

Không dùng sequence trong Redis. Collision của SHA-256 được xem là negligible; natural key vẫn được giữ.

### Timeline algorithm

Một BigQuery `MERGE` đơn giản chỉ đóng row hiện tại rồi insert row mới thường sai với late-arriving update và không thể vừa update vừa insert từ cùng matched source theo cách plan mẫu mô tả. Pipeline chuẩn:

1. Dedupe source change theo natural key + source version/LSN.
2. Lấy timeline bị ảnh hưởng từ current dimension + staging.
3. Chuẩn hóa Type 1 fields theo latest known value.
4. Tính `attribute_hash` chỉ từ Type 2 fields.
5. Collapse consecutive duplicates.
6. Dùng `LEAD(effective_from)` để tạo `effective_to` exclusive.
7. `MERGE`/replace chỉ các natural keys bị ảnh hưởng trong một transaction/script.
8. Chạy tests one-current/no-overlap.

Fact lookup dùng event time:

```sql
event.occurred_at >= dim.effective_from
AND event.occurred_at < COALESCE(dim.effective_to, TIMESTAMP '9999-12-31')
```

Nếu dimension đến sau fact, fact giữ natural key và `unknown` SK; job late-binding cập nhật SK sau. Mỗi dimension có durable `unknown`, `not_applicable` records.

## 5. Example core table: LLM attempt

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
  input_tokens INT64 NOT NULL,
  cached_input_tokens INT64,
  output_tokens INT64 NOT NULL,
  total_tokens INT64 NOT NULL,
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

Không lưu `error_message` thô ở core vì provider message có thể echo prompt/key. Raw sanitized error hết hạn ngắn; core chỉ giữ controlled `error_class`.

## 6. Marts

Marts không phải source để rebuild dimensions/facts:

- `mart_user_daily_activity`: user × date, login/session/LMS/Lab/Chat/AI/MCP measures.
- `mart_user_ai_daily`: user × date × task/provider optional drilldown.
- `mart_course_daily`: course × date, acquisition/engagement/completion/quality.
- `mart_content_funnel_daily`: content × date, impression → start → complete → assess.
- `mart_lab_daily`: lab × date, sessions/submissions/pass/time/resource.
- `mart_ai_platform_daily`: task/model/provider/day cost, token, latency, failure/fallback.
- `mart_mcp_daily`: tool/client/day calls, write/read, failure, latency.
- `mart_data_quality_daily`: source freshness, rejection and reconciliation.

Không tạo một cột cho từng task code trong platform mart; task code là row dimension để thêm task mới không cần schema migration. Dashboard pivot khi cần.

## 7. Partition, clustering và retention

| Layer | Partition | Cluster | Retention default |
|---|---|---|---|
| Raw event | ingestion date | event type, source | 30–90 ngày |
| Staging | event/source update date | natural key/type | 7–30 ngày |
| Transaction facts | event date | 2–4 common filters | 13–25 tháng theo policy |
| SCD dimensions | effective date nếu lớn; unpartitioned nếu nhỏ | natural key, current | theo business/privacy policy |
| Daily snapshots/marts | activity date | user/course/feature | 13–25 tháng |
| Restricted PII | update date | user id | tối thiểu cần thiết |

Không partition bảng nhỏ chỉ để “đúng mẫu”; partition overhead và filter complexity có thể không đáng. Không cluster theo `DATE(timestamp)` vì partition đã xử lý date.

## 8. Metrics definitions bắt buộc

Một metric contract phải có owner, grain, numerator/denominator, filters, timezone, late-data window và version. Ví dụ:

- DAU: distinct authenticated users có qualifying activity, không phải token refresh/background polling.
- Daily login: successful interactive authentication, không tính refresh.
- Feature used: semantic success/attempt event, không suy ra chỉ từ HTTP 200.
- AI user cost: tổng mọi provider attempt attributable cho user; platform/system cost tách riêng.
- Completion: domain-defined verified completion; terminal idle/time-open không đủ.
- Course progress: snapshot cuối ngày, không sum progress percentages.

## 9. Data tests

- `dim_*`: unique current natural key, no interval overlap, effective_to > effective_from.
- facts: event ID/source key unique after conformance.
- non-negative token/duration/cost/count.
- `total_tokens` reconcile with token components under provider-specific rule.
- user/course SK resolution rate ≥ agreed SLO; unresolved queue visible.
- attempt numbers monotonic within request/job.
- enrollment milestones ordered.
- raw-to-staging and staging-to-core row reconciliation by date/source.
- restricted-pattern scanner on raw payload and error fields.
