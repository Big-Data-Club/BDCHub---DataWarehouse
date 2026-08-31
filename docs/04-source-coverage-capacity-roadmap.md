# Source coverage, Kafka capacity và implementation roadmap

## 1. Inventory từ CoreApplication tại 2026-08-31

### Hạ tầng

- K3s single node.
- Kafka 3.7 KRaft, 1 broker, auto-create topic, default 3 partitions.
- Kafka heap 512 MiB–1 GiB; request 0.5 CPU/1 GiB; limit 2 CPU/2 GiB; PVC 10 GiB.
- Kafka internal replication factor 1: không chịu được broker/PVC loss.
- PostgreSQL production host là Neon pooled endpoint; nhiều logical DB cho Auth/LMS/Lab/Chat/AI.
- Personalize dùng DuckDB/PVC; không được xem là warehouse bền vững.

### Existing Kafka coverage

| Domain | Existing signals found | Gap |
|---|---|---|
| LMS | `learning-events`, `lms.analytics.interactions`, `lms.document.uploaded`, `lms.ai.command`, maintenance/graph commands | login/course enrollment/content lifecycle producer chưa đồng bộ chuẩn envelope |
| AI | `ai.document.processed.status`, `ai.job.status`, `ai.graph.status`, `ai.graph.node_merged` | per-user LLM context, embedding batches, studio usage |
| Lab | `lab.job.command`, `lab.job.status`, `lab.session.checkpoint`, `lab.session.idle` | documented analytics publication incomplete; submission/task completion envelope |
| Personalize | consumes analytics/login/course/recommender; publishes profile/notification | một số consumed topics chưa thấy producer đáng tin cậy |
| Recommender | `recommender.interactions.v1` | standardized impression→outcome attribution |
| Auth | notification consumer | login/session/security activity producer |
| Chat | DB/Redis realtime | metadata analytics producer |
| MCP | PostgreSQL `mcp_audit_log` | Kafka/outbox, latency/correlation/client/tool version |

## 2. Source-to-target coverage matrix

| Source entity/event | Capture | Warehouse target | Priority |
|---|---|---|---|
| Auth users/roles/org | CDC + snapshot | `dim_user`, membership bridge, `dim_organization` | P0 |
| Interactive login attempt | domain event/outbox | `fact_login_attempt` | P0 |
| Auth session lifecycle | event + state snapshot | `fact_session` | P1 |
| LMS course/section/content | CDC + domain publish events | content dimensions | P0 |
| Enrollment mutation | outbox | accumulating enrollment fact | P0 |
| Learning/micro interaction | existing event normalized | content/feature facts | P0 |
| Quiz/question/answer | outbox/CDC selected | quiz/question dims and attempt/response facts | P1 |
| Lab/lab version/task | CDC selected | lab dimensions | P0 |
| Lab session/submission/test result | existing + outbox | lab facts | P0 |
| HPC job/resource | runtime event | HPC fact | P1 |
| Chat channel/member/message metadata | outbox | chat facts/bridge | P1 |
| Recommendation impression/outcome | existing contract upgrade | recommendation facts | P1 |
| AI model/task config | CDC selected | AI dimensions | P0 |
| LLM attempt | source migration + outbox/CDC | LLM attempt fact | P0 |
| Agent turn/session/feedback | outbox/CDC selected | agent facts | P1 |
| Index/embedding/VLM/rerank | instrument job pipeline | index/embedding facts | P0 |
| Studio generation/export | instrument job pipeline | studio fact | P1 |
| MCP key lifecycle/tool call | outbox | MCP facts | P0 |
| Personalize skill/profile state | event + daily snapshot/export | skill evidence/snapshot | P1 |

## 3. Kafka capacity model

Không dùng estimate cố định nếu chưa đo. Dùng:

```text
ingress_bytes_day = events_day × avg_encoded_event_bytes × producer_overhead
retained_bytes    = ingress_bytes_day × retention_days × replication_factor
                     × compression_ratio × safety_factor
network_bytes_sec ≈ event_rate × avg_bytes × (producer + consumer copies)
```

Với broker hiện tại replication factor = 1. `compression_ratio` dùng kích thước sau nén (ví dụ 0.3–0.7), safety factor ít nhất 1.5 cho segment/index/retry/spike. Cộng internal topics và headroom; không cho Kafka PVC vượt 70% steady-state trên node từng DiskPressure.

### Planning scenarios — chỉ dùng để sizing trước khi có metrics

Giả định payload trung bình 1.5 KiB sau envelope, compression 0.5, safety 1.5, retention 3 ngày:

| Events/day | Avg rate | Peak 20× | Estimated retained analytics data |
|---:|---:|---:|---:|
| 100,000 | 1.2/s | 23/s | ~0.32 GiB |
| 1,000,000 | 11.6/s | 232/s | ~3.14 GiB |
| 5,000,000 | 57.9/s | 1,158/s | ~15.7 GiB |

Scenario 1M/day đã chiếm phần đáng kể PVC 10 GiB sau khi cộng existing topics/internal logs; do đó sampling/batching UI telemetry và giảm retention là bắt buộc trước scale. Throughput CPU không phải rủi ro đầu tiên; disk/retention và single-broker durability mới là rủi ro chính.

### Resource recommendation Phase 1

Không tăng Kafka một cách mù quáng. Trước tiên:

- bật exporter/JMX metrics: bytes in/out, messages in, request latency, under-replicated/offline partitions, log size;
- topic explicit config, không phụ thuộc auto-create default 3 partitions;
- analytics retention 24–72 giờ; DLQ 7 ngày với volume cap;
- LZ4/Zstd producer compression;
- consumer sink request `100m CPU/128 MiB`, limit `500m CPU/512 MiB`, benchmark rồi sửa;
- sink batch 500–5,000 events hoặc 1–5 phút, bounded memory;
- cảnh báo Kafka PVC 60/70/80%, lag age và disk growth slope;
- giữ ít nhất 25–30% root/PVC free; image/log cleanup không được xóa Kafka data.

Nếu measured sustained rate vượt khả năng hoặc cần HA, chuyển transport sang managed Kafka/Pub/Sub có ngân sách; không tăng retention trên root disk.

## 4. Storage budget model

So sánh free 10 GiB với **retained logical bytes**, không phải riêng “GB phát sinh mỗi tháng”. Ví dụ 3.7 GiB dữ liệu mới/tháng và giữ 12 tháng có thể thành ~44 GiB trước compression/expiration, vượt free storage.

Mỗi table phải có:

```text
rows/day × logical bytes/row × retention days
```

Budget Phase 1 đề xuất:

- raw 30 ngày, sampling high-volume events;
- core transaction facts 13 tháng;
- SCD dimensions theo policy;
- marts 25 tháng nếu nhỏ;
- chat/MCP/AI payload chỉ metadata;
- monthly job báo storage by table và forecast 30/90 ngày.

Nếu forecast vượt free tier: giảm raw retention, aggregate older facts, export cold Parquet sang storage có policy phù hợp, hoặc chấp nhận ngân sách trả phí. Không silent drop core facts.

## 5. Implementation phases

### Phase 0 — contracts và governance (repository này)

1. Chốt ADR, model, metric definitions, privacy/retention.
2. Đo baseline Kafka/DB/event volume 7 ngày.
3. Tạo GCP dev project, IAM, budgets, datasets bằng IaC.
4. Viết contract tests và source/target catalog.

Exit: architecture review accepted; không còn P0 data owner/privacy question.

### Phase 1 — vertical slice, không CDC

1. Deploy raw tables/governance tables.
2. Build lightweight sink với existing `lms.analytics.interactions` và `recommender.interactions.v1`.
3. Standardize event envelope and dedupe.
4. Build `dim_date`, `dim_feature`, minimal `dim_user/course` snapshots.
5. Produce one end-to-end mart and reconciliation dashboard.

Exit: replay test, duplicate test, quota failure test, source count reconciliation pass.

### Phase 2 — actor/context telemetry và outbox

1. Add Auth login/session event.
2. Add LMS enrollment/content outbox.
3. Add Lab submission/task/HPC events.
4. Add Chat metadata outbox.
5. Extend `llm_usage_log` context and MCP audit.
6. Implement AI/index/MCP facts and daily marts.

Exit: per-user AI/MCP cost traces from UI action → tool/job → every LLM attempt.

### Phase 3 — selective CDC/SCD2

1. Snapshot selected dimensions.
2. Enable Neon logical replication in controlled maintenance window.
3. Deploy one connector first; monitor scale-to-zero, egress and WAL.
4. Validate late/out-of-order SCD timeline.
5. Expand allowlist only after capacity/privacy review.

Exit: restore/slot loss/re-snapshot runbook tested.

### Phase 4 — complete marts and optimization

1. User/course/content/lab/chat/AI/MCP marts.
2. Cohort, funnel, retention, feature adoption and cost attribution.
3. Aggregate/expire based on measured cost.
4. Row/column security for instructor/org/admin analytics.

## 6. Acceptance criteria

- A user action is traceable by request/trace ID across domain, MCP/AI job and LLM attempts.
- Login exact time and daily login definition are independently testable.
- Query “user/course state at event time” resolves đúng SCD version.
- Duplicate/replay produces identical core result.
- Warehouse outage does not fail application requests.
- Kafka lag recovers before retention deadline under peak scenario.
- No secrets/raw restricted payload found by privacy tests.
- Every dashboard metric links to a versioned definition and owner.
- Daily reconciliation variance is within explicit threshold.
- Cost guardrails demonstrably prevent unbounded scans and stop pipeline on quota/budget signal.

## 7. Open inputs before production

Các input này không chặn việc viết foundation nhưng phải chốt trước Phase 1 go-live:

- GCP project/billing owner và organization IAM.
- Data retention/consent/legal basis, đặc biệt learner/chat/AI telemetry.
- Expected DAU, peak concurrent users và 7-day measured event volume.
- BigQuery region cuối cùng nếu data residency yêu cầu ngoài Singapore.
- Dashboard freshness SLO: 1, 5 hay 15 phút.
- RPO/RTO cho analytics và mức chấp nhận loss khi single Kafka broker hỏng.
- Model provider price source và currency conversion policy.
