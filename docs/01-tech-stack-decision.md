# ADR-001: Serverless warehouse và processing stack

- Status: Accepted for Phase 0
- Date: 2026-08-31
- Owners: BDC Hub platform/data team
- Scope: analytical storage, ingestion, transformation, orchestration, catalog and cost controls

## 1. Context và ràng buộc thực tế

BDC Hub chạy nhiều service trên một node K3s. Kafka hiện là một KRaft broker, JVM heap `512 MiB–1 GiB`, request `0.5 CPU/1 GiB`, limit `2 CPU/2 GiB`, PVC `10 GiB`, replication factor `1`. Node từng gặp DiskPressure nên không được đặt warehouse database, Kafka Connect cluster hoặc object lake dài hạn lên node này.

Operational PostgreSQL đang dùng Neon qua pooled endpoint. Các domain chính là Auth, LMS, Lab, Chat và AI. Personalize có DuckDB cục bộ; Recommender phát Kafka event. Warehouse phải rẻ, serverless, tách tải khỏi K3s và vẫn hỗ trợ lịch sử SCD, semi-structured events, partition pruning và SQL chuẩn.

“0 đồng” được hiểu là **design target trong free allowance**, không phải SLA về giá. Free tier, currency, egress và quota có thể đổi; hệ thống phải có cảnh báo và kill switch.

## 2. Decision drivers

Theo thứ tự ưu tiên:

1. Không dùng disk/compute thường trực trên K3s.
2. Hỗ trợ dimensional model, window functions, `MERGE`, partition và clustering.
3. Có đường đi từ vài GB đến hàng trăm GB mà không redesign toàn bộ.
4. Chi phí ban đầu gần 0 và đo/khóa được chi phí.
5. Vận hành ít, backup/HA do cloud quản lý.
6. Ingest batch lẫn near-real-time, có idempotency và replay.
7. IAM, audit, row/column policy và tách PII.
8. Developer workflow bằng SQL, Git và CI.

## 3. Các lựa chọn

| Lựa chọn | Phù hợp | Không phù hợp | Kết luận |
|---|---|---|---|
| BigQuery | Serverless thật; columnar; partition/cluster; `MERGE`; free usage cho storage/query; load job shared pool miễn phí; IAM/policy tốt | Cần billing để dùng DML/streaming lâu dài; query/egress sai có thể phát sinh phí | **Chọn** |
| BigQuery Sandbox | Không cần thẻ; phù hợp POC | Table tự hết hạn sau 60 ngày; không streaming, DML hoặc Data Transfer Service; không chạy được SCD2 production | Chỉ demo |
| ClickHouse Cloud | OLAP realtime rất nhanh; MergeTree/materialized views mạnh | Cloud là trial/pay-as-you-go, không có cam kết perpetual free; mô hình/ops khác BigQuery | Không chọn ở giai đoạn 0đ |
| Neon/PostgreSQL | Đã quen; relational/SCD dễ viết | Free storage hiện nhỏ; row store không phải analytical warehouse; analytics tranh compute/egress với OLTP | Không dùng làm DW |
| DuckDB trên K3s | Đơn giản, SQL tốt | File locking, single-host disk và backup; đã từng gây xung đột lock; trái mục tiêu serverless | Chỉ local test |
| MotherDuck | DuckDB managed, DX tốt | Free limits và product dependency; governance/integration thấp hơn BigQuery cho mục tiêu hiện tại | Candidate cho prototype, không canonical target |

## 4. Quyết định

### 4.1 Warehouse

Dùng **BigQuery, location `asia-southeast1` (Singapore)** cho mọi dataset liên quan. Không trộn region vì cross-region load/transfer có thể phát sinh phí và tăng độ trễ.

Project/dataset đề xuất:

```text
GCP project: bdc-hub-analytics-<env>

bdc_raw_<env>         append-only event/CDC landing, expiration ngắn
bdc_staging_<env>     typed, deduplicated, quarantined
bdc_core_<env>        conformed dim/fact/bridge/snapshot
bdc_mart_<env>        dashboard-optimized aggregates
bdc_governance_<env>  metadata, watermarks, DQ results, deletion ledger
bdc_pii_<env>         restricted identity map when strictly needed
```

Không tạo dataset tên `dim`/`fact`: dim/fact là loại bảng trong `bdc_core`; dataset biểu diễn lifecycle/security boundary.

### 4.2 Ingestion

Phase 1 dùng một **lightweight sink** chạy một replica trên K3s:

- consume các Kafka topic đã có và outbox topic mới;
- buffer trong RAM có giới hạn, flush theo 1–5 phút hoặc batch size;
- ghi BigQuery qua Storage Write API committed/default stream hoặc batch load phù hợp;
- commit Kafka offset chỉ sau khi BigQuery xác nhận;
- dùng `event_id` để deduplicate vì crash giữa write và offset commit tạo duplicate.

Không chạy full Kafka Connect + bốn Debezium connector trên node ngay từ đầu. CDC được bật có chọn lọc ở Phase 2 sau benchmark và có watchdog cho replication slot/WAL.

BigQuery hiện công bố first 2 TiB/tháng Storage Write API gRPC miễn phí; batch load qua shared slot pool cũng miễn phí. Pipeline vẫn phải đọc pricing bằng IaC/runbook trước go-live vì chính sách có thể đổi.

### 4.3 Transformation

Dùng **Dataform Core SQLX** hoặc SQL chuẩn được chạy bởi GitHub Actions/Cloud Scheduler. Phase 1 ưu tiên SQL chuẩn + script mỏng để tránh lock-in framework. Transformations chạy theo DAG:

```text
raw → stg normalize/dedupe → conformed dimensions → facts → snapshots → marts
```

Mỗi model khai báo unique key, partition filter, owner, description, freshness và data tests. Không dùng Redis làm surrogate-key counter.

### 4.4 Orchestration

- Near-real-time ingest do consumer loop quản lý.
- Transform micro-batch theo watermark, mặc định 5 phút cho facts và 15 phút cho dimensions.
- Daily marts chạy sau khi đóng partition ngày với allowed lateness 48 giờ.
- GitHub Actions dùng cho deploy/schema validation, không dùng như daemon ingest.

### 4.5 Observability

OpenTelemetry/Prometheus metrics tối thiểu:

- records read/written/rejected/duplicated;
- bytes read/written và batch latency;
- Kafka lag per topic/partition;
- source event-time lag và warehouse freshness;
- BigQuery bytes processed, storage bytes, slot/load errors;
- CDC slot retained WAL bytes;
- DQ pass rate và quarantine count.

## 5. Cost guardrails

Không thể dùng budget alert như hard cap. Áp dụng đồng thời:

1. GCP budget alerts ở 25/50/75/90/100% và Pub/Sub notification.
2. Custom query quota theo project/user; `maximum_bytes_billed` cho mọi ad-hoc/scheduled query.
3. `require_partition_filter = true` cho fact lớn.
4. Raw expiration 30–90 ngày; staging 7–30 ngày; core theo policy.
5. Dashboard chỉ đọc marts, không quét raw.
6. Service account sink không có quyền query; transform account không có quyền quản lý IAM.
7. Cost anomaly job hằng ngày; kill switch dừng sink/transform nhưng không dừng ứng dụng nguồn.
8. Không lưu prompt, completion, chat content hoặc source file vào BigQuery mặc định.

## 6. Security và privacy

### Data classification

| Class | Ví dụ | Warehouse policy |
|---|---|---|
| Public | course title đã publish, feature catalog | Core/mart được phép |
| Internal | event type, latency, score, rank | Core, IAM nội bộ |
| Confidential/PII | email, display name, organization membership | Tách `bdc_pii`, column policy/masking |
| Restricted | password hash, bearer/API key, raw IP, chat body, prompt/response, learner code | Không ingest mặc định |

IP được HMAC với secret xoay vòng và chỉ giữ coarse geo nếu có consent. User agent được parse thành device/browser/os rồi raw value hết hạn sớm. Chat analytics chỉ giữ metadata (`char_count`, attachment/code flags), không giữ nội dung. MCP chỉ giữ tool, actor, scope, target, result class, latency; tuyệt đối không giữ API key hay arguments chứa nội dung nhạy cảm.

Để hỗ trợ xóa dữ liệu, facts dùng `user_id` pseudonymous ổn định, PII mapping tách riêng. Deletion ledger theo dõi request; pipeline xóa/tombstone identity và dữ liệu thuộc phạm vi policy trong raw/core/mart.

## 7. Consequences

### Positive

- K3s chỉ chịu một consumer nhỏ và Kafka retention ngắn.
- Warehouse scale độc lập và query không đè OLTP.
- SCD và replay thực hiện bằng SQL/version-controlled contracts.
- Có đường audit per-user/per-feature/per-model mà không lưu nội dung nhạy cảm.

### Trade-offs

- Billing account cần bật để có DML/streaming production.
- Eventual consistency 1–15 phút.
- At-least-once buộc mọi bảng có idempotency key.
- BigQuery không enforce PK/FK; data tests và pipeline phải bảo vệ integrity.
- CDC Neon giữ connection mở, ngăn scale-to-zero và tạo egress/WAL risk.

## 8. Rejected assumptions trong plan ban đầu

1. `GENERATE_UUID()` không thể nhét vào `INT64`; surrogate key dùng deterministic `STRING` hash hoặc UUID string.
2. `FARM_FINGERPRINT` là 64-bit và có collision risk; key canonical dùng SHA-256 trên natural key + effective time, giữ natural key để đối soát.
3. Không mặc định 3 partitions cho mọi topic. Topic low-volume dùng 1 partition; scale theo measured throughput/lag.
4. SCD2 và SCD4 không áp dụng đồng thời cho cùng một lịch sử role nếu không có mục đích riêng; chọn một canonical history.
5. `dim_ai_model` không thể Type 1 nếu muốn tính lại historical cost đúng giá tại thời điểm call. Model config/pricing là SCD2 hoặc snapshot giá trực tiếp trên fact.
6. Estimate “3.7 GB/tháng” phải dựa trên event volume × encoded row size × retention, không cộng monthly numbers rồi so trực tiếp với 10 GB persistent storage.

## 9. Official references

- [BigQuery pricing and free usage](https://cloud.google.com/bigquery/pricing)
- [BigQuery Sandbox limitations](https://cloud.google.com/bigquery/docs/sandbox)
- [Batch loading and pricing](https://cloud.google.com/bigquery/docs/batch-loading-data)
- [BigQuery Storage Write API](https://cloud.google.com/bigquery/docs/write-api)
- [Neon logical replication considerations](https://neon.com/docs/guides/logical-replication-neon)
- [Neon network transfer](https://neon.com/docs/introduction/network-transfer)
- [Debezium PostgreSQL connector and WAL risks](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
- [ClickHouse Cloud pricing](https://clickhouse.com/pricing)
