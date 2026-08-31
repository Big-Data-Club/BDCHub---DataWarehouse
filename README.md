# BDC Hub Data Warehouse

Kho thiết kế và, ở các phase tiếp theo, mã nguồn cho Data Warehouse của BDC Hub.

Mục tiêu là tạo một lớp dữ liệu phân tích có lịch sử, có thể trả lời được:

- ai đã làm gì, ở đâu, khi nào và kết quả ra sao trên Auth, LMS, Lab, Chat, AI, Personalize, Recommender và MCP;
- trạng thái user/course/content/lab tại đúng thời điểm sự kiện xảy ra;
- mức sử dụng từng tính năng, phiên đăng nhập, funnel học tập và cohort;
- token, latency, model, fallback và chi phí của từng LLM call, AI job, indexing job, agent session và MCP tool call;
- số liệu vận hành có thể đối soát với database nguồn mà không biến warehouse thành source of truth.

## Trạng thái

Repository đang ở **Phase 0 — architecture contract**. Chưa có pipeline nào được triển khai hoặc kết nối production.

Các tài liệu nền tảng:

1. [Tech stack decision](docs/01-tech-stack-decision.md)
2. [ETL/ELT and CDC pipeline](docs/02-etl-cdc-pipeline.md)
3. [Dimensional modelling](docs/03-data-warehouse-modelling.md)
4. [Source coverage, Kafka capacity and rollout](docs/04-source-coverage-capacity-roadmap.md)

## Quyết định tóm tắt

- Target: **Google BigQuery**, dataset tại `asia-southeast1`, billing-enabled free tier với budget/alert/quotas. BigQuery Sandbox chỉ dùng demo, không dùng production.
- Pattern: **ELT**, raw append-only → staging → conformed dimensions/facts → marts.
- Delivery: domain event + transactional outbox cho nghiệp vụ mới; CDC có chọn lọc cho dimension/backfill/reconciliation.
- Ingestion ban đầu: micro-batch 1–5 phút; không dựng Kafka Connect/Debezium nặng trên node K3s đang thiếu disk.
- Semantics: at-least-once transport + idempotent event keys + deterministic `MERGE`, tạo hiệu quả exactly-once ở warehouse.
- Privacy: raw chat body, source code, bearer/API key, prompt/response và IP nguyên bản không được đưa vào analytics warehouse mặc định.
- Cost: “0 đồng” là budget objective, không phải guarantee. Pipeline phải dừng/fail closed trước khi vượt guardrail.

## Nguyên tắc bất biến

1. PostgreSQL của service vẫn là operational source of truth.
2. Kafka là transport/replay buffer, không phải kho lưu lịch sử vĩnh viễn.
3. Raw zone bất biến; transformation có thể chạy lại.
4. Mọi fact phải có grain, event time, ingestion time, source và idempotency key.
5. Không suy diễn `user_id` của AI call bằng timestamp; context phải được truyền xuyên suốt request/job.
6. SCD chỉ áp dụng khi lịch sử thuộc tính có giá trị phân tích; không SCD2 mọi cột.
7. Schema, PII classification, retention và owner phải được khai báo trước khi onboard nguồn mới.

## Cấu trúc dự kiến

```text
data-warehouse/
├── docs/                    # ADR, modelling, runbook
├── contracts/               # JSON Schema/Protobuf event contracts
├── ddl/                     # BigQuery datasets/tables/policies
├── transforms/              # SQL/Dataform transformations and tests
├── sink/                    # lightweight Kafka -> BigQuery loader
├── migrations/              # source telemetry/outbox migrations
└── tests/                   # contract, data quality, replay tests
```

## Điều kiện bắt đầu Phase 1

- Có GCP project, billing account, budget alert và service account tối thiểu quyền.
- Chốt retention theo lớp dữ liệu và chính sách consent/xóa dữ liệu.
- Đo Kafka bytes-in, bytes-out, consumer lag, PVC used bytes và event rate ít nhất 7 ngày.
- Bổ sung actor/context cho `llm_usage_log` và telemetry cho MCP trước khi ingest.
- Xác nhận location của BigQuery và các thành phần trung gian cùng region.
