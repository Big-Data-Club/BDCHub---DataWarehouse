# BigQuery setup and current live-data foundation

Owner: BDC Hub platform/data team.

## Current configuration

The selected project is `bdc-web-487000`, location `asia-southeast1`, with billing
**disabled at the project owner's request**. See
[the active decision](../../docs/adr/2026-10-21-bigquery-billing-disabled-foundation.md).
Only real BDC source events are eligible for ingestion. No sample rows, generated
users, invented dimensions, or public demo datasets are loaded.

The implementation entry points are:

- [Project configuration](../config/project.json)
- [Foundation DDL](../ddl/001_foundation.sql)
- [Staging, fact, mart, and quality views](../transforms/001_lms_views.sql)
- [Deployment and verification CLI](../scripts/warehouse.py)
- [Bounded source capture](../scripts/export_lms_kafka.py)
- [Batch loader](../scripts/load_lms_batch.py)
- [Source-to-mart reconciliation](../scripts/reconcile_lms_batch.py)
- [Observed rollout](deployment-status.md)

## Authentication and recovery

Install the official [Google Cloud CLI](https://docs.cloud.google.com/sdk/docs/install)
in a persistent location and put `gcloud` on `PATH`. Authentication stays in the
user's Google CLI configuration outside the repository. Never place a password,
OAuth code, access token, service-account key, or live capture in Git.

```bash
gcloud auth login --project=bdc-web-487000
python3 data-warehouse/scripts/warehouse.py check
```

If the CLI is not on `PATH`, supply `--gcloud-bin` with its executable path. The
CLI uses Google CLI credentials and automatic token refresh; it does not require
separate ADC login. Other future Google client-library applications may require
[ADC](https://docs.cloud.google.com/bigquery/docs/authentication).

`check` reads the project state, billing state, BigQuery access, and target dataset
locations. It prints metadata only. A short-lived access token expiring does not
mean the stored login has expired; Google CLI normally refreshes it. If refresh
fails, authenticate again. A removed executable in `/tmp` requires restoring the
CLI, not inventing replacement data or deleting warehouse resources.

## Deploy or inspect existing resources

Render the exact SQL before applying if review is needed:

```bash
python3 data-warehouse/scripts/warehouse.py render
python3 data-warehouse/scripts/warehouse.py apply
python3 data-warehouse/scripts/warehouse.py verify
```

Run commands from the parent CoreApplication checkout. From the standalone
warehouse checkout, use `python3 scripts/warehouse.py ...` instead.

The deployment creates these datasets, all in Singapore:

| Dataset | Implemented resource |
|---|---|
| `bdc_raw_prod` | `lms_micro_interactions`, partitioned by actual ingestion time |
| `bdc_staging_prod` | Deduplicated `lms_micro_interactions` view |
| `bdc_core_prod` | `fact_content_interaction` view |
| `bdc_mart_prod` | `mart_course_daily_activity` view |
| `bdc_governance_prod` | `lms_identity_conflicts` view |

The `prod` suffix identifies the source application data. It is not a claim of
high availability, permanent history, or a deployed continuous consumer.

Dataset creation and raw-table creation use `IF NOT EXISTS`. Applying the model
replaces the named views but does not seed, truncate, or replace the raw table.
Existing datasets in another region cause deployment to stop. Every query job has
a configured byte limit. `verify` reads metadata and dry-runs the mart dependency
chain without returning learner rows.

A multi-statement deployment can leave earlier successful DDL in place if a later
statement fails. The CLI prints each real job ID before submission. Inspect an
interrupted job before retrying. Do not delete datasets or replay inputs to hide
a partial deployment.

## First real source

The implemented source contract is
[LMS MicroInteractionEvent](../../lms-service/pkg/kafka/events.go), published by
[the owning service](../../lms-service/internal/service/micro_interaction_service.go)
on `lms.analytics.interactions`. When viewed from the warehouse submodule, these
source links refer to sibling paths in the parent checkout.

The capture command runs inside the existing Personalize runtime, using its broker
configuration and installed `aiokafka`. It captures a finite start/end-offset
snapshot with no consumer group and no offset commits. It does not read the LMS
database or change application consumers. Capture records are projected to the
approved source fields; arbitrary payload metadata and content are excluded.
Unsafe values fail the batch rather than being printed or silently discarded.

Store captures in private temporary directories outside the repository. Transfer
them through the authorized SSH path, not chat or application logs. The loader
validates source IDs, timestamps, Kafka coordinates, field allowlists, and payload
hashes before making a batch-load job:

```bash
python3 data-warehouse/scripts/load_lms_batch.py /path/to/private/real-capture.ndjson
```

That path must be an actual previously captured source file. The loader does not
generate records. An empty capture is not submitted. It derives a deterministic
load-job ID from target and immutable file bytes; rerunning that exact file checks
the existing acknowledgement instead of appending it twice. Keep the original
capture if acknowledgement is uncertain. No Kafka offset is committed by this
initial one-shot path.

Reuse also checks the raw table's actual creation time. A load acknowledged before
an expired table was recreated cannot acknowledge rows in its replacement; the
loader assigns a new job identity for that new table incarnation.

Verify source-to-warehouse counts without printing learner records:

```bash
python3 data-warehouse/scripts/reconcile_lms_batch.py /path/to/private/real-capture.ndjson
```

The source's current publication is best effort after a durable LMS write.
Kafka-retained events are not the complete historical database. A source-owned
export/outbox/reconciliation capability is needed for history outside Kafka
retention or failed source publications.

## Sandbox limits and expiry

Google's [Sandbox documentation](https://docs.cloud.google.com/bigquery/docs/sandbox)
excludes streaming, DML (`INSERT`, `UPDATE`, `DELETE`, `MERGE`), and Data Transfer
Service. Tables, views, and partitions expire after sixty days. Current storage
and query allowances must be checked against that documentation; deletion does
not necessarily replenish the lifetime sandbox storage allowance.

This implementation therefore uses batch load jobs and logical SQL views, not
streaming, scheduled transfers, or SCD2 mutations. Reporting uses retained raw
history, Vietnam reporting dates, and real source keys. A long-term SCD warehouse
remains future work under the original architecture contract.

Check metadata expiry with `verify` before assuming a resource must be recreated.
If an expired raw table is recreated, it starts empty and must be restored from
retained approved source data. Reapplying DDL cannot recover expired history, and
this implementation does not pretend to extend Sandbox into permanent storage.

Runtime IAM, recurring scheduling, durable acknowledgements/backups, erasure, and
historical reconciliation must be completed before treating this as a continuous
production analytics service. No billing or IAM grant is changed by the CLI.
