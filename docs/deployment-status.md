# Observed BigQuery rollout

Owner: BDC Hub platform/data team.
Observed on: 2026-10-21, with final source-to-mart verification at approximately
15:50 UTC. Re-run the metadata checks before relying on this snapshot later.

## Actual resources

- Project: `bdc-web-487000`, ACTIVE, authenticated access confirmed.
- Location: `asia-southeast1` for all five warehouse datasets.
- Billing: disabled, unchanged.
- Datasets: `bdc_raw_prod`, `bdc_staging_prod`, `bdc_core_prod`,
  `bdc_mart_prod`, `bdc_governance_prod`.
- Raw table: `bdc_raw_prod.lms_micro_interactions`.
- Views: `bdc_staging_prod.lms_micro_interactions`,
  `bdc_core_prod.fact_content_interaction`,
  `bdc_mart_prod.mart_course_daily_activity`,
  `bdc_governance_prod.lms_identity_conflicts`.

The raw table expires at **2026-12-20T15:10:44.388Z** according to BigQuery
metadata. View expirations are a few seconds later. This is a bounded Sandbox
slice, not permanent retained history. `warehouse.py verify` reports live expiry
rather than treating five hours of inactivity as an expired resource.

## Real source capture and load

The initial batch came from the existing `lms.analytics.interactions` Kafka topic.
The source snapshot contained 205 actual records from partition 0, offsets
`[3818, 4023)`. Capture used the existing Personalize runtime with no consumer
group and no Kafka offset commits. No source database was accessed.

The private capture was 92,398 bytes. Its SHA-256 matched across the authorized
SSH transfer. Learner records and credentials were not printed or committed.

BigQuery load job:

```text
bdc_lms_8cda9d9db10419db5646e7ea08fbe3e439352d86eabf23f882481190fa00a1a0
```

This is operational metadata, not an authentication token.

## Verified results

| Check | Actual result |
|---|---:|
| Source capture records | 205 |
| Validated records / acknowledged load rows | 205 / 205 |
| Raw deliveries for the captured coordinates | 205 |
| Acknowledged distinct Kafka coordinates | 205 |
| Source-to-raw payload hash mismatches | 0 |
| Captured distinct fact events | 205 |
| Conflicting event identities | 0 |
| Total fact events | 205 |
| Sum of daily mart interaction counts | 205 |
| Replay of the identical immutable file | Reused the acknowledged load job |
| Raw metadata row count after replay | 205 |

The aggregate reconciliation job was
`bdc_dw_7926c2fed838429abfa098325801740d`. Queries returned only counts.
These checks used the real source capture; no seeded test records were loaded.

## Scope of completion

The foundation, the first real source snapshot, batch load, logical transforms,
count/hash reconciliation, and immutable-batch retry path are implemented and
exercised. The Google CLI is installed outside `/tmp`; its saved login refreshed
without a new browser login during this rollout.

Continuous ingestion is **not running in the last verified rollout**. Keyless
runtime/deployment identities are provisioned; the worker, durable checkpoint
implementation, PVC manifest, and CI image build are implemented. The production
release `8f6fc8b18b989ae30edebbc8af4a961d03773a23` stopped at migration job
`bdc_release_378850ae39b14621a989779b79e51c7f`: bootstrap `CREATE SCHEMA` required
project-level `bigquery.datasets.create`, which the dataset-scoped deployment
identity intentionally lacks. Runtime migrations now target only tables/views
inside the five pre-provisioned datasets and fail closed if a dataset is missing.
A successful corrected rollout and real acknowledged worker batch are still
required before claiming continuous ingestion. Source-owned historical
reconciliation and agreed privacy/retention handling remain necessary.
The retained Kafka window does not recover all past LMS history or publications
that failed before reaching Kafka. Do not describe this one-time capture as a
complete historical or continuously updated warehouse.
