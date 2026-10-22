# Continuous LMS ingestion and deployment

Owner: BDC platform/data team. Source owner: LMS. Transport/operator owner: DevOps.
Use [deployment evidence](deployment-status.md) for observed runtime state; source
files alone do not prove that a release is deployed.

## Release path

1. Commit/publish warehouse code in its submodule repository.
2. Update the pinned `data-warehouse` commit in CoreApplication.
3. The parent `Data warehouse - verify, build and deploy` workflow tests the exact
   pinned source and verifies the locked runtime dependency export.
4. On main, a hosted runner builds an immutable Docker Hub image with provenance
   and SBOM. The deploy job consumes its digest, never a mutable `latest` tag.
5. The existing production runner applies only warehouse service accounts, identity
   routing ConfigMaps, and its checkpoint PVC. A separate migration Job completes
   schema/view changes before the worker Deployment is updated.
   Dataset creation is an operator bootstrap action (`ddl/001_foundation.sql`).
   Runtime releases require all five datasets to exist and execute only
   `ddl/002_runtime_models.sql` and the view definitions. They do not require
   project-level `bigquery.datasets.create`. Migration logs include a job ID before
   execution so failures can be inspected without exposing provider payloads.
6. Rollout checks pod readiness and exact source revision. Failure restores the
   previous warehouse pod specification when one exists. Schema changes must remain
   compatible; restoring a prior model release requires its migration image too.

Manual dispatch is supported on main. PR jobs test on hosted runners without
production credentials. Production jobs share the existing production concurrency
group. Warehouse-only/documentation changes no longer trigger the legacy workflow's
blanket application restarts; that path selects actual changed application owners.

## Keyless runtime identity

The K3s issuer's public JWKS is registered in workload identity pool `bdc-k3s`,
provider `warehouse`. Two exact Kubernetes subjects are allowed:

- `default/data-warehouse` impersonates `bdc-warehouse-ingest`. It has BigQuery Job
  User on the project and Data Editor only on the raw dataset.
- `default/data-warehouse-deployer` impersonates `bdc-warehouse-deploy`. It has
  Job User and Data Editor on the five warehouse datasets for migration jobs.

Projected audience-bound K3s tokens expire in one hour and are renewed by Kubernetes.
Google libraries exchange them for short-lived credentials. The committed external
account JSON files contain only public routing metadata and token-file references;
no service account keys, human refresh tokens, or passwords are deployed.

The operator bootstrap command is `scripts/bootstrap_workload_identity.py`, using
approved public issuer/JWKS files and an authorized Google CLI session. Re-run its
provider update when K3s signing keys rotate, before retiring the previous signer.
Do not copy K3s admin kubeconfig or private signing keys into the worker.

## Consumer and delivery contract

The original worker uses `lms.analytics.interactions` and group
`bdc-warehouse-lms-v1`. The platform-envelope configuration uses
`bdc-warehouse-platform-v1` for the implemented LMS and recommendation topics,
separate from application groups. It reports optional missing topics; permission
or metadata failures do not masquerade as absent sources. One Recreate
replica and a PVC file lock enforce one writer. Explicit assignment is captured at
startup; changes to topic partitions require a controlled restart.
Partition discovery uses the broker's topic-specific Admin metadata response;
listing topic names does not populate an unsubscribed consumer's partition cache.

Each batch is bounded by 1,000 records/1 MiB and a five-minute flush interval.
Actual source event IDs, event times, and Kafka coordinates are preserved. No
sample records or fabricated historical dimensions are inserted. A new group can
replay retained real events; raw deliveries can repeat while conformed facts remain
unique by stable source identity and payload hash.

Platform cutover uses a separate journal directory on the existing PVC and refuses
startup if the legacy journal still has a pending batch. Journal identity binds
the group, target and table incarnation. Legacy raw evidence remains available
through reconciled learning staging/core views; its expiry is unchanged. Rollback
preserves the new journal for explicit recovery rather than resetting its group.

Payload bytes and the next offset frontier are stored atomically in a private
SQLite journal with full synchronous durability. The worker then submits a stable
BigQuery batch-load job. Only a successful job with the expected output row count
allows an acknowledgement, Kafka commit, and journal removal. A crash during
upload or before commit recovers the same job and batch. Fetched records beyond a
batch boundary are rewound on every partition, including unprocessed partitions.

The one-shot capture's previous load job is not a continuous consumer checkpoint.
Initial replay is legitimate source replay, not an artificial user action. Marts
deduplicate the source identity and do not count a replay as a new interaction.

## Failure and operating limits

- Google errors retain the pending journal and do not advance offsets. Raw provider
  messages and source records are not logged.
- Invalid/oversized source records block the partition and readiness until an
  explicit correction/quarantine procedure exists; they are never silently skipped.
- A checkpoint outside Kafka retention fails closed. Source-owned reconciliation or
  retained approved exports are required for a recovery gap.
- The spool is bounded and only one batch is pending. The PVC is 256 MiB, not an
  unbounded object store or a backup; node/PVC loss still affects recovery.
- Load attempts have a durable daily limit of 1,000 and uploads are spaced by at
  least sixty seconds. Do not raise cadence to evade BigQuery load quotas.
- Sandbox expiration is unchanged. An old successful job for a previous raw-table
  incarnation cannot acknowledge the recreated table. Expiry requires deliberate
  restoration/replay, not automatic offset advancement.
- Shutdown has a five-minute grace period. A remaining durable pending batch is
  recovered on the next start. Pod restart is not permission to reset offsets.

The internal HTTP endpoints are `/health/live`, `/health/ready`, and `/status` on
port 8080. Status includes revision, partition count, source lag, pending records,
acknowledged row/batch counts, last acknowledgement, and raw-table expiry. There is
no public ingress and status contains no learner records or credentials.

## Source-owned AI usage snapshots

`data-warehouse-ai-snapshot` runs at minute 17 every six hours UTC with Kubernetes
`concurrencyPolicy: Forbid`. It uses the narrow ingest identity and a separate
private 256 MiB state PVC. The owning AI endpoint is
`GET /ai/internal/warehouse/llm-attempts`; it requires the existing internal secret,
accepts a 1–60 day window, and exports at most 10,000 metadata rows per snapshot.
The manifest starts with a seven-day overlapping window. This is a bounded
capture, not an all-time export or low-latency AI event feed.

Count and cursor reads share one read-only repeatable-read transaction. Header
and trailer establish expected count and canonical payload hash. Partial streams,
oversized windows, invalid contracts and missing trailers cannot be acknowledged.
The collector caps line/total bytes, row count and network/query duration. It
never receives source database credentials or prints source records.

Only confirmed identities/hashes enter a local seen cache. Overlapping exports
skip confirmed unchanged rows instead of repeatedly loading the same seven days.
Conflicting versions still land for governance inspection. Upload failure retains
the journal; restart recovers the same BigQuery job. An acknowledged batch
interrupted before cache update/finish is also recoverable. Cache/journal identity
includes target incarnation, preventing reuse after table recreation. Late commits
inside the overlap are rediscovered; older transactions require owner backfill.
Serial IDs are not treated as commit watermarks.

Deploy the AI endpoint before manually starting the first snapshot Job. A failed
scheduled Job remains failure with pending state; it is not zero AI activity.
Reduce the window or implement the planned outbox before exceeding these bounds.
Measure source load, raw growth and replay capacity before raising limits.

## Verification

Run the warehouse recovery tests, the parent legacy-selection tests, and Kustomize
validation before publishing. Inspect the real workflow run, immutable digest,
migration Job, pod readiness/version, Kafka group offset, and BigQuery counts.
A green no-change workflow or ready pod before its first batch is not proof of
end-to-end ingestion. Verify a real acknowledged batch and subsequent checkpoint
recovery without inserting artificial events.
