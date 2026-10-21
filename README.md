# BDC Hub Data Warehouse

Owner: BDC Hub platform/data team.

This directory contains warehouse architecture, deployment code, and the initial
live-data BigQuery models. The warehouse is intended to
answer historical questions across Auth, LMS, Lab, Chat, AI, Personalize,
Recommender, and MCP:

- Who performed an action, on which resource, when, and with what result?
- What was the user, course, content, or lab state when the action occurred?
- How do feature adoption, interactive logins, learning funnels, and cohorts evolve?
- What tokens, latency, models, retries, fallbacks, and costs belong to each LLM
  attempt, AI/indexing job, agent session, and MCP tool call?
- Can analytical totals be reconciled with their owning operational services?

## Implementation status

**Initial live-data foundation:** project `bdc-web-487000`, region
`asia-southeast1`, with billing disabled at the owner's request. Five datasets,
one raw LMS table, and four logical staging/core/mart/quality views were deployed
and verified on 2026-10-21. Source capture and batch-load commands are implemented;
the first real batch of 205 LMS events has been loaded and reconciled. Recurring
ingestion, historical backfill, and a persistent SCD2 model are not deployed. See
[observed deployment evidence](docs/deployment-status.md).

The active [billing-disabled decision](../docs/adr/2026-10-21-bigquery-billing-disabled-foundation.md)
supersedes ADR-001's billing assumption for this project. Resources expire under
Sandbox rules; this initial slice must not be presented as permanent history.
Only real source captures are loaded, without sample rows or invented dimensions.

The existing DuckDB/Parquet pipeline in `personalize-service` is a separate,
implemented analytics/profile boundary. See the
[Data Platform guide](../docs/DATA_PLATFORM.md) for its current contracts.

## Reading order

| Document | Purpose |
|---|---|
| [Technology decision](docs/01-tech-stack-decision.md) | Platform choice, constraints, security, and cost controls |
| [Pipeline design](docs/02-etl-cdc-pipeline.md) | Events, outbox, selective CDC, ingestion, replay, and failures |
| [Warehouse modelling](docs/03-data-warehouse-modelling.md) | Fact grains, dimensions, SCD, marts, and metric contracts |
| [Coverage and rollout](docs/04-source-coverage-capacity-roadmap.md) | Source gaps, sizing, implementation phases, and acceptance criteria |
| [BigQuery setup](docs/05-bigquery-setup.md) | Active project, authentication, deployment, real source capture, and recovery |
| [Engineering optimization](docs/06-data-engineering-optimization.md) | Measurement, ingestion, SQL, storage, and recovery trade-offs |
| [Continuous deployment](docs/07-continuous-deployment.md) | Keyless identities, durable worker, CI/CD, retries, and runtime verification |

For agent-assisted design and implementation, use the repository
[`bdc-data-engineering` skill](../.agents/skills/bdc-data-engineering/SKILL.md).

## Architecture decisions

- Use Google BigQuery in `asia-southeast1`. The active project keeps billing
  disabled and uses batch loads/logical views with Sandbox retention limits.
  The broader billing-enabled architecture remains a future option, not an action
  authorized by current project setup.
- Use ELT: append-only raw landing, typed staging, conformed dimensions/facts,
  and purpose-built marts. Apply validation and privacy filtering before raw landing.
- Use domain events and transactional outbox for new business mutations;
  selective CDC supports dimensions, backfills, and reconciliation in Phase 3.
- Start with a lightweight Kafka consumer and 1–5 minute micro-batches. Avoid
  adding Kafka Connect/Debezium to a disk-constrained K3s node before measurement.
- Accept at-least-once delivery; produce repeatable core results through stable
  event keys, source versions, and deterministic transformations.
- Exclude raw chat, learner code, bearer/API keys, prompts/responses, and raw IP
  addresses from default warehouse ingestion.
- Treat near-zero cost as a budget objective. Alerts alone cannot enforce a
  spending cap; use quotas, bounded queries, retention, and pipeline stop controls.

## Invariants

1. Each service remains the owner and operational source of truth for its data.
2. Kafka is a transport/replay buffer, not permanent historical storage.
3. Raw records are immutable under normal processing; controlled privacy erasure
   and retention still apply. Transformations must be reproducible.
4. Every fact declares its grain, event time, ingestion time, source, and idempotency key.
5. AI attribution must propagate actor/context through requests and jobs; never
   infer a user from timestamp proximity.
6. Apply SCD only where attribute history has analytical value.
7. Declare schema, privacy class, retention, and ownership before onboarding a source.

## Implementation and planned layout

`config/`, `ddl/`, `scripts/`, and `transforms/` now contain the initial implementation.
Additional contracts and automated service components will be added when needed.

```text
data-warehouse/
├── docs/                    # architecture decisions, modelling, setup, runbooks
├── config/                  # project/mode configuration, no credentials
├── contracts/               # versioned event schemas
├── ddl/                     # BigQuery datasets, tables, and policies
├── transforms/              # SQL/Dataform models and data tests
├── scripts/                 # deployment, real source capture, batch load
├── sink/                    # lightweight Kafka-to-BigQuery loader
└── tests/                   # contract, quality, replay, and integration tests
```

Operational migrations belong in the owning service's migration directory, not
in the warehouse directory.

## Before the Phase 1 production rollout

- Assign a GCP project/billing owner; configure budgets and workload identities.
- Agree on layer retention, access, consent, and erasure requirements.
- Measure Kafka traffic, lag, event rate, and PVC usage for at least seven days.
- Keep related datasets and any intermediate storage in the selected region.
- Validate one end-to-end mart using existing LMS/recommendation events.
- Complete actor/context and MCP telemetry work before onboarding those sources;
  this does not block the initial LMS/recommendation development slice.
