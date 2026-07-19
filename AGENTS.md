# AGENTS.md — NYC High-Volume Ride-Hailing Lakehouse

## Source of truth

* Read `docs/PROJECT2_BLUEPRINT_FINAL.md` before editing.
* The blueprint overrides obsolete Instacart, ML, recommendation, MongoDB, and unrestricted query documentation.
* Report important conflicts before changing code.

## Dataset Contract

Required source dataset:

NYC TLC High Volume For-Hire Vehicle Trip Records.

Expected input:

monthly HVFHV parquet files.

Required source tables:

- fhvhv_tripdata_YYYY-MM.parquet
- taxi_zone_lookup.csv


Do NOT use:

- pre-aggregated trip summaries
- dashboard datasets
- ML feature datasets
- cleaned star schemas
- Kaggle transformed copies


Bronze layer must preserve source-level records.

## Delivery priority

The priority order is:

1. make Milestone A run for one month;
2. complete Milestone B for the junior Definition of Done;
3. implement Milestone C only when explicitly requested.

Do not begin schema evolution, snapshot pinning, compaction, full-year backfills, or advanced maintenance before the one-month Bronze -> Silver -> Gold -> DuckDB path works.

## Working mode

* Implement exactly one blueprint phase per run.
* Inspect relevant code, tests, and legacy paths before editing.
* Prefer small, reviewable diffs.
* Preserve the previous runnable path until the replacement passes.
* Do not touch unrelated files.
* Do not provision AWS resources without explicit user approval.

# Repository Cleanup Rules

Before implementing any new phase:

1. Inspect existing code, docs, configs and infrastructure files.

2. Identify:
   - obsolete implementations
   - previous dataset references
   - outdated architecture documents
   - unused dependencies
   - dead code
   - duplicate scripts
   - deprecated Docker files

3. Do not preserve old components only for historical reasons.

4. If a previous implementation conflicts with the current blueprint:
   - remove it,
   - archive it,
   - or replace it.

5. Keep repository clean:
   - no unused Java classes
   - no abandoned notebooks
   - no outdated README sections
   - no obsolete diagrams
   - no unused dependencies

6. Every phase report must include:

## Cleanup Report

Removed:
- file/path

Archived:
- file/path

Kept:
- file/path

Reason:
- why this artifact remains

## Locked architecture

```text
NYC TLC HVFHV monthly Parquet -> S3 -> Airflow 3 -> Glue/PySpark
-> Iceberg Bronze/Silver -> dbt Gold -> basic quality -> DuckDB read-only
```

Rules:

* Iceberg on S3 is canonical.
* DuckDB is a read-only consumer, not a second warehouse.
* Airflow orchestrates; transformation logic belongs in PySpark, dbt, or focused utilities.
* Junior Gold is limited to three dimensions, one fact, and two marts.
* Do not add ML, recommendation, MongoDB, PostgreSQL serving, ClickHouse, ScyllaDB, Redis, Elasticsearch, Trino, Kubernetes, dashboards, or unrestricted SQL endpoints.
* Do not add weather, traffic, maps, demographics, or route optimization.

## Resource limits

* The laptop must not run full Spark, Airflow Compose, or the AWS stack.
* Use deterministic fixtures for local tests.
* Use a disposable remote machine for DAG/dbt and small Spark integration tests.
* Use AWS trials/credits only after code, tests, Terraform validation, smoke scripts, and teardown instructions are ready.
* Start with one month, then three months; full 2024 is optional.

## Learning rule

For each phase:

1. identify one core decision the user must understand;
2. let Codex implement boilerplate and tests;
3. require the user to explain or manually verify that decision;
4. end with three teach-back questions and one failure/rerun experiment.

Core decisions include source grain, idempotency key, Bronze/Silver boundaries, quarantine, fact grain, Iceberg role, Airflow rerun semantics, and DuckDB's read-only role.

Do not block all implementation waiting for the user; clearly mark the single student learning task for the phase.

## Phase Gate Rule

A phase cannot start if previous phase report contains:

- NOT SATISFIED
- NOT VERIFIED
- FAILED

unless the user explicitly overrides.

## Verification

* Predict expected behavior before important tests.
* Run all credential-independent checks relevant to the diff.
* Report exact commands and concise results.
* Mark AWS, Glue, Airflow deployment, idempotency, schema evolution, snapshot pinning, performance, and recovery claims `NOT VERIFIED` until evidence exists.
* Never fabricate source counts, snapshots, query plans, costs, logs, or cloud resources.

Typical checks:

* Python/unit/fixture tests;
* Silver validation and reconciliation tests;
* Airflow 3 DAG import test;
* dbt parse/compile/build at the available level;
* one quality checkpoint on fixtures;
* DuckDB fixed-query tests;
* idempotent rerun test;
* Terraform `fmt -check` and `validate`.

## Data and security

* Do not commit NYC TLC monthly source files.
* Commit only small deterministic fixtures.
* Preserve source-faithful Bronze columns plus ingestion metadata.
* Invalid rows go to quarantine with reason codes; do not silently drop them.
* Keep README and diagrams aligned with implemented reality.
* Never commit AWS credentials, secrets, account IDs, private URLs, `.env`, or Terraform state.
* Do not claim `production-ready`, `deployed`, `idempotent`, `schema-evolved`, or `snapshot-pinned` without evidence.

## Required run report

End every run with:

1. phase implemented;
2. files changed;
3. commands and results;
4. verified acceptance criteria;
5. `NOT VERIFIED` items;
6. blockers and risks;
7. the student learning task;
8. three teach-back questions;
9. confirmation that no later phase was started.
