# Phases 6–7 Report — Terraform, CI, cloud runbook, and lifecycle contracts

Date: 2026-07-19

## Phases implemented

The user explicitly requested Phases 6 and 7 together, overriding the
one-phase-per-run wording and the previous report's cloud `NOT VERIFIED`
gate. Both requested phases were implemented; no work beyond Phase 7 was
started.

Phase 6 replaces the obsolete Instacart/ML Terraform with a minimal NYC-only
S3, IAM, Glue Catalog, and Glue-job stack; adds credential-independent GitHub
Actions checks; and provides a bounded cloud evidence and teardown runbook.
No AWS resource was provisioned, and no cloud demo was claimed.

Phase 7 adds remote-only contracts for one-time 2025 schema evolution, exact
six-table snapshot manifests and pinned-reference handoff, threshold-based
compaction decisions, retention dry runs, and orphan-file dry runs. These
contracts do not execute lifecycle mutation locally.

## Files changed

Added:

- `terraform/main.tf`
- `terraform/variables.tf`
- `terraform/s3.tf`
- `terraform/glue_catalog.tf`
- `terraform/iam.tf`
- `terraform/glue_jobs.tf`
- `terraform/terraform.tfvars.example`
- `.github/workflows/ci.yml`
- `docs/CLOUD_DEMO_RUNBOOK.md`
- `docs/CLOUD_EVIDENCE_TEMPLATE.md`
- `etl/iceberg/lifecycle.py`
- `etl/glue_jobs/nyc_schema_evolution_2025.py`
- `tests/unit/test_iceberg_lifecycle.py`
- `docs/PHASE_6_7_REPORT.md`

Updated:

- `etl/iceberg/__init__.py`
- `docs/DATASET_NOTES.md`
- `README.md`

Archived under `legacy/terraform_instacart/`:

- former `main.tf`, `variables.tf`, `s3.tf`, `glue_catalog.tf`, `glue_jobs.tf`,
  `iam.tf`, `ml_jobs.tf`, and `terraform.tfvars.example`.

## Phase 6 contract

The active Terraform stack owns only:

- one private, versioned, encrypted S3 bucket with public access blocked;
- one NYC Glue Catalog database;
- one least-scoped Glue service role/policy for the bucket and catalog;
- four Phase 5 Glue jobs (initializer, Bronze, Silver, quality), plus the
  explicitly isolated 2025 schema-evolution job.

The stack uses `force_destroy = false`, so teardown cannot silently recurse
through canonical data. CI runs Python compile/tests, credential-independent
dbt parse, Terraform format, and backend-free validation. It does not contain
AWS credentials or an apply step.

The operator procedure, evidence fields, budget warning, and safe teardown
sequence are in `docs/CLOUD_DEMO_RUNBOOK.md`; no account, bucket, snapshot,
cost, log, or teardown result has been fabricated.

## Phase 7 contract

`etl/iceberg/lifecycle.py` provides pure contracts for:

- a 2025-or-later Bronze `cbd_congestion_fee DECIMAL(18,2)` evolution plan,
  restricted to the known Bronze table;
- a manifest containing exactly one positive snapshot ID for each six Gold
  tables, serialized deterministically;
- an explicit ID lookup for a future catalog-specific pinned query adapter;
- compaction only when file count exceeds the configured threshold and average
  file size is below the configured threshold;
- retention eligibility that always keeps the newest minimum number of
  snapshots and can only produce a dry-run candidate list;
- orphan-file candidates as a sorted list with no delete operation.

`etl/glue_jobs/nyc_schema_evolution_2025.py` is remote-only and executes the
planned DDL only after an approved 2025 source run. It does not claim that the
current local DuckDB connector can query a pinned Iceberg snapshot.

## Commands and results

Expected behavior before verification: all pure lifecycle boundaries should
pass deterministic tests; the active Terraform should format and validate
without an AWS backend; CI YAML should parse; dbt graph parsing should remain
green; no local test should start Spark, Airflow services, or AWS.

| Command | Result |
| --- | --- |
| `python -m pytest -p no:cacheprovider tests\unit tests\contract -q` | PASS: 44 passed in 2.25 s. |
| `python -m compileall -q consumer etl tests` | PASS (exit 0); Python reported an inaccessible pre-existing dbt package directory but did not fail compilation. |
| `dbt parse --profiles-dir . --target local_parse --no-partial-parse` with local placeholder variables | PASS (exit 0), dbt 1.11.12 / dbt-spark 1.10.3. |
| `terraform fmt -check` over the seven active `.tf` files | PASS. |
| First `terraform fmt -check -diff` attempt | NOT a formatting result: Windows lacks the optional `diff` executable; rerun without `-diff` passed. |
| `terraform init -backend=false -input=false` | NOT VERIFIED: provider registry access was blocked by sandbox network policy. No apply was attempted. |
| `terraform validate` using the existing local provider cache | PASS with one warning from the pre-existing ignored `terraform.tfvars` containing obsolete `s3_raw_prefix`; no configuration error. |
| CI YAML parse with PyYAML | PASS. |
| Terraform scope scan for Instacart/MongoDB/ML/Kaggle references | PASS: none in active Terraform `.tf` or example variables. |
| Lifecycle unit tests | PASS within the 44-test suite: schema-plan bounds, manifest completeness/stability, compaction thresholds, retention dry-run, and orphan listing. |

## Verified acceptance criteria

- Minimal NYC Terraform files are active and the obsolete Instacart/ML stack is
  archived.
- S3 teardown is guarded by `force_destroy = false`.
- CI covers local Python/tests, dbt parse, and Terraform checks without cloud
  credentials.
- A bounded-cloud runbook and evidence template explicitly preserve
  `NOT VERIFIED` until outputs exist.
- 2025 schema evolution is constrained to the known Bronze table and new field.
- Snapshot manifests require all six Gold tables and positive exact IDs.
- Compaction is conditional, retention is dry-run-only, and orphan detection
  only lists candidates.
- Existing Phase 1–5 tests and dbt parse remain green.

## NOT VERIFIED

- Terraform provider installation in a clean environment, AWS credentials,
  `terraform plan/apply/destroy`, S3, IAM, Glue Catalog, Glue jobs, CloudWatch,
  cost, recovery, and teardown.
- GitHub Actions execution, hosted-runner dependency installation, and CI
  status checks.
- A real 2025 Parquet upload, Iceberg schema evolution, resulting table schema,
  snapshot IDs, manifest object, pinned DuckDB snapshot query, file metrics,
  compaction, snapshot expiration, and orphan-file dry run against S3.
- Any destructive retention, compaction, or orphan-file operation.
- Airflow runtime behavior, dbt-glue build/test, and three-/four-month cloud
  backfill evidence.
- Performance, recovery, snapshot pinning support in the selected DuckDB
  catalog path, and exact costs.

## Blockers and risks

- The sandbox cannot reach registry.terraform.io, so clean-provider Terraform
  initialization is not locally verified. The existing local plugin permitted
  validation.
- The ignored workspace `terraform.tfvars` still contains a legacy undeclared
  `s3_raw_prefix`; it was not edited because it is user state. Fresh CI checkouts
  do not contain that file.
- The Airflow 3.3.0 environment remains independently broken at import time
  (`ObjectStoragePath` missing from `airflow.sdk`), as recorded in Phase 5.
- The lifecycle module intentionally avoids inventing catalog-specific snapshot
  syntax or automatic deletion semantics. A remote Iceberg/DuckDB connector
  decision is required before pinned querying or maintenance.

## Cleanup Report

Removed:

- Active Instacart/ML Terraform sources and example variables from the
  `terraform/` module.
- The unused active Terraform documentation had already been archived in Phase
  4; its replacement is the NYC cloud runbook and evidence template.

Archived:

- Old Terraform `.tf` sources and `terraform.tfvars.example` under
  `legacy/terraform_instacart/`.

Kept:

- Ignored `terraform.tfstate`, backup state, plan, private `terraform.tfvars`,
  and provider lock/cache files; they were not deleted or committed.
- Legacy historical reports and the Phase 5 Airflow implementation.
- The 2025 deterministic fixture and ignored 2024 production Parquet files.

Reason:

The active Terraform replacement is validated locally enough for a bounded
plan, while state/cache artifacts are user/environment state and destructive
removal was not authorized. Phase 7 stays optional and remote-only by design.

## Student learning tasks

Phase 6 core decision: explain which resources are ephemeral infrastructure and
which S3/Iceberg data is canonical; manually read `force_destroy = false` and
the teardown section before any cloud approval.

Phase 7 core decision: explain why snapshots identify immutable table states,
why compaction must be threshold-driven, and why orphan deletion begins as a
dry run.

## Teach-back questions

1. Why can Terraform destroy the Glue job and IAM role but not safely delete a
   non-empty canonical S3 bucket automatically?
2. What does an exact six-table snapshot manifest protect that a “latest” query
   does not?
3. Why is a compaction threshold decision safe to plan locally while actual
   compaction, retention, and orphan deletion remain cloud-dependent?

## Failure/rerun experiment

The first Terraform format run used `-diff` and failed because Windows lacked
the external `diff` tool. Rerunning the same explicit file set without that
optional flag passed. The lifecycle tests also exercise failure boundaries:
2024 schema evolution is rejected, incomplete snapshot manifests are rejected,
and retention execution with `dry_run=false` is rejected; valid plans then pass
without mutating data.

## Later-phase confirmation

No work beyond Phase 7 was started. No AWS provisioning, CI execution, snapshot
mutation, compaction, retention, orphan deletion, or full-year backfill was
performed.
