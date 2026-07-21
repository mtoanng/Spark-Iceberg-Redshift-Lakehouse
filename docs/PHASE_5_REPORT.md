# Phase 5 Report — Airflow, quality, and three-month rerun

Date: 2026-07-19

## Phase implemented

Phase 5 only. The user explicitly requested this phase despite the previous
report's `NOT VERIFIED` cloud items, satisfying the phase-gate override.

The active orchestration path is now a manual Airflow 3 monthly DAG with
`year`, `month`, and `force` parameters, plus a three-month sequential
backfill DAG. It invokes focused Glue/dbt tasks and a read-only explicit
quality checkpoint. The quality contract, backfill planning, source audit
binding, retry/force semantics, and DAG topology are covered by deterministic
local tests.

No Terraform, CI, cloud provisioning, cloud run, teardown, snapshot work, or
Phase 6 implementation was started.

## Files changed

Added:

- `etl/orchestration/__init__.py`
- `etl/orchestration/nyc_hvfhs_runs.py`
- `etl/quality/__init__.py`
- `etl/quality/nyc_hvfhs_checkpoint.py`
- `etl/glue_jobs/nyc_quality_checkpoint.py`
- `etl/dags/nyc_hvfhs_monthly_dag.py`
- `tests/unit/test_nyc_phase5_contracts.py`
- `tests/unit/test_nyc_airflow_dag.py`
- `docs/PHASE_5_REPORT.md`

Updated:

- `README.md`
- `.env.example`
- `requirements.txt`
- `Dockerfile.airflow`

Archived under `legacy/instacart_service/`:

- `etl/dags/instacart_pipeline_dag.py`
- `etl/glue_jobs/bronze_ingestion.py`
- `etl/glue_jobs/silver_transformation.py`
- `etl/ml/`
- `config/instacart_config.py`

## Orchestration and quality contract

`nyc_hvfhs_monthly` is manually triggered and runs this order:

```text
prepare_month -> bronze_ingestion -> silver_transform -> dbt_build -> quality_checkpoint
```

`prepare_month` resolves immutable source facts from Airflow Variables and
creates a deterministic audit payload. `force` is only a retry request for the
same source identity; the existing manifest contract blocks a changed checksum.

`nyc_hvfhs_three_month_backfill` chains three `TriggerDagRunOperator` tasks.
It accepts the same `year`, `month`, and `force` parameters, where `month` is
the first month and is constrained to January–October. Each monthly child run
waits to finish before the next starts.

The explicit quality checkpoint is the allowed equivalent to a Great
Expectations checkpoint. Its remote Glue entry point reads only canonical
Iceberg tables and fails when:

- Bronze count does not equal Silver plus quarantine count;
- Silver has duplicate `trip_id` values for the source month;
- a quarantine row lacks `reason_code`; or
- Gold `fct_trips` count does not equal valid Silver count for the month.

The pure local counterpart validates the same reconciliation, uniqueness, and
quarantine evidence using deterministic fixtures.

## Commands and results

Expected behavior before verification: local contracts should demonstrate one
valid quality pass, quality failures for duplicate identities and missing
quarantine reason, ordered three-month requests, force semantics, and the two
DAG topologies without starting Spark, Airflow services, Docker Compose, or
AWS.

| Command | Result |
| --- | --- |
| `python -m compileall -q etl\orchestration etl\quality etl\glue_jobs\nyc_quality_checkpoint.py etl\dags tests\unit` | PASS (exit 0). |
| Initial `pytest ... test_nyc_phase5_contracts.py test_nyc_airflow_dag.py` | FAILED: 3 quality tests used `fixture://filename`, whose filename is a URI host rather than path; Bronze correctly rejected it. No production code was changed for this. |
| Corrected focused test command | PASS: 7 passed. |
| `python -m pytest -p no:cacheprovider tests\unit tests\contract -q` | PASS: 39 passed in 1.27 s. |
| `dbt parse --profiles-dir . --target local_parse --no-partial-parse` with documented placeholder environment values | PASS (exit 0), dbt 1.11.12 / dbt-spark 1.10.3. No local compile/build was run because it starts Spark. |
| Active documentation scan, excluding historical reports and the governing blueprint | PASS: no active Instacart/MongoDB/recommendation/FastAPI/query-API/Kaggle references. |
| Active orchestration scan | PASS: no legacy Instacart/ML/MongoDB references in active DAG, Glue, orchestration, or quality paths. |
| Quality checkpoint mutation scan | PASS: no write/merge operation in `nyc_quality_checkpoint.py`. |
| Real installed Airflow 3 DAG import with disposable `AIRFLOW_HOME` | FAILED before project DAG code loaded: the installed Airflow 3.3.0 environment raised `ImportError: cannot import name 'ObjectStoragePath' from airflow.sdk`. `pip check` still reported no broken requirements. |
| Stubbed Airflow DAG import/topology test | PASS: validates both DAG definitions, required parameter defaults, five monthly tasks, and ordered three-month trigger chain without launching an Airflow service. |

## Verified acceptance criteria

- An active manual DAG declares `year`, `month`, and `force` parameters.
- The monthly task chain places the explicit quality checkpoint after dbt Gold.
- The three-month backfill contract returns exactly three ordered months and
  prevents a cross-year start.
- The Airflow backfill DAG chains child monthly triggers rather than running
  them in parallel.
- The deterministic source manifest behavior skips identical processed input,
  permits a forced retry of that same immutable source, and blocks checksum
  changes.
- The equivalent local quality checkpoint passes the valid fixture and rejects
  duplicate canonical IDs and missing quarantine reasons.
- The remote quality Glue job is statically compiled and contains no data write
  statement.
- The former active Instacart DAG/ML/configuration and replaced Glue paths are
  archived after the replacement topology test passed.

## NOT VERIFIED

- Airflow service startup, actual Airflow 3 DAG import in a healthy runtime,
  scheduler execution, task retries, task clearing, XCom rendering, Variables,
  Connections, and trigger waiting behavior.
- The local Airflow import is additionally blocked by the installed package
  runtime error recorded above; this is not an AWS credential issue.
- AWS credentials, Glue job registration/execution, S3, Glue Catalog, IAM,
  dbt-glue build/test, and physical Iceberg tables/snapshots.
- Runtime quality-checkpoint counts and failure behavior against real Bronze,
  Silver, quarantine, and Gold tables.
- A cloud retry/clear/rerun experiment and idempotency of physical Iceberg
  writes. Local tests prove only the pure manifest and deterministic-transform
  contracts.
- Actual sequential processing of three source months. The user’s four ignored
  local 2024 Parquet files were not loaded or scanned in this phase.
- Terraform validation, CI, deployment evidence, cost, and teardown; these are
  Phase 6 concerns.

## Blockers and risks

- The installed Airflow 3.3.0 environment cannot import its own DAG API because
  `ObjectStoragePath` is missing from `airflow.sdk`, despite `pip check`
  passing. Repairing or recreating that environment would require dependency
  installation outside this Phase 5 repository edit.
- Airflow Variable names, Glue job names, checksum/size facts, project root,
  S3 locations, and `aws_default` connection are placeholders until a bounded
  cloud environment is provisioned.
- The new quality checkpoint assumes the monthly source columns exist in all
  four canonical tables and that dbt publishes `glue_catalog.gold.fct_trips`.
  This is static code only until Glue/dbt runtime evidence exists.
- The legacy Terraform code remains active and incompatible with the target
  architecture; it is deliberately left for its Phase 6 replacement rather
  than changed in this phase.

## Cleanup Report

Removed:

- Legacy Instacart/MongoDB environment example values.
- The unused Great Expectations dependency declaration; this phase implements
  the blueprint-permitted explicit equivalent quality gate instead.

Archived:

- The weekly Instacart ML/MongoDB DAG after the NYC DAG topology test passed.
- The directly coupled old Glue entry points, ML directory, and configuration
  module under `legacy/instacart_service/`.

Kept:

- `Dockerfile.airflow`, updated to Airflow 3.3.0, as the active orchestration
  image definition; it was not built locally.
- Existing Terraform files, to be replaced only in Phase 6.
- Historical reports, source fixtures, and ignored local Parquet sources.

Reason:

The replacement DAG and its local contracts pass before legacy orchestration
was archived. Terraform and cloud deployment changes are expressly assigned to
the next phase, while local monthly source data stays uncommitted and unused.

## Student learning task

Core decision: **retry versus rerun versus backfill**. Read
`etl/orchestration/nyc_hvfhs_runs.py` and the two DAG definitions. Explain why
a task retry uses the same DAG run, a forced rerun still requires the same
checksum, and the three-month backfill is three ordered monthly DAG runs rather
than one parallel batch. Manually trace the DAG arrows and confirm where the
quality gate appears.

## Teach-back questions

1. Why must a changed checksum be blocked even when `force=true`?
2. What is the difference between clearing a failed Airflow task and triggering
   a new forced run of the same month?
3. Why is the quality checkpoint after dbt Gold rather than before Silver?

## Failure/rerun experiment

The first test run deliberately exposed a malformed fixture URI and failed
before Bronze metadata could be created. After correcting the fixture URI to
place the filename in its path, the same quality contract passed. Separately,
the manifest test verifies an identical processed source is skipped on a normal
rerun and becomes `process_forced_retry` only with `force=true`; a checksum
change remains blocked. This is local contract evidence, not a cloud-write
idempotency claim.

## Later-phase confirmation

No Phase 6 work was started: no Terraform replacement, CI configuration,
provisioning, cloud demo, evidence capture, or teardown was performed.
