# Phase 0 Report — Inventory and Freeze Baseline

Date: 2026-07-19

## Phase implemented

Phase 0 only: inventory and baseline freeze. No Phase 1 dataset adapter,
fixtures, contracts, source manifest, or production/cloud resources were
started.

## Baseline state preserved

The repository is a legacy **Instacart market-basket / ML recommendation
warehouse**, not yet the NYC HVFHV lakehouse in the blueprint. No existing
implementation file was deleted, moved, reformatted, or rewritten in this
phase.

The pre-existing worktree is dirty. It includes untracked `AGENTS.md` and
`docs/PROJECT2_BLUEPRINT_FINAL.md`, plus pre-existing deletions under the
vendored `etl/dbt_project/dbt_packages/dbt_utils/` tree. Those changes are
not part of this phase and must be reviewed separately before any cleanup.

## Inventory

| Area | Current baseline | Blueprint conflict / migration status |
| --- | --- | --- |
| Configuration | `config/instacart_config.py` defines Instacart CSV names and requires `MONGODB_URI` at import time. | Replace with NYC HVFHV contract configuration in Phase 1; do not import this module from new code. |
| Ingestion | `etl/glue_jobs/bronze_ingestion.py` reads six Instacart CSVs and writes Iceberg tables with `createOrReplace`. | Retain as legacy baseline. Replace with one-month HVFHV Parquet Bronze ingestion in Phase 2. |
| Transformation | `etl/glue_jobs/silver_transformation.py` enriches order/product data. | Retain as legacy baseline. Replace with trip validation, deduplication, and quarantine in Phase 2. |
| Orchestration | `etl/dags/instacart_pipeline_dag.py` is an Instacart DAG. | Retain until the Airflow 3 parameterized NYC DAG is implemented in Phase 5. |
| dbt | `etl/dbt_project/` models Instacart staging, dimensions, facts, analytics marts, and an ML feature mart. | Retain until Phase 3 creates only the required NYC Gold models. The ML mart has no replacement in the locked architecture. |
| ML and MongoDB | `etl/ml/spark_recommendations.py`, `warehouse/recommendation_store.py`, Docker Compose MongoDB, and `terraform/ml_jobs.tf` implement recommendations. | Frozen legacy scope; no replacement is authorized. Do not remove until a replacement path and cleanup decision are explicitly approved. |
| Warehouse API | `warehouse/api/`, SQL validation, cache, SDK, and DuckDB engine expose a legacy service/query path. | Keep untouched. Phase 4 will add a fixed, read-only DuckDB consumer; it must not extend the unrestricted API pattern. |
| Infrastructure | `terraform/` creates Instacart-named S3, Glue Catalog/jobs, IAM, and ML job resources. | Preserve and re-scope only in Phase 6 after the application path is ready. No AWS provisioning occurred. |
| Tooling and docs | `README.md`, `Makefile`, `requirements.txt`, and most docs describe Instacart, Kaggle, MongoDB, API, and ML workflows. | Freeze now. Update active documentation only when the replacement phases implement their corresponding paths. |
| Tests | Warehouse cache and SQL-validator tests exist; `test_complete_pipeline.py` checks legacy paths and obsolete documentation names. | Preserve as baseline evidence. Add NYC fixture/contract tests beginning in Phase 1. |

## Migration map

| Legacy path | Decision | Replacement phase |
| --- | --- | --- |
| `config/instacart_config.py` | Do not reuse in NYC pipeline. | Phase 1 |
| `etl/glue_jobs/bronze_ingestion.py` | Preserve until new Bronze job passes. | Phase 2 |
| `etl/glue_jobs/silver_transformation.py` | Preserve until new Silver/quarantine job passes. | Phase 2 |
| `etl/dbt_project/models/` | Preserve existing models; add NYC Gold model set only when Iceberg Silver contract exists. | Phase 3 |
| `warehouse/` API, MongoDB store, SDK, cache | Freeze; do not extend. Introduce separate fixed-query consumer later. | Phase 4 |
| `etl/dags/instacart_pipeline_dag.py` | Preserve until new Airflow 3 DAG is import-tested. | Phase 5 |
| `terraform/` | Preserve, then replace/re-scope only after runnable code and smoke scripts exist. | Phase 6 |
| ML/recommendation files and related dependencies | No architectural replacement; remain frozen legacy material pending explicit cleanup approval. | Not scheduled |

## Commands and results

Expected behavior was recorded before each check: Python syntax should compile;
the warehouse suite was expected to fail collection because the test imports a
module path that does not exist; Terraform validation was expected to be local
and credential-independent.

| Command | Result |
| --- | --- |
| `.\.venv\Scripts\python.exe -m compileall -q config etl warehouse test_complete_pipeline.py` | PASS (exit 0). `compileall` warned that it could not list the inaccessible vendored `etl/dbt_project/dbt_packages/dbt_utils` directory. |
| `.\.venv\Scripts\python.exe -m pytest warehouse\tests -v` | FAILED (exit 2 at collection): `warehouse/tests/test_sql_validator.py` imports missing `warehouse.sql_validator`; implementation is located at `warehouse/parser/sql_validator.py`. Pytest also could not write the pre-existing `.pytest_cache`. |
| `.\.venv\Scripts\python.exe test_complete_pipeline.py` | FAILED (exit 1): 7/8 legacy checks passed; its documentation check expects six obsolete/missing root-level documents. The optional localhost API was not running and was treated as expected by the script. |
| `terraform fmt -check -recursive` | FAILED (exit 3): `terraform.tfvars` is not format-clean. No formatting was applied. |
| `terraform validate` | PASS (exit 0): configuration is valid. |
| `.\.venv\Scripts\dbt.exe --version` | PASS (exit 0): dbt-core 1.11.12 and dbt-spark 1.10.3 installed. Version update lookup was unavailable. |
| `.\.venv\Scripts\dbt.exe parse --profiles-dir . --target glue` from `etl/dbt_project` | FAILED (exit 2): required `GLUE_ROLE_ARN` environment variable was absent. No cloud connection was attempted. |
| `.\.venv\Scripts\python.exe -m pytest warehouse\tests\test_sql_validator.py -q` | Controlled failure/rerun experiment: reproduced the same missing-module collection failure (exit 2) without modifying baseline files. |

## Verified acceptance criteria

- Existing Instacart, ML, MongoDB, API, dbt, Glue, Terraform, and documentation paths were inspected.
- Baseline build/check outcomes and concrete failures are recorded.
- A migration map exists.
- No legacy working path was removed or changed.
- No full Spark, Airflow Compose, Docker Compose, AWS stack, or cloud resource was run.

## NOT VERIFIED

- AWS/S3/Glue/Iceberg deployment and execution.
- Airflow deployment, DAG import, retry, clear, and rerun behavior.
- NYC source availability, schemas, checksums, fixtures, or source manifest behavior.
- Bronze/Silver/Gold processing, quarantine, reconciliation, and idempotency.
- dbt compilation/build against Glue (parse is blocked by missing `GLUE_ROLE_ARN`).
- DuckDB consumption of published Iceberg Gold tables.
- Schema evolution, snapshot pinning, performance, recovery, costs, and teardown.

## Blockers and risks

- The repository contains both legacy-only components prohibited by the locked architecture and pre-existing uncommitted changes. They must remain untouched until their replacement and cleanup are explicitly sequenced.
- The current warehouse test suite is broken at import collection.
- The legacy pipeline health script has stale documentation expectations.
- Terraform is valid but currently fails the formatting check.
- The dbt project cannot parse without `GLUE_ROLE_ARN`; cloud-dependent dbt validation remains unverified.
- Because this report contains `NOT VERIFIED` and `FAILED` results, the Phase Gate Rule prevents Phase 1 from starting unless the user explicitly overrides it.

## Student learning task

Core decision: **source grain**. Explain in your own words why the new Bronze
contract must represent one source HVFHV trip record (plus ingestion metadata),
instead of reusing the legacy order-product tables or a pre-aggregated trip
summary. Manually verify the decision by comparing the Bronze contract in the
blueprint with the `INSTACART_FILES` mapping in `config/instacart_config.py`.

## Teach-back questions

1. What is the source grain for the NYC Bronze trip table, and why is a monthly summary an invalid substitute?
2. Why must the legacy Glue jobs remain available until their NYC replacements have passed checks?
3. Why does a successful `terraform validate` not prove that the AWS lakehouse has been deployed or works?

## Failure/rerun experiment

Run the SQL-validator test command above twice without changing code. Both runs
should fail at collection with `ModuleNotFoundError: warehouse.sql_validator`.
This demonstrates a reproducible baseline failure and avoids masking it during
the migration.

## Later-phase confirmation

No later phase was started.
