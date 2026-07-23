# Codebase index

The active project is the NYC TLC HVFHV lakehouse described in
[`PROJECT2_BLUEPRINT_FINAL.md`](PROJECT2_BLUEPRINT_FINAL.md).

| Area | Active paths | Responsibility |
| --- | --- | --- |
| Source contract | `etl/sources/nyc_hvfhs.py`, `scripts/fetch_source.py`, `scripts/upload_release_dataset.py` | Official filenames, local identity, immutable S3 landing |
| Bronze | `etl/glue_jobs/nyc_bronze_ingestion.py` | One source month, ingestion metadata, guarded month write |
| Quality gate | `etl/glue_jobs/nyc_great_expectations_checkpoint.py`, `etl/quality/` | Blocking structural and non-empty checks |
| Silver/quarantine | `etl/glue_jobs/nyc_silver_transform.py`, `etl/transforms/` | Deterministic trip IDs, reason-coded validation, reconciliation |
| Gold | `etl/dbt_project/` | Three dimensions, one fact, and two marts |
| Publication | `etl/glue_jobs/nyc_quality_checkpoint.py`, `etl/glue_jobs/nyc_publish_manifest.py` | Post-Gold reconciliation and publication manifest |
| Orchestration | `etl/dags/nyc_hvfhs_monthly_dag.py` | Monthly DAG and sequential four-month trigger DAG |
| Athena | `athena/`, `athena/sql/` | Read-only, Gold-only bounded queries and verification |
| Infrastructure | `terraform/` | S3, Glue, IAM, Athena, optional bounded runner |
| CI | `.github/workflows/ci.yml`, `requirements-ci.txt` | Credential-independent compile, tests, dbt, package, shell, Terraform, hygiene |
| Tests | `tests/unit/`, `tests/contract/`, `tests/fixtures/` | Deterministic contracts without Spark or AWS |

## Operational documentation

- [`DEPLOYMENT_RUNBOOK.md`](DEPLOYMENT_RUNBOOK.md): one-month vertical slice.
- [`FOUR_MONTH_BACKFILL_RUNBOOK.md`](FOUR_MONTH_BACKFILL_RUNBOOK.md): sequential bounded backfill.
- [`TEARDOWN_RUNBOOK.md`](TEARDOWN_RUNBOOK.md): protected teardown.
- [`CLOUD_EVIDENCE_TEMPLATE.md`](CLOUD_EVIDENCE_TEMPLATE.md): first-run evidence.
- `docs/evidence/final-e2e/`: redacted retained evidence location after AWS execution.
- [`ARCHITECTURE.md`](ARCHITECTURE.md): boundaries and deferred maintenance.
- [`MAINTENANCE_PREPARATION.md`](MAINTENANCE_PREPARATION.md): future, gated Iceberg procedures.
- [`IMPLEMENTATION_STATUS.md`](IMPLEMENTATION_STATUS.md): honest verification state.

`data/`, virtual environments, dbt targets, build outputs, Terraform state and
plans are generated or private and must not be committed. Historical phase
reports are evidence of earlier work, not active architecture documentation.
