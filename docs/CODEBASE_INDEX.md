# Codebase index

This is the navigation map for the active NYC TLC High-Volume For-Hire Vehicle
(HVFHV) lakehouse. The active architecture is:

```text
official HVFHV Parquet + Taxi Zone lookup -> S3 Landing -> Airflow 3
-> Glue/PySpark Bronze -> Iceberg Bronze -> Great Expectations checkpoint
-> Glue/PySpark Silver + quarantine -> dbt-glue Gold
-> publication manifest -> Amazon Athena
```

AWS, Glue, Airflow service execution, dbt cloud builds, and Iceberg lifecycle
operations remain environment-dependent unless a phase report says otherwise.

## Start here

1. [README.md](../README.md) — current architecture, local checks, and remote
   run order.
2. [AGENTS.md](../AGENTS.md) — repository rules, learning contract, resource
   limits, and reporting requirements.
3. [PROJECT2_BLUEPRINT_FINAL.md](PROJECT2_BLUEPRINT_FINAL.md) — source of truth
   for scope and phase acceptance criteria.
4. [DATASET_NOTES.md](DATASET_NOTES.md) — approved TLC sources, fixture
   provenance, and source-schema notes.
5. [PHASE_6_7_REPORT.md](PHASE_6_7_REPORT.md) — latest implementation and
   verification status.

## Active code map

### Source contract and transformations

| Path | Responsibility |
| --- | --- |
| `etl/sources/nyc_hvfhs.py` | Official monthly filename/URI, required columns, deterministic `trip_id`, source manifest decisions, and stable run IDs. |
| `etl/transforms/nyc_hvfhs.py` | Source-faithful Bronze metadata, Silver validation, quarantine reason codes, deduplication, and reconciliation. |
| `etl/orchestration/nyc_hvfhs_runs.py` | Monthly request validation, immutable-source audit binding, and four-month sequential planning. |
| `etl/quality/nyc_hvfhs_ge.py` | Fixture-tested Great Expectations suite definition for the mandatory pre-Silver gate. |
| `etl/quality/nyc_hvfhs_checkpoint.py` | Credential-free post-Gold reconciliation contract; separate from Great Expectations. |

### Glue/PySpark entry points

| Path | Responsibility | Runtime |
| --- | --- | --- |
| `etl/glue_jobs/initialize_nyc_iceberg_tables.py` | Create Bronze/Silver/operations Iceberg namespaces and tables. | AWS Glue only |
| `etl/glue_jobs/nyc_bronze_ingestion.py` | Read one month and replace its guarded Bronze partitions. | AWS Glue only |
| `etl/glue_jobs/nyc_great_expectations_checkpoint.py` | Run the mandatory month-scoped GE gate and persist its result. | AWS Glue only |
| `etl/glue_jobs/nyc_silver_transform.py` | Require `ge_passed`, validate, reconcile, and replace Silver/quarantine partitions. | AWS Glue only |
| `etl/glue_jobs/nyc_quality_checkpoint.py` | Read-only Bronze/Silver/quarantine/Gold reconciliation gate. | AWS Glue only |
| `etl/glue_jobs/nyc_schema_evolution_2025.py` | Apply the bounded 2025 congestion-fee DDL plan. | AWS Glue only; Phase 7 |

### Airflow orchestration

| Path | Responsibility |
| --- | --- |
| `etl/dags/nyc_hvfhs_monthly_dag.py` | Manual one-month DAG contract; target order is Bronze → Great Expectations → Silver → dbt Gold → publication manifest → Athena smoke. |
| `etl/dags/nyc_hvfhs_monthly_dag.py` | Also defines the four-month sequential backfill DAG using ordered child triggers. |
| `etl/orchestration/nyc_hvfhs_runs.py` | Pure contracts used by the DAG; no scheduler or AWS calls. |

### Iceberg and lifecycle

| Path | Responsibility |
| --- | --- |
| `etl/iceberg/catalog.py` | Credential-free table specifications and DDL for the four upstream Iceberg tables. |
| `etl/iceberg/lifecycle.py` | Phase 7 schema-evolution plan, snapshot manifest/pinning references, compaction thresholds, retention dry run, and orphan-file dry run. |
| `etl/iceberg/__init__.py` | Public exports for catalog and lifecycle contracts. |

### dbt Gold

Project root: `etl/dbt_project/`

| Path | Responsibility |
| --- | --- |
| `dbt_project.yml` | NYC project configuration and Iceberg table materialization. |
| `profiles.yml` | `glue` runtime target and credential-free `local_parse` target. |
| `models_nyc/sources.yml` | Bronze Taxi Zone and Silver trip source declarations. |
| `models_nyc/schema.yml` | Gold model and column tests. |
| `models_nyc/dimensions/` | `dim_date`, `dim_operator`, and `dim_zone`. |
| `models_nyc/facts/fct_trips.sql` | One row per validated, deduplicated `trip_id`. |
| `models_nyc/marts/` | Hourly zone demand and operator metrics marts. |
| `tests/fct_trips_reconciles_to_silver.sql` | Silver-to-fact reconciliation assertion. |

### Athena serving

| Path | Responsibility |
| --- | --- |
| `athena/query_runner.py` | Generic Boto3 runner using the AWS SDK credential chain and one workgroup. |
| `athena/verify_gold.py` | Minimal Gold smoke verifier. |
| `athena/sql/` | Four bounded SQL artifacts: Gold smoke, business mart, history/snapshots, and version-travel template. |

### Infrastructure and CI

| Path | Responsibility |
| --- | --- |
| `terraform/main.tf` | AWS provider, tags, and outputs. |
| `terraform/s3.tf` | Private versioned/encrypted S3 bucket with `force_destroy = false`. |
| `terraform/glue_catalog.tf` | NYC Glue Catalog database. |
| `terraform/iam.tf` | Glue service role and lakehouse permissions. |
| `terraform/glue_jobs.tf` | S3 script objects and the active Glue jobs. |
| `terraform/variables.tf` | Bounded environment inputs and validations. |
| `terraform/terraform.tfvars.example` | Non-secret example inputs only. |
| `.github/workflows/ci.yml` | Python/tests, dbt parse, Terraform format, and validation workflow. |
| `Dockerfile.airflow` | Airflow 3 orchestration image definition; not run locally. |

## Tests and fixtures

| Path | Coverage |
| --- | --- |
| `tests/fixtures/nyc_hvfhs/` | Small source-shaped 2024/2025 trip fixtures and Taxi Zone lookup. |
| `tests/unit/test_nyc_hvfhs_source.py` | Source schema, checksum identity, URI, and manifest decisions. |
| `tests/unit/test_nyc_hvfhs_transform.py` | Bronze/Silver/quarantine and rerun contracts. |
| `tests/unit/test_iceberg_catalog.py` | Upstream Iceberg table specifications and DDL. |
| `tests/unit/test_dbt_gold_contract.py` | Exactly six active Gold models and dbt graph contract. |
| `tests/unit/test_athena_runner.py` | Mocked Boto3 runner behavior, pagination, token forwarding, failure, cancellation, and timeout. |
| `tests/unit/test_athena_sql.py` | Exactly four read-only Athena SQL artifacts and safe parameter/template rules. |
| `tests/unit/test_nyc_phase5_contracts.py` | Quality gate, Airflow request/audit, four-month planning, and force semantics. |
| `tests/unit/test_nyc_airflow_dag.py` | DAG topology import using local Airflow interface stubs. |
| `tests/unit/test_iceberg_lifecycle.py` | Phase 7 schema, snapshot, compaction, retention, and orphan contracts. |
| `tests/contract/test_athena_smoke.py` | Mocked minimal Gold smoke-verifier contract. |

Credential-independent local checks:

```powershell
.\.venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\unit tests\contract -q
.\.venv\Scripts\python.exe -m compileall -q athena etl tests
Push-Location etl\dbt_project
..\..\.venv\Scripts\dbt.exe parse --profiles-dir . --target local_parse --no-partial-parse
Pop-Location
terraform fmt -check terraform\main.tf terraform\variables.tf terraform\s3.tf terraform\glue_catalog.tf terraform\iam.tf terraform\glue_jobs.tf
terraform -chdir=terraform validate
```

## Documentation and evidence

| Path | Purpose |
| --- | --- |
| `docs/PHASE_0_REPORT.md` through `docs/PHASE_5_REPORT.md` | Historical implementation reports. |
| `docs/CODEBASE_COMPLETION_REPORT.md` | Latest closure-phase implementation report. |
| `docs/CLOUD_DEMO_RUNBOOK.md` | Approved disposable-cloud procedure and teardown safety. |
| `docs/CLOUD_EVIDENCE_TEMPLATE.md` | Blank evidence record; all fields begin `NOT VERIFIED`. |

## Archived material

`legacy/` is not an active runtime path:

- `legacy/dbt_instacart_models/` — former Instacart dbt graph.
- `legacy/instacart_service/` — former Instacart DAG, Glue/ML jobs, warehouse
  API, MongoDB recommendation store, Docker, Makefile, and CI.
- `legacy/docs_instacart/` — obsolete development, Glue-access, and Terraform
  documentation.
- `legacy/terraform_instacart/` — replaced Instacart/ML Terraform sources.

Do not import or deploy archived paths. They are retained for migration history,
not as supported alternatives to the NYC architecture.

## Data and generated state

- `data/` contains locally staged monthly Parquet and is ignored; never commit
  production source files.
- `.env` and Terraform state/plan files are environment data and must not be
  committed.
- `etl/dbt_project/target/`, virtual environments, logs, and caches are
  generated artifacts.

## Scope boundary

The repository intentionally has no active ML, recommendation, MongoDB,
unrestricted SQL API, dashboard, or advanced Iceberg mutation service. Cloud
deployment, snapshot IDs, schema-evolution execution, compaction, retention,
orphan deletion, performance, and recovery require retained remote evidence.
