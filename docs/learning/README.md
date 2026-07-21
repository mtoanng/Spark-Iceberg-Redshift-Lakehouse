# Learning package: NYC HVFHV Iceberg Lakehouse

This package teaches the CODEBASE-READY repository in two layers:

1. architecture: why the system exists and how data, metadata, orchestration, quality, serving, deployment, and failure boundaries fit together;
2. code: where each implementation lives, how to run or verify it, and what is still NOT VERIFIED until an approved AWS run exists.

Start here, then use:

- [Component map](COMPONENT_MAP.md)
- [Interview guide](INTERVIEW_GUIDE.md)
- [Project blueprint](../PROJECT2_BLUEPRINT_FINAL.md)
- [Codebase index](../CODEBASE_INDEX.md)
- [Deployment runbook](../DEPLOYMENT_RUNBOOK.md)

## 30-second pitch

This repository builds a monthly batch lakehouse for official NYC TLC High Volume For-Hire Vehicle trip records. One immutable monthly Parquet file and the Taxi Zone lookup land in S3, Airflow orchestrates Glue jobs, Bronze preserves source-shaped records in Iceberg, Great Expectations blocks unsafe promotion, Silver validates and quarantines bad rows, dbt-glue publishes a small Gold model, a publication manifest records validated state, and Athena provides bounded read-only analytics. The codebase is credential-independent tested, but real AWS execution remains **NOT VERIFIED**.

## 90-second pitch

The business problem is to produce a reliable analytical lakehouse for high-volume ride-hailing trips without silently changing source data or overbuilding the serving layer. The source of truth is the official NYC TLC HVFHV monthly Parquet file named `fhvhv_tripdata_YYYY-MM.parquet`, plus `taxi_zone_lookup.csv`.

The architecture is deliberately bounded. S3 stores immutable landing data and Iceberg table data. Glue/PySpark owns Bronze and Silver transformations. Bronze adds only ingestion metadata. Great Expectations runs after Bronze and before Silver as a blocking promotion gate. Silver derives the project fields, creates deterministic `trip_id`, validates timestamps, zones, non-negative metrics, and duplicates, then writes accepted rows to `silver.silver_trips` and invalid rows to `silver.quarantine_trips` with `reason_code`.

dbt-glue turns valid Silver data into exactly six Gold models: `dim_operator`, `dim_zone`, `dim_date`, `fct_trips`, `mart_hourly_zone_demand`, and `mart_operator_metrics`. A publication manifest records validated publication state. Athena reads only bounded Gold SQL artifacts. Terraform defines protected S3, Glue Catalog namespaces, Glue jobs, IAM roles/policies, an Athena workgroup, and an optional Airflow runner. The repository has local unit/contract tests, dbt parse contracts, packaging checks, and Terraform validation guidance; AWS apply, Glue runs, Airflow scheduling, dbt-glue execution, Athena query results, and teardown evidence are **NOT VERIFIED**.

## 5-minute walkthrough

The project starts with official NYC TLC monthly HVFHV source files. The active source adapter is [etl/sources/nyc_hvfhs.py](../../etl/sources/nyc_hvfhs.py). It defines the official base URI constants `NYC_TLC_TRIP_DATA_BASE_URI` and `NYC_TLC_TAXI_ZONE_URI`, builds official filenames with `monthly_trip_filename`, builds source URLs with `monthly_trip_uri`, validates required columns with `validate_trip_schema`, hashes local files with `inspect_local_source`, computes deterministic row identity with `canonical_trip_id`, and creates a stable monthly run identifier with `stable_run_id`.

S3 is the first cloud boundary. Terraform defines one protected project bucket in [terraform/s3.tf](../../terraform/s3.tf), with source landing under `landing_prefix`, Taxi Zone reference data under `reference_prefix`, Iceberg warehouse files under `warehouse_prefix`, and Athena results under `athena_results_prefix`. Source files must not be committed.

Airflow is the coordinator, not the transformation engine. [etl/dags/nyc_hvfhs_monthly_dag.py](../../etl/dags/nyc_hvfhs_monthly_dag.py) defines `nyc_hvfhs_monthly` and `nyc_hvfhs_four_month_backfill`. The monthly DAG accepts `year`, `month`, and `force`, reads immutable source facts from Airflow Variables, and runs:

```text
prepare_month
-> bronze_ingestion
-> great_expectations_checkpoint
-> silver_transform
-> dbt_build
-> quality_checkpoint
-> publication_manifest
-> athena_smoke
```

Bronze ingestion is [etl/glue_jobs/nyc_bronze_ingestion.py](../../etl/glue_jobs/nyc_bronze_ingestion.py). It reads `SOURCE_URI`, adds `_source_file`, `_source_year`, `_source_month`, `_source_checksum`, `_ingestion_run_id`, and `_ingested_at`, writes `bronze.bronze_hvfhs_trips`, writes `bronze.bronze_taxi_zones`, and merges state into `ops.source_run_manifest`. It blocks changed source URI or checksum for an existing month. If the same completed source is rerun without `FORCE=true`, it skips canonical writes.

Iceberg table definitions are centralized in [etl/iceberg/catalog.py](../../etl/iceberg/catalog.py). The active upstream tables are `bronze.bronze_hvfhs_trips`, `bronze.bronze_taxi_zones`, `silver.silver_trips`, `silver.quarantine_trips`, and `ops.source_run_manifest`. Glue Catalog is the canonical catalog, and S3/Iceberg is canonical storage.

The quality gate is mandatory before Silver. [etl/glue_jobs/nyc_great_expectations_checkpoint.py](../../etl/glue_jobs/nyc_great_expectations_checkpoint.py) runs suite `nyc_hvfhs_bronze_pre_silver` against the Bronze month and persists either `ge_passed` or `ge_blocked` in `ops.source_run_manifest`. Silver publication is blocked unless the manifest row is `ge_passed`. The pure local Great Expectations contract is in [etl/quality/nyc_hvfhs_ge.py](../../etl/quality/nyc_hvfhs_ge.py).

Silver transformation is [etl/glue_jobs/nyc_silver_transform.py](../../etl/glue_jobs/nyc_silver_transform.py). It filters to one `SOURCE_YEAR`, `SOURCE_MONTH`, and `INGESTION_RUN_ID`, requires `ge_passed`, computes the same `trip_id` fingerprint, joins Taxi Zones, validates timestamp ordering, zone resolution, non-negative metrics, and duplicates, then writes accepted records to `silver.silver_trips` and rejected records to `silver.quarantine_trips`. The pure fixture equivalent is [etl/transforms/nyc_hvfhs.py](../../etl/transforms/nyc_hvfhs.py).

Gold is dbt-glue under [etl/dbt_project](../../etl/dbt_project). The project declares sources in [etl/dbt_project/models_nyc/sources.yml](../../etl/dbt_project/models_nyc/sources.yml), model tests in [etl/dbt_project/models_nyc/schema.yml](../../etl/dbt_project/models_nyc/schema.yml), and exactly six models: `dim_operator`, `dim_zone`, `dim_date`, `fct_trips`, `mart_hourly_zone_demand`, and `mart_operator_metrics`. The fact grain is one row per validated, deduplicated `trip_id`.

Publication state is separate from data transformation. [scripts/publish_publication_manifest.py](../../scripts/publish_publication_manifest.py) writes a manifest document with source identity, run ID, Gold row count, and validation status. [scripts/reconcile_outputs.py](../../scripts/reconcile_outputs.py) validates manifest-level reconciliation locally. This does not replace the Great Expectations gate.

Athena is the serving layer. [athena/query_runner.py](../../athena/query_runner.py) defines `AthenaQueryRunner`, `AthenaQueryResult`, and `AthenaQueryError`; it uses Boto3 and the default AWS SDK credential chain. [athena/verify_gold.py](../../athena/verify_gold.py) runs the minimal smoke check. SQL is limited to [athena/sql/gold_smoke.sql](../../athena/sql/gold_smoke.sql), [athena/sql/mart_hourly_zone_demand.sql](../../athena/sql/mart_hourly_zone_demand.sql), [athena/sql/iceberg_history.sql](../../athena/sql/iceberg_history.sql), and [athena/sql/time_travel.sql.tmpl](../../athena/sql/time_travel.sql.tmpl).

Deployment is Terraform plus scripts, but cloud execution is not proven by the repository alone. [terraform](../../terraform) defines S3, Glue Catalog, IAM, Glue jobs, Athena, and the optional Airflow runner. [scripts/package_glue_jobs.py](../../scripts/package_glue_jobs.py) builds the deterministic Glue package. [scripts/run_smoke.ps1](../../scripts/run_smoke.ps1), [scripts/run_release.ps1](../../scripts/run_release.ps1), and [scripts/run_e2e.py](../../scripts/run_e2e.py) define smoke/release command plans. [scripts/teardown.ps1](../../scripts/teardown.ps1) and [scripts/verify_teardown.py](../../scripts/verify_teardown.py) cover teardown behavior. Actual Terraform apply/destroy, Glue execution, Airflow runtime, dbt-glue build, Athena results, and teardown are **NOT VERIFIED** until evidence is captured.

## Layer 1: architecture

### Business and dataset problem

The business goal is a reliable monthly analytical lakehouse for NYC high-volume ride-hailing activity. The input grain is a source trip record from the official TLC monthly HVFHV Parquet file. The output is a small, tested Gold model that supports bounded Athena analytics by pickup time, pickup zone, and operator.

The repository intentionally avoids ML, recommendation systems, dashboards, generic query APIs, alternate serving databases, route optimization, traffic, weather, demographics, and unrestricted SQL endpoints.

### Source provenance

The approved source is official NYC TLC data:

- monthly trips: `fhvhv_tripdata_YYYY-MM.parquet`;
- reference lookup: `taxi_zone_lookup.csv`.

The source URI constants and filename rules live in [etl/sources/nyc_hvfhs.py](../../etl/sources/nyc_hvfhs.py). Dataset notes live in [docs/DATASET_NOTES.md](../DATASET_NOTES.md). Deterministic fixtures live in [tests/fixtures/nyc_hvfhs](../../tests/fixtures/nyc_hvfhs).

### Complete interaction

```text
NYC TLC source
-> S3 Landing
-> Airflow
-> Glue Bronze
-> Iceberg Bronze
-> Great Expectations
-> Glue Silver/quarantine
-> Iceberg Silver
-> dbt Gold
-> publication manifest
-> Athena
```

### Data plane

The data plane carries trip and reference records:

- S3 landing stores immutable monthly Parquet and Taxi Zone lookup objects.
- Glue Bronze reads source objects and writes Iceberg Bronze tables.
- Great Expectations reads Bronze and writes validation state to the manifest.
- Glue Silver reads Bronze and Taxi Zones, writes accepted rows and quarantine rows.
- dbt-glue reads Silver and Bronze Taxi Zones, writes six Gold Iceberg models.
- Athena reads Gold Iceberg tables through Glue Catalog.

### Orchestration flow

Airflow owns sequencing and retries. Transformation logic stays in Glue/PySpark or dbt.

The monthly DAG is `nyc_hvfhs_monthly`. It runs the exact task chain shown above. The backfill DAG is `nyc_hvfhs_four_month_backfill`; it triggers four monthly DAG runs sequentially.

### Metadata plane

The metadata plane is split:

- Glue Data Catalog is the canonical table catalog.
- Iceberg metadata in S3 tracks table state.
- `ops.source_run_manifest` records source URI, checksum, size, month, run ID, status, counts, validation result fields, and failure fields.
- The publication manifest records validated Gold publication state for serving.

### Quality gate

Great Expectations is mandatory and blocking between Bronze and Silver. It does not clean data. If the gate fails, the manifest records `ge_blocked`, the Glue job raises an error, and Silver must not publish.

Silver still applies the same row-level business validation and preserves bad rows in `silver.quarantine_trips` with deterministic `reason_code`.

### State and manifest ownership

`ops.source_run_manifest` owns monthly run state. [etl/manifests/nyc_hvfhs.py](../../etl/manifests/nyc_hvfhs.py) defines `RunStatus`, `SourceRunManifest`, and `retry_is_safe`. [etl/glue_jobs/nyc_bronze_ingestion.py](../../etl/glue_jobs/nyc_bronze_ingestion.py), [etl/glue_jobs/nyc_great_expectations_checkpoint.py](../../etl/glue_jobs/nyc_great_expectations_checkpoint.py), and [etl/glue_jobs/nyc_silver_transform.py](../../etl/glue_jobs/nyc_silver_transform.py) mutate manifest status during remote execution.

The publication manifest is produced after dbt and reconciliation by [scripts/publish_publication_manifest.py](../../scripts/publish_publication_manifest.py).

### Serving flow

Athena reads only Gold tables. The repository includes four bounded SQL artifacts and a Boto3 runner. Athena is not canonical storage and does not write project data.

### Deployment architecture

Terraform defines:

- protected S3 bucket;
- Glue Catalog databases for `bronze`, `silver`, `ops`, and `gold`;
- Glue job scripts and package objects;
- Glue jobs for initialize, Bronze, Silver, quality, Great Expectations, and 2025 schema evolution;
- IAM roles and policies for Glue, Athena Gold query access, and optional Airflow runner access;
- Athena workgroup `${var.project_name}-${var.environment}-gold`;
- optional EC2 Airflow runner using an instance profile and IMDSv2.

### Failure boundaries

Failures are bounded by stage:

- source mismatch: blocked by manifest source identity checks;
- Bronze failure: manifest can be marked `failed` with `failure_stage='bronze'`;
- Great Expectations failure: manifest becomes `ge_blocked`, Silver is blocked;
- Silver failure: manifest can be marked `failed` with `failure_stage='silver'`;
- post-Gold reconciliation failure: quality task fails after dbt and before publication;
- publication or Athena smoke failure: serving publication evidence is incomplete, data remains in canonical Iceberg tables.

### Rerun and retry behavior

The safe path depends on immutable source identity:

- same month, same URI, same checksum, same size: eligible for retry behavior;
- already `silver_published` and `force=false`: skip canonical writes;
- already `silver_published` and `force=true`: forced retry of identical source;
- changed URI/checksum/size: blocked, not replaced by this workflow.

Physical exactly-once behavior is **NOT VERIFIED** until a real AWS retry/rerun experiment captures evidence.

## Representative record trace

Use the deterministic 2024 fixture [tests/fixtures/nyc_hvfhs/fhvhv_tripdata_2024-01.fixture.json](../../tests/fixtures/nyc_hvfhs/fhvhv_tripdata_2024-01.fixture.json) and Taxi Zone fixture [tests/fixtures/nyc_hvfhs/taxi_zone_lookup.fixture.csv](../../tests/fixtures/nyc_hvfhs/taxi_zone_lookup.fixture.csv).

1. Source identity is described by `SourceFile` in [etl/sources/nyc_hvfhs.py](../../etl/sources/nyc_hvfhs.py): `source_year=2024`, `source_month=1`, source URI, checksum, and size.
2. `stable_run_id` returns a deterministic ID shaped like `fhvhv-2024-01-<checksum-prefix>`.
3. `bronze_records` in [etl/transforms/nyc_hvfhs.py](../../etl/transforms/nyc_hvfhs.py) copies the record and adds only `_source_file`, `_source_year`, `_source_month`, `_source_checksum`, `_ingestion_run_id`, and `_ingested_at`.
4. In cloud execution, [etl/glue_jobs/nyc_bronze_ingestion.py](../../etl/glue_jobs/nyc_bronze_ingestion.py) writes the month partition to `glue_catalog.bronze.bronze_hvfhs_trips` and Taxi Zones to `glue_catalog.bronze.bronze_taxi_zones`.
5. [etl/glue_jobs/nyc_great_expectations_checkpoint.py](../../etl/glue_jobs/nyc_great_expectations_checkpoint.py) validates suite `nyc_hvfhs_bronze_pre_silver`. If the batch is non-empty and structural expectations pass, it writes `ge_passed`.
6. `transform_silver` and [etl/glue_jobs/nyc_silver_transform.py](../../etl/glue_jobs/nyc_silver_transform.py) compute `trip_id` from source identity columns, validate timestamp order, resolve `PULocationID` and `DOLocationID`, check non-negative measures, and classify duplicates.
7. A valid record becomes one row in `silver.silver_trips` with fields such as `operator_code`, `pickup_zone_id`, `dropoff_zone_id`, `trip_time_seconds`, `passenger_fare`, `trip_duration_minutes`, `pickup_date`, and `pickup_hour`.
8. An invalid record becomes one row in `silver.quarantine_trips` with `reason_code`, for example `DROPOFF_BEFORE_PICKUP`, `UNKNOWN_PICKUP_ZONE`, `NEGATIVE_TRIP_MILES`, or `DUPLICATE_TRIP_ID`.
9. dbt model [etl/dbt_project/models_nyc/facts/fct_trips.sql](../../etl/dbt_project/models_nyc/facts/fct_trips.sql) turns the valid Silver row into one Gold fact row.
10. dbt marts aggregate the fact row into [etl/dbt_project/models_nyc/marts/mart_hourly_zone_demand.sql](../../etl/dbt_project/models_nyc/marts/mart_hourly_zone_demand.sql) and [etl/dbt_project/models_nyc/marts/mart_operator_metrics.sql](../../etl/dbt_project/models_nyc/marts/mart_operator_metrics.sql).
11. [scripts/publish_publication_manifest.py](../../scripts/publish_publication_manifest.py) records validated publication metadata.
12. [athena/verify_gold.py](../../athena/verify_gold.py) and SQL under [athena/sql](../../athena/sql) query the Gold state through Athena.

## Design trade-offs

- Bronze is source-faithful, so business validation moves to the Bronze-to-Silver boundary instead of mutating raw data early.
- Great Expectations blocks promotion, while Silver quarantine preserves row-level evidence. The gate and quarantine have different jobs.
- `trip_id` is deterministic from source-level fields, avoiding dependence on ingestion timestamps.
- One month first, four months second, full-year optional. This keeps cost, runtime, and debugging bounded.
- Athena is read-only and bounded to four SQL artifacts, avoiding an unrestricted serving API.
- Glue Catalog is canonical metadata, not local files or an alternate query service.
- The optional Airflow runner uses instance-profile authentication, avoiding committed static AWS keys.
- Terraform uses `force_destroy = false` for canonical storage, favoring data safety over easy cleanup.
- Phase 7 schema evolution exists in code but is advanced and must not be used to claim junior completion.

## Current limitations

- AWS execution is **NOT VERIFIED**.
- Terraform apply/destroy is **NOT VERIFIED**.
- Glue job registration and execution are **NOT VERIFIED**.
- Physical Iceberg writes, snapshots, compaction, retention, orphan handling, and time travel are **NOT VERIFIED**.
- Airflow service deployment and scheduler behavior are **NOT VERIFIED**.
- dbt-glue build against AWS is **NOT VERIFIED**.
- Athena query IDs, scanned bytes, result locations, metrics, and workgroup enforcement are **NOT VERIFIED**.
- Real source row counts, costs, performance, and recovery behavior are **NOT VERIFIED**.
- Legacy files under [legacy](../../legacy) are archived, not active.
- Some script names under [scripts](../../scripts) are legacy/Kaggle-oriented; they exist in the repository but conflict with the current NYC-only blueprint if treated as active source ingestion.
- `scripts/__pycache__` exists as generated state and should not be treated as source code.
- Git status could not be verified in this run because the repository is marked dubious by Git safe-directory ownership.

## Prohibited claims until AWS verification

Do not claim:

- production-ready;
- deployed;
- AWS-verified;
- exactly-once;
- cost-verified;
- performance-tested;
- schema evolution executed;
- snapshot-pinned;
- compaction executed;
- orphan deletion safe in production;
- full-year backfill completed;
- Athena results validated against real AWS;
- Airflow retry/rerun behavior verified in a real scheduler;
- Terraform resources applied or destroyed successfully.

## Student learning task

Explain why Great Expectations is a blocking Bronze-to-Silver gate but does not replace Silver quarantine. Use `ge_passed`, `ge_blocked`, `silver.silver_trips`, and `silver.quarantine_trips` in your answer.

## Teach-back questions

1. Which fields define immutable source identity for a monthly run, and why does a changed checksum block the normal rerun path?
2. What belongs in Bronze versus Silver in this repository?
3. Why is Athena a bounded read-only serving layer instead of the canonical storage or metadata layer?

## Failure/rerun experiment

On fixtures, explain the expected result before running it: rerun the same source without `force` and expect no duplicate canonical rows; rerun the same source with `force` and expect an intentional retry path; change the checksum and expect the manifest contract to block the run.
