# NYC High-Volume Ride-Hailing Lakehouse

For a file-by-file navigation map, see the [codebase index](docs/CODEBASE_INDEX.md).

Milestone A implements the one-month code path:

```text
Official NYC TLC HVFHV Parquet + Taxi Zone lookup -> S3 Landing -> Airflow 3
-> Glue/PySpark Bronze -> Iceberg Bronze -> Great Expectations checkpoint
-> Glue/PySpark Silver + quarantine -> dbt-glue Gold
-> publication manifest -> Amazon Athena
```

Iceberg on S3 and the AWS Glue Data Catalog are canonical. Great Expectations
blocks the Bronze-to-Silver transition; Silver quarantine preserves invalid
rows with reason codes. Athena is the bounded read-only analytical serving
layer over validated Gold tables.

## Current evidence

Credential-independent fixture tests cover the 2024 source contract, Bronze
metadata, Silver validation/quarantine, deterministic `trip_id`, Gold graph,
the explicit quality gate, Airflow DAG structure, and four-month planning.
Athena contracts are tested locally; Great Expectations runtime, AWS execution, real Airflow
scheduling, physical Iceberg snapshots, and dbt build/test against Glue are
**NOT VERIFIED**.

Milestone A deliberately targets one month first (`2024-01`). Four consecutive
2024 Parquet files may remain in ignored `data/` for the later full junior
scope, but they must not be committed and the extra months are not run in this
phase.

## Local verification

Use the repository virtual environment; do not start local Spark or Airflow.

```powershell
.\.venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\unit tests\contract -q
.\.venv\Scripts\python.exe -m compileall -q athena etl tests

$env:GLUE_ROLE_ARN='arn:aws:iam::000000000000:role/local-parse-only'
$env:S3_GOLD_PATH='s3://local-parse-only/gold'
Push-Location etl\dbt_project
..\..\.venv\Scripts\dbt.exe parse --profiles-dir . --target local_parse --no-partial-parse
Pop-Location
```

The first command includes fixture and contract checks. The parse target is
syntax/graph validation only; do not compile it,
because that adapter starts a local Spark session.

## Exact Milestone A run order

The following is the remote/AWS order after infrastructure and named Glue jobs
exist. Replace angle-bracket placeholders with the bounded environment values.
These commands are an operator runbook, not evidence that AWS has been run.

1. Calculate checksums for the selected trip file and Taxi Zone lookup, then
   upload those exact files to the landing prefix.

   ```powershell
   Get-FileHash data\fhvhv_tripdata_2024-01.parquet -Algorithm SHA256
   Get-FileHash data\taxi_zone_lookup.csv -Algorithm SHA256
   aws s3 cp data\fhvhv_tripdata_2024-01.parquet s3://<bucket>/landing/fhvhv_tripdata_2024-01.parquet
   aws s3 cp data\taxi_zone_lookup.csv s3://<bucket>/reference/taxi_zone_lookup.csv
   ```

2. Run `etl/glue_jobs/initialize_nyc_iceberg_tables.py` once with the warehouse
   root.

   ```powershell
   aws glue start-job-run --job-name <nyc-iceberg-initialize-job> --arguments '{"--WAREHOUSE_URI":"s3://<bucket>/warehouse"}'
   ```

3. Run `etl/glue_jobs/nyc_bronze_ingestion.py` for `2024-01`. Use the actual
   SHA-256 values from step 1 and a unique ingestion run ID.

   ```powershell
   aws glue start-job-run --job-name <nyc-bronze-job> --arguments '{"--SOURCE_URI":"s3://<bucket>/landing/fhvhv_tripdata_2024-01.parquet","--SOURCE_YEAR":"2024","--SOURCE_MONTH":"1","--SOURCE_CHECKSUM":"<trip-sha256>","--INGESTION_RUN_ID":"<run-id>","--TAXI_ZONE_URI":"s3://<bucket>/reference/taxi_zone_lookup.csv","--TAXI_ZONE_CHECKSUM":"<zone-sha256>"}'
   ```

4. Run the mandatory Great Expectations checkpoint after Bronze and before
   Silver. It blocks Silver when schema, timestamps, measures, or Taxi Zone
   expectations fail; invalid source rows remain available for Silver
   quarantine with reason codes.

   ```powershell
   airflow tasks test <nyc-great-expectations-task>
   ```

5. After the checkpoint passes, run the month-scoped Silver transformation.

   ```powershell
   aws glue start-job-run --job-name <nyc-silver-job>
   ```

6. After Silver/quarantine reconciliation is checked, build and test the six
   Gold models from a disposable remote environment with `dbt-glue` installed.

   ```powershell
   Push-Location etl\dbt_project
   dbt build --profiles-dir . --target glue
   Pop-Location
   ```

7. Publish the validated manifest, then run
   the bounded Athena Gold smoke, business-mart, and history/snapshots queries.

Do not advance to another month until the selected month reconciles from valid
Silver rows through `fct_trips`, the publication manifest is valid, and the
Athena smoke result is reviewed.

## Phase 5 manual orchestration

`etl/dags/nyc_hvfhs_monthly_dag.py` defines the manually triggered Airflow 3
DAG `nyc_hvfhs_monthly` with `year`, `month`, and `force` parameters. Its task
order is `prepare_month -> bronze_ingestion -> great_expectations ->
silver_transform -> dbt_build -> publication_manifest -> athena_smoke`.
`nyc_hvfhs_four_month_backfill` triggers four monthly
runs in sequence, starting at a month no later than September.

`force` is a same-source retry signal, not permission to replace a source with a
different checksum. The deployed Airflow instance must hold the documented
source checksums and sizes as Variables; no cloud scheduler, retry, clear, or
backfill run has been executed yet.

## Gold and query contract

Gold contains exactly `dim_date`, `dim_operator`, `dim_zone`, `fct_trips`,
`mart_hourly_zone_demand`, and `mart_operator_metrics`. The bounded Athena
query pack is:

1. Gold smoke reconciliation;
2. representative hourly zone-demand business query;
3. Iceberg history/snapshots metadata query;
4. parameterized version-travel template (manual verification only).

## Terraform, CI, and advanced lifecycle

Phase 6 supplies minimal NYC-only Terraform, a credential-independent CI
workflow, and the approved bounded-cloud runbook in
[CLOUD_DEMO_RUNBOOK.md](docs/CLOUD_DEMO_RUNBOOK.md). Terraform apply, AWS
execution, evidence capture, and teardown are **NOT VERIFIED** until an
approved disposable environment is used.

Phase 7 supplies remote-only planning contracts for the 2025 congestion-fee
schema addition, exact six-table snapshot manifests, pinned-reference handoff,
threshold-based compaction decisions, retention dry runs, and orphan-file dry
runs. No lifecycle operation deletes or rewrites canonical data locally.

Start with [AGENTS.md](AGENTS.md), the
[blueprint](docs/PROJECT2_BLUEPRINT_FINAL.md), and the latest
[closure reports](docs/CODEBASE_COMPLETION_REPORT.md).
