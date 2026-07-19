# NYC High-Volume Ride-Hailing Lakehouse

Milestone A implements the one-month code path:

```text
NYC TLC HVFHV Parquet -> S3 -> AWS Glue/PySpark
-> Iceberg Bronze/Silver -> dbt Gold -> DuckDB fixed queries
```

Iceberg on S3 is the canonical store. DuckDB is a read-only analytical
consumer: it reads the six published Gold tables through reviewed, named
queries and does not accept arbitrary SQL.

## Current evidence

Credential-independent fixture tests cover the 2024 source contract, Bronze
metadata, Silver validation/quarantine, deterministic `trip_id`, Gold graph,
and all five DuckDB queries. AWS execution, physical Iceberg snapshots, dbt
build/test against Glue, and DuckDB reads from S3 are **NOT VERIFIED**.

Milestone A deliberately targets one month first (`2024-01`). Four consecutive
2024 Parquet files may remain in ignored `data/` for the later full junior
scope, but they must not be committed and the extra months are not run in this
phase.

## Local verification

Use the repository virtual environment; do not start local Spark or Airflow.

```powershell
.\.venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\unit tests\contract -q
.\.venv\Scripts\python.exe -m compileall -q consumer etl\sources etl\transforms tests

$env:GLUE_ROLE_ARN='arn:aws:iam::000000000000:role/local-parse-only'
$env:S3_GOLD_PATH='s3://local-parse-only/gold'
Push-Location etl\dbt_project
..\..\.venv\Scripts\dbt.exe parse --profiles-dir . --target local_parse --no-partial-parse
Pop-Location
```

The first command includes the end-to-end DuckDB smoke and expected-result
checks. The parse target is syntax/graph validation only; do not compile it,
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

4. After Bronze counts and metadata are checked, run
   `etl/glue_jobs/nyc_silver_transform.py`.

   ```powershell
   aws glue start-job-run --job-name <nyc-silver-job>
   ```

5. After Silver/quarantine reconciliation is checked, build and test the six
   Gold models from a disposable remote environment with `dbt-glue` installed.

   ```powershell
   Push-Location etl\dbt_project
   dbt build --profiles-dir . --target glue
   Pop-Location
   ```

6. After dbt tests pass, provide the six validated Gold Iceberg table S3
   locations to `DuckDBGoldConsumer.from_iceberg_locations(...)` and run only
   members of `QueryName`. The DuckDB host needs the Iceberg extension installed
   and an AWS credential chain with read-only S3/catalog permissions.

Do not advance to another month until the selected month reconciles from valid
Silver rows through `fct_trips` and the five fixed queries return reviewed
results.

## Gold and query contract

Gold contains exactly `dim_date`, `dim_operator`, `dim_zone`, `fct_trips`,
`mart_hourly_zone_demand`, and `mart_operator_metrics`. The fixed query pack is:

1. hourly pickups by zone;
2. operator trip count and average fare;
3. top pickup zones for a selected month;
4. fare and driver-pay reconciliation;
5. `EXPLAIN ANALYZE` for a filtered fact query.

Start with [AGENTS.md](AGENTS.md), the
[blueprint](docs/PROJECT2_BLUEPRINT_FINAL.md), and the latest
[phase report](docs/PHASE_4_REPORT.md).
