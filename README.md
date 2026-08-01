# NYC HVFHV core industrial lakehouse

Production-shaped, cost-bounded AWS reference architecture for one immutable
NYC TLC HVFHV month:

```text
upstream-owned S3 landing
-> regular Amazon MWAA / Airflow 3
-> EMR Serverless Spark Bronze
-> EMR Serverless Spark Silver + quarantine
-> S3 Iceberg + Glue Data Catalog
-> Redshift Serverless Spectrum
-> Cosmos Watcher + one dbt-redshift build
-> six managed Gold relations
-> reconciliation -> publication -> bounded verification
```

Glue Data Catalog stores metadata only; all open-layer and Gold SQL reads use
Redshift Serverless/Spectrum. EMR Serverless is the sole Spark compute path.

The repository deliberately starts at the S3 landing contract. It does not
download or upload the producer's data. The upstream producer must land:

- `landing/fhvhv_tripdata_YYYY-MM.parquet`;
- `reference/taxi_zone_lookup.csv`;
- non-empty objects with lowercase SHA-256 in S3 metadata key `sha256`.

## Correctness contract

- Source identity: URI + SHA-256 + byte size + year + month.
- `row_id`: exact-row SHA-256 and the only deduplication/merge key.
- `business_trip_key`: analytical probable-trip key; never drops a row.
- Bronze is source-faithful and owns object/schema/non-empty checks.
- Silver owns deterministic validation, deduplication, and quarantine.
- `Bronze = Silver + quarantine`.
- `Silver = Gold fct_trips`.
- Gold is exactly three dimensions, one fact, and two marts.
- Publication is one immutable JSON release for the stable source run ID.

An identical completed source rerun reuses its open-layer snapshots and first
successful dbt artifact, then revalidates the serving output. A changed URI,
checksum, or byte size for an existing month is rejected; there is no `force`
bypass.

## Gold data product

The managed Redshift output contains:

```text
dim_date
dim_operator
dim_zone
fct_trips
mart_hourly_zone_demand
mart_operator_metrics
```

The marts support zone/hour demand planning and monthly operator performance
analysis. A dashboard is intentionally outside this repository: the Gold
relations are the consumer contract.

## Local verification

```powershell
venv\Scripts\python.exe -m black --check etl scripts tests
venv\Scripts\python.exe -m compileall -q etl scripts tests
venv\Scripts\python.exe -m pytest -p no:cacheprovider tests/unit -q
venv\Scripts\python.exe scripts/package_spark_jobs.py --output build/nyc_spark_jobs.zip --check

$env:DBT_CI_REDSHIFT_HOST='127.0.0.1'
$env:DBT_CI_REDSHIFT_USER='ci'
$env:DBT_CI_REDSHIFT_PASSWORD='ci-not-used'
$env:DBT_CI_REDSHIFT_DATABASE='lakehouse'
venv\Scripts\dbt.exe parse --project-dir etl/dbt_project --profiles-dir etl/dbt_project --target ci --no-partial-parse
venv\Scripts\dbt.exe compile --project-dir etl/dbt_project --profiles-dir etl/dbt_project --target ci --no-partial-parse --no-introspect --no-populate-cache
venv\Scripts\python.exe scripts/verify_dbt_manifest.py

terraform -chdir=terraform fmt -check
terraform -chdir=terraform init -backend=false
terraform -chdir=terraform validate
```

Deployment must use a dedicated NYC Terraform state. A first-deployment plan
must contain no destroy actions; see the runbook preflight before applying.

See [the blueprint](docs/PROJECT2_BLUEPRINT_FINAL.md), [runtime
semantics](docs/SEMANTICS.md), and [deployment/runbook](docs/RUNBOOK.md).

No AWS execution is claimed by repository tests. MWAA, S3, EMR Serverless,
Iceberg commits, Redshift Serverless/Spectrum, rerun, schema evolution, and
teardown remain **NOT VERIFIED** until a retained bounded AWS run exists.
