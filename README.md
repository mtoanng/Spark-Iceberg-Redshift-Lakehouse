# AWS Iceberg Athena Lakehouse

Replayable monthly NYC TLC HVFHV lakehouse:

```text
immutable S3 landing
-> Airflow 3
-> EMR Serverless PySpark Bronze Iceberg
-> EMR Serverless PySpark Silver + quarantine Iceberg
-> Redshift Serverless external Bronze/Silver schemas
-> Cosmos `DbtTaskGroup` + dbt-redshift managed Gold
-> Athena + Redshift Data API reconciliation
-> deterministic publication + verification
```

Iceberg on S3 remains canonical for Bronze, Silver, quarantine, and operational
state. Glue Data Catalog remains their Iceberg catalog. Redshift Serverless
owns the six managed Gold relations.
The first release is one immutable 2024 month; four consecutive months follow
only after that baseline succeeds.

## Identity and quality

`row_id` is the policy-versioned SHA-256 exact-row key used by Silver
deduplication and dbt merge. `business_trip_key` is a probable-trip analytical
key and never drops data. `identity_policy_version` makes the ordered field set
auditable. Python and Spark share
`etl/contracts/nyc_hvfhs_identity.py`.

Bronze checks source existence, checksum, size, schema, and non-empty input.
Silver alone owns row-level validation and deterministic quarantine. Required
invariant:

```text
Bronze = Silver + quarantine
Silver = Gold fct_trips
```

## Local verification

```powershell
venv\Scripts\python.exe -m pytest -q
venv\Scripts\python.exe -m compileall -q etl athena scripts tests
venv\Scripts\dbt.exe deps --project-dir etl/dbt_project --profiles-dir etl/dbt_project
venv\Scripts\dbt.exe parse --project-dir etl/dbt_project --profiles-dir etl/dbt_project --target ci
venv\Scripts\dbt.exe compile --project-dir etl/dbt_project --profiles-dir etl/dbt_project --target ci --no-introspect --no-populate-cache
venv\Scripts\python.exe scripts/verify_dbt_manifest.py
terraform -chdir=terraform fmt -check
terraform -chdir=terraform init -backend=false
terraform -chdir=terraform validate
```

Package EMR Serverless Spark imports deterministically:

```powershell
venv\Scripts\python.exe scripts/package_spark_jobs.py --output build/nyc_spark_jobs.zip --check
```

Deployment and execution commands are in [docs/RUNBOOK.md](docs/RUNBOOK.md).
Operational semantics are in [docs/SEMANTICS.md](docs/SEMANTICS.md).

No real AWS execution is claimed. Redshift connectivity, external Iceberg
reads, managed Gold builds, reconciliation, publication, and verification are
**NOT VERIFIED** until a bounded AWS run retains evidence.
