# AWS Iceberg Athena Lakehouse

Replayable monthly NYC TLC HVFHV lakehouse:

```text
immutable S3 landing
-> Airflow 3
-> EMR Serverless PySpark Bronze Iceberg
-> structural Great Expectations gate
-> EMR Serverless PySpark Silver + quarantine Iceberg
-> Cosmos `DbtTaskGroup` + dbt-glue Gold
-> reconciliation + snapshot-aware publication
-> bounded read-only Athena
```

Iceberg on S3 is canonical and Glue Data Catalog is the canonical catalog.
The first release is one immutable 2024 month; four consecutive months follow
only after that baseline succeeds.

## Identity and quality

`row_id` is the policy-versioned SHA-256 exact-row key used by Silver
deduplication and dbt merge. `business_trip_key` is a probable-trip analytical
key and never drops data. `identity_policy_version` makes the ordered field set
auditable. Python and Spark share
`etl/contracts/nyc_hvfhs_identity.py`.

Great Expectations checks structure, non-empty month, and identity inputs.
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
terraform -chdir=terraform fmt -check
terraform -chdir=terraform init -backend=false
terraform -chdir=terraform validate
```

Package Glue imports deterministically:

```powershell
venv\Scripts\python.exe scripts/package_spark_jobs.py --output build/nyc_spark_jobs.zip --check
```

Deployment and execution commands are in [docs/RUNBOOK.md](docs/RUNBOOK.md).
Operational semantics are in [docs/SEMANTICS.md](docs/SEMANTICS.md).

No real AWS execution is claimed. `DEPLOYMENT-VERIFIED` is **NOT VERIFIED**
until one bounded run and teardown retain evidence.
