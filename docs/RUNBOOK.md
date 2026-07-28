# Deployment and execution runbook

All AWS writes and costs require separate approval. The commands below are
instructions; repository validation does not execute `terraform apply`.

## 1. Validate locally

```powershell
venv\Scripts\python.exe -m black --check athena etl scripts tests
venv\Scripts\python.exe -m compileall -q athena etl scripts tests
venv\Scripts\python.exe -m pytest -p no:cacheprovider tests/unit tests/contract -q
venv\Scripts\python.exe scripts/check_repository_hygiene.py
venv\Scripts\python.exe scripts/package_spark_jobs.py --output build/nyc_spark_jobs.zip --check
```

For credential-independent dbt parsing:

```powershell
$env:DBT_CI_REDSHIFT_HOST='127.0.0.1'
$env:DBT_CI_REDSHIFT_USER='ci'
$env:DBT_CI_REDSHIFT_PASSWORD='ci-not-used'
$env:DBT_CI_REDSHIFT_DATABASE='lakehouse'
venv\Scripts\dbt.exe parse --project-dir etl/dbt_project --profiles-dir etl/dbt_project --target ci --no-partial-parse
venv\Scripts\dbt.exe compile --project-dir etl/dbt_project --profiles-dir etl/dbt_project --target ci --no-partial-parse --no-introspect --no-populate-cache
venv\Scripts\python.exe scripts/verify_dbt_manifest.py
```

## 2. Prepare the deployment inputs

Copy `terraform/terraform.tfvars.example` to ignored
`terraform/terraform.tfvars`. Provide:

- a globally unique private S3 bucket name;
- one existing VPC;
- exactly two private subnet IDs in different Availability Zones;
- NAT/VPC endpoint routing needed by MWAA for AWS APIs and PyPI.

The producer—not this repository—must already have landed:

```text
s3://<bucket>/landing/fhvhv_tripdata_2024-01.parquet
s3://<bucket>/reference/taxi_zone_lookup.csv
```

Both objects require lowercase SHA-256 in S3 metadata `sha256`.

## 3. Plan infrastructure

```powershell
terraform -chdir=terraform fmt -check
terraform -chdir=terraform init -backend=false
terraform -chdir=terraform validate
terraform -chdir=terraform plan -out=baseline.tfplan
```

Review the plan. Do not save or commit it. Apply only after explicit cost
approval:

```powershell
terraform -chdir=terraform apply baseline.tfplan
```

Terraform uploads the DAG/package/requirements, creates private regular MWAA,
EMR Serverless, Glue namespaces, Athena, and Redshift Serverless, and
bootstraps the Redshift IAM database user used by dbt.

## 4. Configure regular MWAA

Get the non-secret map:

```powershell
terraform -chdir=terraform output -json airflow_variables
```

Import those key/value pairs as Airflow Variables in the MWAA environment.
There are no monthly checksum variables: the DAG reads object identity from
S3. Confirm the DAG import has no error before running it.

## 5. Prove one month

Trigger `nyc_hvfhs_monthly` with:

```json
{"year": 2024, "month": 1}
```

Retain:

- S3 URI, SHA-256, size, and stable run ID;
- Airflow task states and EMR job IDs;
- Bronze/Silver/quarantine counts and snapshot IDs;
- quarantine counts by reason;
- dbt `run_results.json`;
- Redshift six-relation list;
- Athena and Redshift reconciliation IDs;
- publication URI/SHA-256;
- bounded verification result.

Pass criteria:

```text
Bronze > 0
Bronze = Silver + quarantine
Silver = Gold fct_trips
dbt results are success/pass
publication status = published
verification Silver = published Silver
verification Gold = published Gold
```

## 6. Prove retry/rerun

After the baseline succeeds:

1. clear a non-mutating downstream task and let it retry;
2. trigger the same `{year, month}` again;
3. confirm source/run identity, counts, row IDs, reason distribution, and
   published snapshots remain stable;
4. use `scripts/verify_monthly_rerun.py` on retained evidence.

Then change only the producer object's identity in an isolated test
environment and confirm Bronze rejects it before a write.

## 7. Four-month sequence

Only after the one-month proof, trigger
`nyc_hvfhs_four_month_backfill` with the first year/month. It invokes exactly
four monthly runs sequentially.

## 8. Approved 2025 evolution

Submit `apply_nyc_2025_schema_evolution.py` once, using the same Spark catalog
configuration.
Then run one 2025 month and retain old/new snapshot IDs plus Athena version
travel evidence. Use `scripts/verify_schema_evolution.py` to validate the
evidence document.

## 9. Teardown

Generate a review-only bounded destroy plan:

```powershell
.\scripts\teardown.ps1
```

It retains S3, Glue namespaces, and canonical Iceberg data. Applying that plan
requires separate approval. After an approved apply, use the read-only
`scripts/verify_teardown.py`. See [TEARDOWN_RUNBOOK.md](TEARDOWN_RUNBOOK.md).
