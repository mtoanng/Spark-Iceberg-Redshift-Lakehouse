# Deployment code status

Status: **implemented and statically verified; requires AWS execution verification**.

| Component | Code status | First-deployment evidence needed |
| --- | --- | --- |
| S3 canonical bucket | protected, versioned, SSE-S3, public access blocked | bucket configuration and retained-data behavior |
| Glue Catalog/Iceberg | four namespaces, v2 Parquet/Snappy table DDL | tables, partitions, snapshots, and locations |
| Glue jobs | initialize, Bronze, GE, Silver, reconciliation, publication | job run IDs, logs, counts, and failure behavior |
| Source upload | dry-run by default; immutable metadata verification | S3 object URI, bytes, SHA-256 metadata |
| dbt-glue | six models; month-scoped `fct_trips` merge | remote build/test and retry output |
| Airflow 3 | monthly DAG and sequential four-month DAG | import on runner, run IDs, retries, and failure propagation |
| Athena | one workgroup and four read-only SQL artifacts | query IDs, columns, rows, scanned bytes, result URI |
| IAM | Glue role plus bounded runner/dbt/Athena access | policy simulation or successful least-privilege run |
| Teardown | reviewed non-canonical destroy plan and read-only verifier | approved apply and absence checks |

Terraform state, private tfvars, saved plans, credentials, production source
files, build output, virtual environments, and dbt artifacts are ignored and
must not be committed.
