# Cloud evidence template

Status: **NOT VERIFIED** until completed from a real bounded AWS run.

## Run and source identity

- Environment, region, UTC start/end, operator/change reference:
- Budget alert and estimated cost:
- Month, source S3 URI, bytes, SHA-256, ingestion run ID:
- Taxi Zone URI, bytes, SHA-256:

## Provisioning and orchestration

- Terraform plan/apply summary:
- S3, Glue database/job, IAM role, Athena workgroup identifiers:
- Airflow monthly DAG run and task states:
- Retry/clear-and-rerun evidence:
- Immutable-source rejection experiment:

## Data lifecycle

| Check | Evidence |
| --- | --- |
| Bronze rows | NOT VERIFIED |
| Great Expectations blocking result | NOT VERIFIED |
| Silver rows | NOT VERIFIED |
| Quarantine rows and reason distribution | NOT VERIFIED |
| Bronze = Silver + quarantine | NOT VERIFIED |
| Gold fact = Silver | NOT VERIFIED |
| Six Gold models/tests | NOT VERIFIED |
| Publication manifest URI/status | NOT VERIFIED |
| Independent business-total check | NOT VERIFIED |

## Athena and operations

- Query IDs, expected columns, non-empty result:
- Required partition filter and bytes scanned:
- Failure/recovery notes:
- Teardown plan/apply and retained canonical resources:
- Remaining resources and expected cost:

For a four-month run, duplicate the source/data sections per month and attach
the sequential parent/child DAG evidence. Do not invent counts, snapshot IDs,
costs, or query plans.
