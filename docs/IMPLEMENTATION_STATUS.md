# Implementation status

Last updated: 2026-07-21

## Closure Phase A — source to Silver boundary

Status: **CODE IMPLEMENTED; AWS EXECUTION NOT VERIFIED**.

Phase A completes the credential-independent code contracts for the monthly
source boundary through Silver publication. The serving-layer replacement was
intentionally deferred to Closure Phase B.

Implemented:

- `ops.source_run_manifest` Iceberg DDL and a pure manifest state machine with
  immutable source URI/checksum/size, month, run ID, status/timestamps, Bronze,
  Silver and quarantine counts, GE result metadata, and failure details.
- Month-scoped Bronze replacement writes and a checksum/URI guard. A completed
  source is skipped unless an identical source is explicitly forced.
- Mandatory Great Expectations task between Bronze and Silver in the monthly
  Airflow topology. Structural/checkpoint failure persists `ge_blocked` and
  prevents canonical Silver publication.
- Silver is scoped to a year/month/run, checks for `ge_passed`, reconciles
  counts before writing, replaces the month partitions, and retains invalid
  records in reason-coded quarantine.
- Catalog/database arguments use a shared `CATALOG_NAME` plus logical
  `BRONZE_DATABASE`, `SILVER_DATABASE`, and `OPS_DATABASE` wiring with safe
  Glue-Catalog defaults.

Great Expectations does not replace quarantine. Required schema and non-empty
batch conditions are promotion-blocking. Timestamp/order, non-negative metric,
and zone-resolution observations are persisted in the run manifest and then
handled by the same deterministic Silver reason-code logic, preserving every
invalid Bronze row as quarantine evidence.

## Verification status

Credential-independent checks passed: formatting, undefined-name lint,
compile, the 53-test Python unit/contract suite, installed Great Expectations
suite construction, static manifest/idempotency checks, stubbed Airflow DAG
topology, and a five-row constrained local PySpark fixture.

NOT VERIFIED: AWS credentials; S3, Glue Catalog, actual Iceberg table writes;
Glue job registration/execution; Great Expectations on Glue; Airflow service;
physical Iceberg retry behavior; dbt-glue; Terraform plan/apply/destroy; and
Athena. The local PySpark invocation required explicit `PYSPARK_PYTHON` because
the Windows environment does not provide `python3`.

## Closure Phase B â€” Athena serving replacement

Status: **CODE IMPLEMENTED; AWS EXECUTION NOT VERIFIED**.

The active analytical boundary is one Athena workgroup, four read-only SQL
artifacts, one Boto3 runner, and one Gold smoke verifier. Results use a prefix
of the existing project bucket with SSE-S3, CloudWatch query metrics, and a
configurable scan cutoff. One policy grants only Gold object/Glue metadata
reads, one-workgroup query actions, and result-prefix access.

No Lake Formation, customer-managed KMS, separate results bucket, role
hierarchy, dashboard, alarm, evidence exporter, generic query framework, or
large query catalog was added. AWS queries, Terraform apply, and physical
results remain NOT VERIFIED.

## Closure Phase C — deployment closure

Status: **STATIC CODE IMPLEMENTED; AWS EXECUTION NOT VERIFIED**.

Terraform now models only the NYC TLC architecture: protected S3, bronze,
silver, ops, and gold Glue namespaces, packaged Glue jobs, the bounded Athena
workgroup/policy, and an optional IMDSv2-enforced Airflow runner using an EC2
instance profile. The tracked obsolete Terraform plan was removed; ignored
state, private variables, credentials, and generated plans remain ignored.

Glue packaging is deterministic and records entrypoints, runtime dependencies,
artifact S3 key, catalog, and namespace wiring. Airflow deployment includes
the project import path, environment contract, year/month/force parameters,
Bronze → GE → Silver → dbt → reconciliation → publication → optional Athena
smoke ordering. Smoke/release/E2E, reconciliation, package, and guarded
teardown scripts are present and unexecuted against AWS.

The confirmed junior release profile is four sequential months.
