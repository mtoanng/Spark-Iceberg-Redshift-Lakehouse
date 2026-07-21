# Minimal Amazon Athena scope

Audit date: 2026-07-20

Status: **APPROVED CODE SCOPE; NOT IMPLEMENTED; AWS EXECUTION NOT VERIFIED**

## Role in the architecture

Athena is the only active analytical query surface for Project 2. It reads the
Gold Iceberg tables registered in AWS Glue Data Catalog. It does not replace
Glue/PySpark, dbt-glue, the Glue Catalog, or S3/Iceberg canonical storage.

Athena engine version 3 supports querying Glue-cataloged Iceberg tables,
Iceberg metadata tables such as `$history` and `$snapshots`, and version travel
with `FOR VERSION AS OF`. See the AWS documentation for
[Iceberg queries](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg.html),
[metadata tables](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-table-data.html),
and [time/version travel](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-time-travel-and-version-travel-queries.html).

## Terraform: exactly one workgroup

Add one `aws_athena_workgroup` to the existing Terraform module with:

- a project/environment-scoped name;
- engine version 3 selected or allowed as the effective engine;
- `enforce_workgroup_configuration = true`;
- result location `s3://<existing-project-bucket>/<athena-results-prefix>/`;
- result encryption `SSE_S3`;
- `publish_cloudwatch_metrics_enabled = true`;
- configurable `bytes_scanned_cutoff_per_query` with Terraform validation at
  or above Athena's 10 MiB minimum (`10485760` bytes);
- `force_destroy = false` for the workgroup;
- outputs for workgroup name and result prefix only.

The HashiCorp AWS provider exposes these settings directly on
[`aws_athena_workgroup`](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/athena_workgroup).
AWS documents that a workgroup result location is required when no client
location is supplied and that query metrics/cost controls are workgroup
features ([workgroup creation](https://docs.aws.amazon.com/athena/latest/ug/creating-workgroups.html),
[cost controls](https://docs.aws.amazon.com/athena/latest/ug/workgroups-control-limits.html)).

Do not create a bucket. Reuse the existing protected project bucket and add
only a configurable prefix, defaulting to `athena-results`.

## IAM: exactly one least-privilege query policy

Create one policy intended for the existing temporary Airflow/query-runner
principal. Do not create a hierarchy of Athena roles.

The policy must be scoped to:

- Athena actions on the one workgroup:
  `StartQueryExecution`, `GetQueryExecution`, `GetQueryResults`,
  `StopQueryExecution`, and `GetWorkGroup`;
- Glue read actions for the catalog and Gold database/tables only:
  `GetDatabase`, `GetDatabases`, `GetTable`, `GetTables`, and `GetPartitions`;
- S3 list/read access to the Gold Iceberg prefix and Iceberg metadata objects;
- S3 list/read/write access only to the Athena query-result prefix;
- `GetBucketLocation` on the existing project bucket.

Do not grant Athena DDL/DML administration, Glue mutation, broad `athena:*`,
broad `glue:*`, or write access to the Gold data prefix. Athena uses the
caller's S3 and Glue permissions when querying data; see AWS guidance for
[Athena IAM](https://docs.aws.amazon.com/athena/latest/ug/security-iam-athena.html)
and [S3 access](https://docs.aws.amazon.com/athena/latest/ug/s3-permissions.html).

## SQL: exactly four artifacts

Use a small `athena/sql/` package.

1. `gold_smoke.sql`
   - Read `fct_trips` for a selected year/month.
   - Return row count, distinct `trip_id` count, minimum pickup timestamp, and
     maximum drop-off timestamp.
   - The verifier requires `row_count > 0` and
     `row_count = distinct_trip_count`.

2. `mart_hourly_zone_demand.sql`
   - Query the representative business mart for selected year/month or date.
   - Return the top bounded set of pickup date/hour/zone rows by `trip_count`.
   - No CTAS, UNLOAD, INSERT, UPDATE, DELETE, MERGE, OPTIMIZE, or VACUUM.

3. `iceberg_history.sql`
   - Join or read the Gold fact's `$history` and `$snapshots` metadata tables in
     one read-only statement.
   - Return snapshot ID, commit time, parent ID, operation, and summary fields
     needed for manual review.

4. `time_travel.sql.tmpl`
   - Use a strictly validated Gold database/table identifier and
     `FOR VERSION AS OF ?` for the snapshot ID.
   - Accept scalar execution parameters through Athena, not string-concatenated
     user values.
   - Keep automated verification deferred; the template is code/demo material
     only until an operator supplies a real snapshot ID.

Every query must be a single read-only statement, have a deterministic row
limit where results can grow, and address only the Gold database. Tests must
reject mutation keywords and unresolved template placeholders.

## Python: exactly two responsibilities

### Generic Boto3 query runner

Add one runner that:

- receives SQL, Glue database/catalog, workgroup, optional Athena execution
  parameters, timeout, and poll interval;
- calls `start_query_execution` with a stable client request token when the
  caller supplies one;
- polls `get_query_execution` until `SUCCEEDED`, `FAILED`, or `CANCELLED`;
- calls `stop_query_execution` on timeout;
- paginates `get_query_results` and returns column names, rows, query ID,
  scanned bytes, engine time, and result location;
- raises a focused exception containing the query ID and Athena state reason;
- obtains credentials from Boto3's default chain/instance profile and has no
  access-key parameters.

Boto3 supports workgroups, positional execution parameters, and idempotent
client request tokens in
[`start_query_execution`](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena/client/start_query_execution.html).

### Minimal Gold smoke verifier

Add one CLI/module that:

- loads only `gold_smoke.sql`;
- accepts year/month plus catalog/database/workgroup configuration;
- runs it through the generic runner;
- fails if no Gold rows exist, `trip_id` is not unique, required timestamp
  bounds are null, or Athena did not report success;
- prints a concise result with query ID and scanned bytes;
- never claims the full pipeline is valid and never exports a general evidence
  bundle.

## Configuration surface

The future `.env.cloud.example` should contain placeholders for:

```text
AWS_REGION
GLUE_CATALOG_NAME=AwsDataCatalog
GLUE_GOLD_DATABASE
ATHENA_WORKGROUP
ATHENA_RESULTS_PREFIX
ATHENA_BYTES_SCANNED_CUTOFF
```

It must not contain access-key placeholders. The deployed runner uses its EC2
instance profile; developer invocations use the normal AWS SDK credential
chain.

## Required credential-independent checks

- Terraform format and validation for the workgroup, result prefix, cutoff,
  and IAM resource scopes.
- Static SQL tests proving exactly four artifacts and read-only statements.
- Mocked Boto3 tests for success, failure, cancellation, pagination, timeout,
  idempotent token forwarding, execution parameters, and scanned-byte output.
- Mocked smoke-verifier tests for pass, empty Gold, duplicate IDs, and missing
  timestamp bounds.
- Active architecture scan containing Athena and no retired-serving imports, dependency,
  runbook step, or CI job.

All real query results, snapshot IDs, scan sizes, cost, performance, and
time-travel behavior remain **NOT VERIFIED** until a bounded AWS run is retained.

## Explicitly deferred

- customer-managed KMS keys;
- a dedicated Athena results bucket;
- Lake Formation;
- an IAM role hierarchy;
- CloudWatch dashboards and alarms;
- an evidence exporter;
- a generic verification framework;
- a large query library;
- automated time-travel verification;
- Athena writes, CTAS, UNLOAD, maintenance, optimization, or vacuum operations.
