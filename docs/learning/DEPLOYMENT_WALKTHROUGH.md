# Deployment and runtime walkthrough

This guide connects the architecture and code to what happens during an
approved deployment. It is explanatory; the operator checklist remains
[DEPLOYMENT_RUNBOOK.md](../DEPLOYMENT_RUNBOOK.md).

No command in this document is evidence that AWS execution occurred. Do not
run `terraform apply`, upload source data, start paid services, or apply a
destroy plan without separate approval.

## 1. Deployment phases

```text
local validation
-> deterministic Glue package
-> Terraform init/validate/plan
-> separately approved Terraform apply
-> source fetch/hash
-> immutable S3 upload
-> Iceberg initializer
-> temporary Airflow runner configuration
-> one 2024 monthly DAG
-> retained publication/Athena/retry evidence
-> four-month sequence
-> optional 2025 evolution exercise
-> bounded teardown and read-only verification
```

## 2. Local validation before AWS

The local boundary should prove code contracts before any resource is
created. CI performs:

```powershell
python scripts/check_repository_hygiene.py
python -m compileall -q athena etl scripts tests
python -m pytest -p no:cacheprovider tests/unit tests/contract -q
python scripts/package_spark_jobs.py --output build/nyc_spark_jobs.zip --check
```

It also parses all PowerShell, runs `bash -n` on the runner bootstrap, performs
dbt dependency resolution/parse/compile at the credential-independent `ci`
target, and runs Terraform format/init/validate.

Success means `CODEBASE-READY`. It does not mean deployed.

## 3. Deterministic Spark artifact

`scripts/package_spark_jobs.py` scans active Python under `etl/`, excludes DAGs
and caches, pins zip entry timestamps to 1980-01-01, and embeds
`spark_runtime_manifest.json`.

Why this precedes Terraform:

- `terraform/emr_serverless.tf` calls `filemd5()` on the local zip;
- Terraform uploads the zip to `spark_jobs/nyc_spark_jobs.zip`;
- every EMR Serverless submission receives it through `--py-files`;
- the entry script remains separate so Spark has a direct S3 entry point.

The package includes shared contracts. The Airflow DAG is excluded because it
runs on the Airflow runner, not inside EMR Serverless.

## 4. Terraform plan and resource graph

The required private input is a globally unique bucket name. Optional runner
creation also needs an approved AMI and subnet. The plan should be saved but
not committed.

Conceptual dependency graph:

```text
AWS provider/account
  -> S3 bucket/protection
      -> Glue package and scripts
      -> Glue jobs
      -> Athena result configuration
  -> Glue databases
  -> Glue service role/policy
      -> Glue jobs
  -> Airflow role/policies
      -> optional instance profile
          -> optional EC2 runner
  -> Athena workgroup
```

Terraform outputs become downstream configuration:

| Terraform output | Consumer |
| --- | --- |
| `s3_bucket_name` | upload and evidence commands |
| `s3_landing_uri` | Airflow `nyc_landing_uri` |
| `s3_warehouse_uri` | Glue Spark catalog and dbt `S3_GOLD_PATH` |
| `glue_database_name` | dbt/Athena database config |
| `glue_role_arn` | dbt `GLUE_ROLE_ARN` |
| `glue_job_names` | Airflow Glue-job Variables |
| `athena_workgroup_name` | Airflow/Athena verifier |
| `athena_results_prefix` | evidence and teardown verification |
| `publication_prefix_uri` | Airflow publication Variable |
| `airflow_runner_instance_profile` | optional EC2 role binding |

## 5. What Terraform creates

### Canonical storage

One S3 bucket:

- versioning enabled;
- AES256 server-side encryption;
- public access blocked;
- `force_destroy=false`.

It contains canonical tables and run evidence as well as temporary/result
prefixes. There is no second data store.

### Catalog

Exactly four Glue databases:

```text
bronze
silver
ops
gold
```

The initializer creates upstream Iceberg tables. dbt creates Gold model tables.

### Glue jobs

One pre-created EMR Serverless Spark application auto-starts on submission and
auto-stops after its bounded idle period. Every entrypoint is an S3 script and
uses the same versioned Python package. Airflow owns retry boundaries; the
application is never created or deleted by a DAG run.

### Athena

The workgroup enforces:

- its configured result location;
- metrics;
- a per-query scanned-byte cutoff.

IAM further limits the runner to that workgroup, Gold Glue metadata, Gold
warehouse reads, and Athena result writes.

### Temporary Airflow runner

When enabled, EC2 uses:

- the Airflow instance profile;
- encrypted root volume;
- IMDSv2 required;
- no public static access keys in files or environment.

The instance is temporary. The bucket and canonical databases are not.

## 6. IAM collaboration

### Glue service role

Glue can:

- list the project bucket;
- read landing, reference, and Glue job objects;
- read/write/delete Iceberg warehouse objects required for table commits;
- use Glue temporary and manifest prefixes;
- create/read/update required Glue Catalog tables.

Deleting individual Iceberg objects is a normal table-commit capability, not
authorization for a broad manual S3 cleanup.

### Airflow runner role

The runner can:

- start and inspect project-prefixed Glue jobs;
- create/use the dbt Glue interactive session;
- read configuration/catalog facts required by dbt;
- write retained dbt results;
- run only the configured Athena workgroup;
- use SSM for runner access.

### Athena access

The runner can read Gold metadata/data and write only Athena results. Athena
queries themselves remain read-only SQL.

## 7. Source staging and configuration handoff

### Fetch

`python -m scripts.fetch_source` downloads:

```text
data/fhvhv_tripdata_2024-01.parquet
data/taxi_zone_lookup.csv
```

The `data/` directory is ignored and must never be committed.

### Plan upload

Without `--execute`, `upload_release_dataset` calculates and prints:

- local path;
- S3 URI;
- byte size;
- SHA-256;
- planned status;
- Airflow Variables.

Review this JSON before mutation.

### Execute immutable upload

With separately approved `--execute`, the script:

1. calls `HeadObject`;
2. accepts an exact existing object as already present;
3. rejects changed existing content;
4. uploads a missing object with `sha256` metadata;
5. calls `HeadObject` again and verifies size/hash.

The output is the authoritative configuration handoff to Airflow.

## 8. Initializer run

The initializer receives the warehouse URI from Glue default arguments and
creates upstream Iceberg tables.

Baseline invocation does not set the evolution flag. The expected tables are:

```text
bronze.bronze_hvfhs_trips
bronze.bronze_taxi_zones
silver.silver_trips
silver.quarantine_trips
ops.source_run_manifest
```

Verification must inspect the Glue job run, catalog schemas, partitions,
locations beneath the warehouse prefix, and Iceberg format. A submitted job ID
alone is insufficient evidence.

## 9. Airflow runtime configuration

The deployment runbook uses an untracked environment file. Configuration
belongs to these categories.

### Source Variables

```text
nyc_landing_uri
nyc_taxi_zone_uri
nyc_taxi_zone_sha256
nyc_hvfhs_2024_01_sha256
nyc_hvfhs_2024_01_size_bytes
```

Each additional month needs its own hash and size Variables.

### Glue job Variables

```text
nyc_bronze_job_name
nyc_great_expectations_job_name
nyc_silver_job_name
nyc_reconciliation_job_name
nyc_publication_job_name
```

### Runtime path/publication Variables

```text
nyc_project_root
nyc_publication_prefix_uri
```

### dbt environment

```text
GLUE_ROLE_ARN
S3_GOLD_PATH
AWS_REGION
```

### Athena Variables/environment

```text
glue_gold_database
athena_workgroup
athena_bytes_scanned_cutoff
athena_smoke_enabled
aws_region
```

Airflow uses `aws_conn_id=None` for Glue operators so the default instance
profile chain is used.

## 10. Monthly DAG runtime trace

### `prepare_month`

Input:

```json
{"year": 2024, "month": 1, "force": false}
```

Resolution:

```text
landing URI + official filename
+ month-specific SHA-256 and size
+ Taxi Zone URI/SHA-256
-> stable run ID and audit XCom
```

No AWS data mutation occurs in this task.

### `bronze_ingestion`

Airflow maps XCom values to Glue script arguments. Terraform default arguments
provide catalog/database/warehouse/publication settings. The Glue job combines
both sets.

The runtime state transition is:

```text
no row/incomplete identical row
-> verified S3 bytes
-> replaced Bronze partitions
-> Bronze snapshot captured
-> bronze_published
```

### `great_expectations_checkpoint`

The task uses only year/month/run ID because it reads the exact Bronze and Ops
tables. Missing/empty structure produces `ge_blocked` and a failed Airflow
task.

### `silver_transform`

The task reads gated Bronze and writes both possible destinations before
recording snapshots/counts:

```text
one classified frame
  -> reason null -> silver_trips
  -> reason set  -> quarantine_trips
```

This is where row-level business behavior belongs.

### Cosmos `dbt_build` and `dbt_result_artifact`

Cosmos runs a single Watcher-mode dbt producer on EC2, while dbt-glue performs
the transformation. Model watchers make the build visible in Airflow. Explicit
vars bind the fact merge to the current month, and a successful build must also
pass dbt tests.

The producer callback then copies the complete artifact, and the downstream
`dbt_result_artifact` task verifies that it exists and has SHA-256 metadata:

```text
etl/dbt_project/target/run_results.json
-> s3://<bucket>/manifests/dbt-results/year=YYYY/month=MM/<run-id>.json
```

The verified S3 URI is XCom output for publication.

### `reconciliation`

The task requires `silver_published`, recounts actual tables, and moves to:

```text
reconciled + gold_row_count + publication_status=pending
```

### `publication_manifest`

The task resolves table metadata and dbt evidence, writes:

```text
s3://<bucket>/manifests/year=YYYY/month=MM/<run-id>.json
```

Only after a successful S3 write does Ops become:

```text
run_status=published
publication_status=published
publication_manifest_uri=<that S3 URI>
published_at=<timestamp>
```

### `athena_smoke`

When `athena_smoke_enabled=false`, the task prints a deferred message. That is
not Athena verification.

When enabled, it checks catalog shape and executes the parameterized Gold
smoke. Retain:

- query execution ID;
- database and workgroup;
- state;
- result location;
- scanned bytes;
- engine time;
- row and distinct-row counts.

## 11. Evidence for a successful baseline

The retained one-month evidence should connect the same:

```text
year/month
source URI
source SHA-256
source byte size
ingestion run ID
identity policy version
```

across:

- upload output/S3 metadata;
- Airflow `prepare_month` XCom;
- Glue arguments;
- Ops manifest;
- publication JSON.

Count evidence must show:

```text
Bronze = Silver + quarantine
Silver = Gold fact for the month
fact row count = distinct row_id count
```

Snapshot evidence must include Bronze, Silver, quarantine, and all six Gold
tables. Publication must include the durable dbt invocation and results URI.

## 12. Failure, clear, and rerun experiment

The controlled experiment should retain evidence for:

1. initial successful month;
2. selected task failure or test failure;
3. Airflow automatic retry or manual clear;
4. identical completed rerun without force;
5. identical forced rerun;
6. changed checksum attempt that remains blocked.

Compare first and final canonical evidence with
`scripts/verify_monthly_rerun.py`.

Expected stable fields:

- all source identity fields;
- run ID and identity policy;
- all layer counts;
- exact set of `row_id`;
- quarantine reason distribution.

Iceberg snapshot IDs may change on an intentional replay. What must remain
stable is the canonical content/evidence contract unless the experiment says
otherwise.

## 13. Four-month release

Only after one month passes:

1. stage and configure four immutable source months;
2. trigger `nyc_hvfhs_four_month_backfill`;
3. verify each monthly child completes before the next starts;
4. retain independent publication evidence per month;
5. verify current fact and marts contain the intended four-month state;
6. preserve per-month count reconciliation.

The backfill DAG accepts one starting month but resolves exactly four monthly
requests. It cannot cross the calendar year.

## 14. The 2025 evolution exercise

This is not part of the first baseline.

### Evolution

Run the initializer with explicit `APPLY_2025_EVOLUTION=true`. It adds nullable
`cbd_congestion_fee` to:

- Bronze trips;
- Silver trips;
- quarantine.

There is no new schema-evolution Glue job and no maintenance suite.

### New snapshot

Ingest one immutable 2025 month. The 2025 identity policy includes the new
field, so its `row_id` policy version differs from 2024.

### Current read

The current fact schema contains nullable fee:

- 2024 fact rows expose null;
- 2025 rows expose the source value when present.

### Historical read

Use the retained 2024 fact snapshot ID as an Athena execution parameter to the
version-travel template. Identifiers must be validated separately; do not
interpolate the snapshot ID.

### Evidence

`scripts/verify_schema_evolution.py` expects:

- different non-empty 2024 and 2025 snapshots;
- positive historical 2024 count;
- current count greater than historical count;
- evidence that the fee is nullable.

## 15. Bounded teardown

`scripts/teardown.ps1` builds a targeted destroy plan for:

- optional runner;
- runner profile/roles/policies;
- Athena workgroup;
- six Glue jobs;
- Glue service role/policy.

It intentionally excludes:

- S3 bucket;
- Glue databases;
- Iceberg tables/data;
- publication artifacts.

The script does not apply the plan. After separate approval and application,
`verify_teardown.py` uses read-only APIs to require temporary resources absent,
temporary prefixes empty, and canonical bucket/databases present.

## 16. PASS, FAIL, and NOT VERIFIED

### PASS

Use PASS only when the named command or retained evidence satisfies all stated
criteria. Examples:

- local test command exits zero;
- publication JSON contains complete required snapshots;
- Athena result is successful and within the scan bound;
- rerun comparator finds no canonical difference.

### FAIL

Use FAIL when a check was run and did not satisfy its contract. Preserve the
command, failure output, stage, and any cleanup performed.

### NOT VERIFIED

Use NOT VERIFIED when execution did not occur or retained evidence is missing.
Do not convert “code exists,” “Terraform validates,” or “task submitted” into
an AWS runtime PASS.

## 17. Common deployment misunderstandings

### “Terraform validate means the lakehouse is deployed”

No. It checks configuration syntax/provider contracts, not account resources.

### “Airflow succeeded, so publication is proven”

Not by itself. Retain Ops state, the S3 JSON, dbt results, and snapshots.

### “Great Expectations validates every trip”

No. It blocks structurally unsafe batches. Silver classifies trip rows.

### “Force lets us replace a changed month”

No. Force replays only an identical source identity.

### “dbt created Gold, so it is published”

No. Reconciliation and durable publication must still succeed.

### “Athena is the lakehouse”

No. Iceberg on S3 is canonical data, Glue is canonical catalog, and Athena is
a bounded reader.

## Learning task

Take the Terraform outputs and upload-script JSON from a hypothetical 2024-01
run. Build a table showing where each value is stored next: Airflow Variable,
environment variable, XCom, Glue argument, Ops column, publication JSON field,
or Athena evidence field.

## Teach-back questions

1. Why does the Glue package have to exist before Terraform validation and how
   does an entry script differ from `--extra-py-files`?
2. What exact configuration moves from Terraform and source upload into
   `prepare_month`, dbt, publication, and Athena?
3. Which resources does bounded teardown remove, which data does it retain,
   and why is the plan never applied automatically?
