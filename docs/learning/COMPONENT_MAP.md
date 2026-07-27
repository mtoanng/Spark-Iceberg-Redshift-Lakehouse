# Component layer map

This guide explains the repository at component depth: what each component
owns, what it consumes and produces, how it collaborates with adjacent
components, and where it fails. For individual functions and files, continue
to [CODE_MODULE_REFERENCE.md](CODE_MODULE_REFERENCE.md).

## 1. Component interaction map

```text
Source acquisition
  scripts.fetch_source
        |
        v
Immutable landing
  scripts.upload_release_dataset -> S3 landing/reference + Airflow facts
        |
        v
Orchestration
  Airflow monthly DAG -> XCom audit + EMR Serverless/dbt/Athena task calls
        |
        +---------------------------------------------------------------+
        |                                                               |
        v                                                               |
Bronze component                                                        |
  EMR Bronze -> Bronze trips/zones + ops manifest + snapshot            |
        |                                                               |
        v                                                               |
Structural gate                                                         |
  EMR + Great Expectations -> ge_passed/ge_blocked                      |
        |                                                               |
        v                                                               |
Canonicalization component                                              |
  EMR Silver -> Silver + quarantine + snapshots                         |
        |                                                               |
        v                                                               |
Analytical modeling                                                     |
  Redshift external schemas -> dbt-redshift managed Gold + run_results  |
        |                                                               |
        v                                                               |
Reconciliation                                                          |
  EMR reconciliation -> reconciled/pending                              |
        |                                                               |
        v                                                               |
Publication                                                             |
  EMR publication -> durable JSON -> published                          |
        |                                                               |
        v                                                               |
Serving                                                                 |
  Athena catalog check + bounded read-only smoke -----------------------+
```

The backward relationship at the right is evidence, not a data write:
Athena reports whether the published analytical state is queryable within the
expected schema and cost bound.

## 2. End-to-end component table

| Component | Primary code | Input | Output | Reads | Writes | Primary failure boundary |
| --- | --- | --- | --- | --- | --- | --- |
| Source contract | `etl/sources/`, `etl/contracts/` | year/month and source rows/files | source identity, row identities, schema decisions | local files/mappings | none | invalid month, schema, hash, size, or identity input |
| Source staging | `scripts/fetch_source.py`, `scripts/upload_release_dataset.py` | official URLs and local staging | immutable S3 objects and Airflow-variable JSON | public source/local files/S3 head | S3 landing/reference | partial download or changed object |
| Catalog/table contract | `etl/iceberg/catalog.py`, initializer EMR Serverless job | warehouse URI and optional evolution flag | namespaces/table DDL | code constants | Glue Catalog/Iceberg table metadata | invalid DDL or unapproved evolution |
| Orchestration | monthly/backfill Airflow DAG | manual params, Variables, environment | ordered remote tasks and XCom audit | Variables/XCom | Airflow metadata | missing config or task failure |
| Bronze | Bronze EMR Serverless job | landed month, zones, immutable facts | Bronze partitions, run state, snapshot | S3 source, manifest | Bronze + Ops | identity mismatch/read/write failure |
| Structural gate | GX EMR Serverless job | requested Bronze run | `ge_passed` or `ge_blocked` summary | Bronze + Ops | Ops | missing/empty structural batch |
| Silver/quarantine | Silver EMR Serverless job | gated Bronze and zones | two classified partitions and snapshots | Bronze/Silver/Ops | Silver/quarantine/Ops | invalid policy or failed classification/write |
| Gold | dbt-redshift through Cosmos | external Silver and Bronze schemas | three dimensions, fact, two marts, dbt result | Glue/S3 through Spectrum; managed Gold | Redshift Gold + dbt results S3 | connection, model, or data-test failure |
| Reconciliation | reconciliation EMR Serverless job | manifest and actual layer tables | reconciled state and Gold count | legacy Ops/Silver/quarantine/Gold Iceberg contract | Ops | Redshift adapter is outside this phase |
| Publication | publication EMR Serverless job + pure builder | reconciled row, table metadata, dbt results | durable canonical JSON and published state | legacy Ops/Gold Iceberg metadata/S3 contract | manifests S3 + Ops | Redshift evidence adapter is outside this phase |
| Athena serving | runner, verifier, four SQL artifacts | published Gold and parameters | query audit/result | legacy Glue Gold + Gold S3 contract | Athena result prefix | Redshift serving migration is outside this phase |
| Infrastructure | Terraform | approved variables and package | AWS resources and outputs | configuration/package | AWS control plane | plan/apply/IAM/resource error |
| Evidence verification | rerun/evolution/teardown scripts | retained JSON or live read-only APIs | PASS/FAIL result | evidence/AWS APIs | none | incomplete or divergent evidence |

## 3. Source acquisition and immutable landing

### Responsibilities

- Generate the official `fhvhv_tripdata_YYYY-MM.parquet` name and URI.
- Stream the monthly Parquet and Taxi Zone CSV to ignored local `data/`.
- Keep 256 MiB free beyond advertised content length.
- Write to a `.part` file and rename only after a complete download.
- Calculate local SHA-256 and byte size without interpreting Parquet contents.
- Plan S3 keys under `landing/` and `reference/`.
- Upload with `sha256` object metadata.
- Return the exact Airflow Variables needed to bind a run.

### Component boundary

The acquisition component does not start Spark, create tables, or update the
operational manifest. It proves what bytes should be processed. Bronze later
re-verifies the same facts in AWS before reading them.

### Idempotence

| Existing S3 state | Decision |
| --- | --- |
| no object | upload and verify |
| same byte size and `sha256` metadata | return `already-present` |
| different size or hash | fail and refuse overwrite |

### Configuration output

The upload script emits:

- `nyc_landing_uri`;
- `nyc_taxi_zone_uri`;
- `nyc_taxi_zone_sha256`;
- `nyc_hvfhs_YYYY_MM_sha256`;
- `nyc_hvfhs_YYYY_MM_size_bytes`.

Those values are configuration facts, not credentials.

## 4. Identity and contract component

### Responsibilities

This pure-Python component centralizes:

- supported month/file naming;
- year-specific required columns;
- source object validation;
- exact-row hash policy;
- probable-trip analytical hash policy;
- stable run ID;
- local manifest decisions;
- Spark expressions byte-compatible with Python.

### Input/output contract

```text
SourceFile(year, month, s3_uri, sha256, size)
    -> validated immutable monthly identity
    -> stable ingestion_run_id

source row + year
    -> row_id
    -> business_trip_key
    -> identity_policy_version
```

### Consumers

- staging scripts use filename, URI, and local inspection functions;
- Airflow uses `SourceFile` and stable run ID through orchestration contracts;
- Bronze uses source validation, schema validation, and Spark identity
  expressions;
- Silver verifies policy version;
- publication persists policy version;
- tests pin Python/Spark parity.

### Non-negotiable rule

Only `row_id` controls exact deduplication and fact merge.
`business_trip_key` remains analytical evidence and cannot silently affect row
classification.

## 5. Catalog and Iceberg component

### Responsibilities

`etl/iceberg/catalog.py` is the credential-independent declaration of Bronze,
Silver, quarantine, and Ops Iceberg schemas. It produces:

- `CREATE NAMESPACE IF NOT EXISTS`;
- `CREATE TABLE IF NOT EXISTS ... USING iceberg`;
- format-version 2 and Snappy properties;
- bounded S3 locations beneath the supplied warehouse URI;
- exactly three approved 2025 `ADD COLUMN` statements.

### Runtime adapter

`initialize_nyc_iceberg_tables.py` starts a normal Spark session, applies namespace
overrides, creates all declared upstream tables, and optionally executes the
2025 evolution. It does not create Gold; dbt owns Gold model creation.

### Dependency

Terraform creates Glue databases before or alongside the initializer job and
supplies the catalog/warehouse Spark configuration. The initializer still uses
`CREATE ... IF NOT EXISTS` so the table step is repeatable.

## 6. Airflow orchestration component

### Monthly DAG contract

| Property | Value |
| --- | --- |
| DAG ID | `nyc_hvfhs_monthly` |
| Schedule | manual only |
| Parameters | `year`, `month`, `force` |
| Active runs | one |
| Task retries | two, five-minute delay |
| AWS authentication | default SDK chain via EC2 instance profile |
| Transformation logic | none |

`prepare_month` is the binding point between a human request and immutable
source configuration. Its XCom result is the audit payload passed to every
remote task.

### Backfill DAG contract

| Property | Value |
| --- | --- |
| DAG ID | `nyc_hvfhs_four_month_backfill` |
| Schedule | manual only |
| Expansion | exactly four requests |
| Ordering | strictly sequential |
| Year crossing | forbidden by start-month validation |
| Work performed | triggers and waits for monthly DAG runs |

### Configuration classes

Airflow uses three kinds of configuration:

1. Variables for source facts, EMR application/script paths, Gold/Athena
   settings, and publication prefix.
2. Environment variables for dbt and the Athena Bash task.
3. XCom for the run-specific audit and durable dbt results URI.

The exact mapping is in
[DEPLOYMENT_WALKTHROUGH.md](DEPLOYMENT_WALKTHROUGH.md).

## 7. Bronze component

### Preconditions

- complete required Spark entrypoint arguments;
- valid landed `s3://` URI for the requested official filename;
- 64-character source and Taxi Zone checksums;
- positive source size;
- `INGESTION_RUN_ID` equals the stable run ID;
- S3 object metadata and content length match;
- no conflicting year/month operational identity.

### Processing

Bronze reads one Parquet object, verifies required columns, applies identity
expressions, appends ingestion metadata, persists once for count/write, and
replaces the requested partitions. Taxi Zones are read as headered CSV and
written to their month partition.

### Output ownership

- complete source rows in `bronze_hvfhs_trips`;
- reference rows in `bronze_taxi_zones`;
- Bronze row count and snapshot in Ops;
- `bronze_published` state.

### Rerun guard

An already `silver_published`, `reconciled`, or `published` identical month is
skipped unless force is true. A different URI, checksum, or size is rejected
even when force is true.

## 8. Great Expectations component

### Preconditions

The operational row for the exact year/month/run ID must be
`bronze_published`.

### Processing and output

The component filters the exact Bronze run, creates an ephemeral GX Spark data
source/asset/batch, checks required columns, and independently verifies the
frame is non-empty. It stores a compact JSON summary in Ops.

| Outcome | State | Validation status | Downstream |
| --- | --- | --- | --- |
| structural success | `ge_passed` | `passed` | Silver allowed |
| missing/empty failure | `ge_blocked` | `blocked` | Spark raises; Silver blocked |

This component does not clean, transform, deduplicate, or quarantine rows.

## 9. Silver and quarantine component

### Preconditions

- exact manifest row is `ge_passed`; or
- it is a Silver-stage retry with validation still `passed`.

### Collaborating inputs

- requested Bronze trip partition/run;
- requested Taxi Zone partition;
- `row_id` values already canonical outside the requested month;
- shared identity-policy resolver;
- shared ordered Spark reason expression.

### Classification algorithm

```text
Bronze row
  -> resolve pickup/drop-off zones
  -> compute first business reason by priority
  -> number same-row_id occurrences deterministically
  -> if otherwise valid but repeated/already published: DUPLICATE_ROW_ID
  -> reason is null: canonical Silver
  -> reason is set: quarantine
```

The classified DataFrame is persisted so counts and both outputs refer to the
same classification result.

### Output ownership

- requested `silver_trips` partition;
- requested `quarantine_trips` partition;
- counts and current snapshots;
- `silver_published` state.

## 10. dbt-redshift Gold component

### Sources

- `silver_external.silver_trips`;
- `bronze_external.bronze_taxi_zones`.

### Model dependency graph

```text
silver_trips -> dim_operator
silver_trips -> dim_date
bronze_taxi_zones -> dim_zone
silver_trips -> fct_trips
fct_trips -> mart_hourly_zone_demand
fct_trips -> mart_operator_metrics
```

Relationships tests connect fact foreign keys to all three dimensions.
Uniqueness/not-null tests enforce model grains. A singular test compares the
complete Silver and fact counts.

### Runtime behavior

dbt runs on the temporary Airflow runner and connects to the private Redshift
Serverless workgroup with IAM authentication from the default AWS credential
chain. Redshift Spectrum resolves the external schemas through Glue Data
Catalog. The fact merge processes only the requested month and remains keyed
by `row_id`; the other five relations are Redshift-managed tables. No Gold
model carries Spark, Iceberg, partition, distribution, or sort-key config.

### Durable handoff

Airflow copies `target/run_results.json` to S3. The explicit artifact task
requires that object before reconciliation, so a failed model or test blocks
both reconciliation and publication.

### Preserved downstream boundary

The DAG still orders reconciliation, publication, and Athena after dbt. Their
current implementations read Glue-catalogued Gold Iceberg metadata, however,
and were deliberately not rewritten in this phase. Live execution of those
three components against Redshift-managed Gold is **NOT VERIFIED**.

## 11. Reconciliation component

### Required state

The operational row must be `silver_published`.

### Actual counts

The job recounts:

- Silver for requested `source_year/source_month`;
- quarantine for requested `_source_year/_source_month`;
- Gold fact for requested `source_year/source_month`.

It calculates four explicit differences and fails if any is non-zero. Success
sets Gold count, state `reconciled`, and publication `pending`.

This job is read-only for data tables. Its only write is operational state.

## 12. Publication component

### Pure contract

`etl/publication/nyc_hvfhs.py` defines:

- the fixed six-table requirement;
- table publication records;
- deterministic year/month/run object keys;
- validation of layer counts/snapshots, Gold metadata, and dbt evidence;
- sorted canonical JSON bytes.

### Remote adapter

The publication Spark job:

- requires a reconciled manifest;
- reads latest Iceberg snapshots;
- reads table locations through `DESCRIBE TABLE EXTENDED`;
- counts all six Gold tables;
- reads and validates S3 dbt results;
- writes JSON and SHA-256 metadata;
- conditionally updates the still-reconciled Ops row to `published`.

The S3 write precedes the state update. That order is the component’s most
important safety property.

## 13. Athena serving component

### Scope

Exactly four SQL artifacts exist:

1. month-filtered Gold fact smoke;
2. month-filtered top-25 hourly zone demand;
3. bounded Iceberg history/snapshot metadata;
4. bounded parameterized version travel.

Tests reject multiple statements and mutation keywords.

### Runner responsibilities

`AthenaQueryRunner` starts one query, polls terminal state, stops timed-out
queries, paginates results, removes the first-page header, and returns query
audit statistics.

### Verifier responsibilities

`verify_gold` first checks that the Gold catalog contains exactly the expected
six tables and required columns. It then checks the fact smoke result,
uniqueness, timestamp bounds, optional publication count, and scan byte bound.

## 14. Infrastructure component

### Resource groups

| Terraform area | Resources |
| --- | --- |
| S3 | protected bucket, versioning, AES256 encryption, public-access block |
| Catalog | four Glue databases |
| Glue | shared zip, six scripts, six jobs |
| IAM | Glue role/policy, runner role/profile/policies, Athena access |
| Athena | enforced workgroup, result location, scan cutoff |
| Airflow | optional IMDSv2 EC2 instance with instance profile |

### Important permission split

- Glue can read landing/reference/job artifacts and manage canonical
  warehouse/manifest objects plus catalog tables.
- Airflow can submit jobs to the named EMR Serverless application, obtain
  Redshift IAM credentials, copy dbt result
  artifacts, and use the one Athena workgroup.
- Athena access reads only Gold catalog/table objects and writes only the
  Athena results prefix.

The bucket and catalog databases are retained by bounded teardown.

## 15. Packaging and release-control component

`package_spark_jobs.py` builds a deterministic zip:

- includes Python beneath `etl/`, excluding DAGs and caches;
- pins archive timestamps;
- embeds a runtime manifest;
- declares GX as a Glue-installed dependency;
- prints the artifact SHA-256.

`run_smoke.ps1` and `run_release.ps1` build/verify that package, then print
Airflow CLI command plans. `run_e2e.py --execute` intentionally exits; these
scripts do not silently perform cloud work.

## 16. Evidence and teardown components

### Rerun comparator

Compares immutable source fields, stable run/policy, all layer counts, exact
row ID sets, and quarantine reason distributions.

### Schema-evolution verifier

Requires different retained 2024/2025 snapshots, a positive historical 2024
count, larger current count, and nullable congestion-fee evidence.

### Teardown planner/verifier

The PowerShell planner creates a targeted destroy plan only. It does not apply
it. The read-only verifier later expects compute/query/IAM resources absent,
temporary prefixes empty, and canonical bucket/databases present.

## 17. Component-to-test map

| Component | Main tests |
| --- | --- |
| source/landing | `test_nyc_hvfhs_source.py`, `test_source_staging_scripts.py` |
| identity | `test_nyc_identity_contract.py` |
| Bronze/Silver pure logic | `test_nyc_hvfhs_transform.py`, `test_nyc_quality_priority.py` |
| manifest/GX | `test_nyc_manifest_and_ge.py` |
| Iceberg DDL | `test_iceberg_catalog.py` |
| Glue static contracts | `test_nyc_glue_phase_a_contract.py` |
| Airflow topology | `test_nyc_airflow_dag.py` |
| dbt Gold | `test_dbt_gold_contract.py` and dbt parse/compile |
| publication/rerun/evolution | `test_publication_and_rerun.py` |
| Athena | `test_athena_runner.py`, `test_athena_sql.py`, `test_athena_smoke.py` |
| packaging/Terraform/release | `test_phase_c_deployment.py` |
| teardown | unit coverage through deployment/publication tests and verifier review |

All runtime AWS behavior remains **NOT VERIFIED** until retained cloud
evidence exists.
