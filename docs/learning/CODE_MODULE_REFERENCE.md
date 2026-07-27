# Code-module layer reference

This is the file-by-file guide to the active NYC HVFHV implementation. It
explains why each module exists, its important symbols, who calls it, what it
reads or writes, and the non-obvious rules you need to understand before
changing it.

Start with [README.md](README.md) if the end-to-end architecture is not yet
clear. Paths under `learning_playground/` are exercises and are not deployed.
Paths under `legacy/` are archived and are not part of the active architecture.

## 1. How the modules fit together

```text
etl/contracts/nyc_hvfhs_identity.py
  -> etl/sources/nyc_hvfhs.py
  -> etl/transforms/nyc_hvfhs.py
  -> etl/spark_jobs/nyc_bronze_ingestion.py
  -> etl/spark_jobs/nyc_silver_transform.py

etl/contracts/nyc_hvfhs_quality.py
  -> etl/transforms/nyc_hvfhs.py
  -> etl/spark_jobs/nyc_silver_transform.py

etl/sources/nyc_hvfhs.py
  -> scripts/fetch_source.py
  -> scripts/upload_release_dataset.py
  -> etl/orchestration/nyc_hvfhs_runs.py
  -> etl/dags/nyc_hvfhs_monthly_dag.py

etl/iceberg/catalog.py
  -> etl/spark_jobs/initialize_nyc_iceberg_tables.py

etl/publication/nyc_hvfhs.py
  -> etl/spark_jobs/nyc_publish_manifest.py

silver Iceberg
  -> etl/dbt_project/*
  -> etl/spark_jobs/nyc_quality_checkpoint.py
  -> publication
  -> athena/*
```

The pure modules under `etl/contracts`, `etl/sources`, `etl/transforms`,
`etl/manifests`, `etl/quality`, `etl/orchestration`, and `etl/publication`
allow important semantics to be tested without AWS. Glue entrypoints are the
remote Spark/AWS adapters for those contracts.

## 2. Identity contract

### `etl/contracts/nyc_hvfhs_identity.py`

**Purpose:** the only source of truth for exact-row and probable-trip identity.

Important constants:

- `NULL_TOKEN = "<NULL>"` gives missing values a stable representation.
- `FIELD_SEPARATOR = "\x1f"` prevents ambiguous concatenation.
- `IDENTITY_POLICY_2024` and `IDENTITY_POLICY_2025` version the ordered field
  sets.
- `IDENTITY_COLUMNS_2024` lists every exact-row input in order.
- `IDENTITY_COLUMNS_2025` appends only `cbd_congestion_fee`.
- `BUSINESS_KEY_COLUMNS` declares the smaller analytical key.

Important functions:

| Function | Behavior | Called by |
| --- | --- | --- |
| `identity_policy_version(year)` | selects the auditable policy string | Bronze, Silver, manifest |
| `identity_columns(year)` | returns ordered exact-row fields | schema checks, hash generation |
| `required_identity_columns(year)` | returns required field set | source/GX contracts |
| `canonical_value(column, value)` | canonicalizes null/timestamp/int/decimal/text | Python hashing |
| `row_id(record, year)` | validates all exact inputs and hashes them | source wrapper, pure Bronze |
| `business_trip_key(record)` | validates probable-key inputs and hashes them | source wrapper, pure Bronze |
| `spark_canonical_value(column)` | reproduces Python canonical text in Spark | Spark identity builder |
| `spark_identity_expressions(year)` | returns Spark columns for both hashes and policy | Bronze Spark job |

Non-obvious behavior:

- timestamps are normalized to UTC when timezone-aware, stripped to naive UTC,
  and rendered with six fractional digits;
- decimals are quantized to six fractional digits;
- integer identity values pass through `Decimal` before integer conversion;
- field order is part of the identity policy;
- adding or reordering fields is a breaking identity change, not a refactor;
- derived Silver columns and ingestion metadata cannot affect either hash.

Primary verification:

- `tests/fixtures/nyc_hvfhs/identity_golden_vectors.json`;
- `tests/unit/test_nyc_identity_contract.py`.

The Spark parity test starts local Spark when available and verifies byte-for-
byte agreement with the pinned Python results.

## 3. Row-quality contract

### `etl/contracts/nyc_hvfhs_quality.py`

**Purpose:** one ordered reason policy shared by fixture Python and Glue Spark.

`REASON_PRIORITY` is the public ordering contract. `NUMERIC_REASON_COLUMNS`
keeps negative numeric checks ordered and paired with their reason codes.

`reason_code(row, zone_ids)` is the local implementation. It returns the first
matching reason or `None`.

`spark_reason_expression()` builds a chained Spark `when` expression in the
same order. The Silver job applies duplicate logic afterward because duplicate
classification needs a window and existing-table evidence.

Important distinction:

- invalid business data is a classification result;
- duplicate `row_id` is an exact-identity result;
- `business_trip_key` is not consulted.

Primary verification:

- `tests/unit/test_nyc_quality_priority.py`;
- `tests/unit/test_nyc_hvfhs_transform.py`;
- static Glue checks in `test_nyc_glue_phase_a_contract.py`.

## 4. Source contract

### `etl/sources/nyc_hvfhs.py`

**Purpose:** define official source naming, immutable source facts, required
schema, stable run identity, and local manifest decisions without AWS/Spark.

Data types:

- `SourceFile` contains year, month, URI, checksum, and byte size.
- `SourceManifestEntry` is the early pure source-state representation.
- `SourceStatus` has `discovered` and `processed`.
- `ManifestAction` describes new, skipped, forced, or blocked decisions.
- `SourceContractError` is the contract-level validation exception.

Important functions:

| Function | Responsibility |
| --- | --- |
| `_validate_year_month` | four-digit year and month 1–12 |
| `monthly_trip_filename` | official zero-padded Parquet filename |
| `monthly_trip_uri` | official TLC CloudFront URI |
| `validate_landed_source` | exact `s3://.../<official filename>`, SHA-256 shape, positive size |
| `required_trip_columns` | base plus identity inputs; fee required for 2025+ |
| `validate_trip_schema` | reports missing year-specific fields |
| `inspect_local_source` | streams a file into SHA-256 and obtains size |
| `canonical_row_id` | wraps identity errors as source-contract errors |
| `canonical_business_trip_key` | wraps probable-key errors |
| `stable_run_id` | derives deterministic immutable run ID |
| `manifest_decision` | refuses changed content even under force |

Callers:

- source acquisition scripts;
- pure Bronze transform;
- orchestration audit;
- Airflow `prepare_month`;
- Bronze Glue validation;
- GX required-column construction.

Primary verification: `tests/unit/test_nyc_hvfhs_source.py`.

## 5. Pure Bronze/Silver transform

### `etl/transforms/nyc_hvfhs.py`

**Purpose:** a laptop-scale executable model of Bronze, Silver, quarantine, and
count reconciliation.

Data types:

- `BronzeBatch`: immutable tuple of rows plus source and run ID.
- `SilverBatch`: immutable tuples of accepted and quarantined rows.
- `Reconciliation`: three counts and an `explained` property.

`METADATA_COLUMNS` declares the only fields pure Bronze adds.

Important functions:

| Function | Responsibility |
| --- | --- |
| `_source_filename` | extracts a required filename from the source URI |
| `bronze_records` | validates schema, copies source fields, adds metadata and identities |
| `load_zone_ids` | reads only `LocationID` from fixture CSV |
| `_as_datetime` | safe local timestamp parser |
| `_quarantine_row` | preserves row and appends one reason |
| `transform_silver` | classifies, deduplicates, types, and derives fields |
| `reconcile` | enforces Bronze = Silver + quarantine |

`existing_row_ids` simulates canonical rows outside the batch. A row with a
valid business shape but a seen `row_id` is quarantined as
`DUPLICATE_ROW_ID`.

The remote Glue implementation is not imported from this module because Spark
needs distributed expressions and writes, but both paths share identity and
reason contracts.

Primary verification: `tests/unit/test_nyc_hvfhs_transform.py`.

## 6. Operational manifest state machine

### `etl/manifests/nyc_hvfhs.py`

**Purpose:** model durable monthly lifecycle transitions without Spark SQL.

`RunStatus` defines:

```text
DISCOVERED
BRONZE_PUBLISHED
GE_PASSED / GE_BLOCKED
SILVER_PUBLISHED
RECONCILED
PUBLISHED
FAILED
```

`SourceRunManifest` mirrors the important fields of
`ops.source_run_manifest`. Its transition methods return new frozen dataclass
instances:

- `discovered()` derives run and policy identity;
- `bronze_published()` validates a non-negative count;
- `ge_result()` requires Bronze and records pass/block evidence;
- `silver_published()` requires a gate pass and exact classification;
- `reconciled()` requires Gold fact count equal Silver count;
- `published()` requires a durable artifact URI;
- `failed()` requires stage and message.

`retry_is_safe()` returns false when immutable identity differs. A completed
canonical run needs force; an incomplete identical run may proceed.

This state machine is a semantic oracle for tests. EMR Serverless jobs implement the
durable mutations with Spark SQL against the actual Iceberg table.

Primary verification: `tests/unit/test_nyc_manifest_and_ge.py` and
`tests/unit/test_publication_and_rerun.py`.

## 7. Great Expectations contracts

### `etl/quality/nyc_hvfhs_ge.py`

**Purpose:** construct the installed GX suite and run a fixture-scale
structural equivalent.

- `BLOCKING_EXPECTATION_NAMES` names required columns and non-empty batch.
- `GECheckpointResult` reports pass/fail and suite name.
- `expectation_suite(year)` creates column-existence expectations for the
  year-specific source and identity inputs. Metadata records that row
  validation belongs to the quality contract.
- `evaluate_fixture_ge_checkpoint(rows, ...)` checks non-empty input and the
  structural field set without Spark.

The unused `_zone_ids` parameter remains compatibility-shaped but does not
make zone membership a structural gate.

### `etl/quality/nyc_hvfhs_checkpoint.py`

**Purpose:** fixture equivalent of post-Gold-independent classification
quality checks.

`evaluate_fixture_checkpoint()`:

- calls `reconcile`;
- requires every quarantine row to have a reason;
- requires non-empty unique Silver `row_id`;
- returns counts and distinct count.

The remote reconciliation job also compares the Gold fact count.

Primary verification:

- `tests/unit/test_nyc_manifest_and_ge.py`;
- `tests/unit/test_nyc_phase5_contracts.py`.

## 8. Orchestration contracts

### `etl/orchestration/nyc_hvfhs_runs.py`

**Purpose:** validate manual run requests and make audit/backfill behavior
testable without Airflow.

- `MonthlyRunRequest` restricts supported years and months.
- `RunAudit` serializes run/source facts without credentials.
- `audit_for_source()` requires request/source year-month agreement and derives
  stable run ID.
- `sequential_backfill_requests()` returns exactly four requests and rejects a
  start after September.

Consumers:

- Airflow DAG;
- `scripts/run_e2e.py`;
- unit tests.

## 9. Iceberg table declarations

### `etl/iceberg/catalog.py`

**Purpose:** centralize DDL-visible table schemas and approved evolution.

`TableSpec` contains namespace, table name, ordered columns, and partitions.
`TABLE_SPECS` declares five upstream tables:

1. `bronze.bronze_hvfhs_trips`;
2. `bronze.bronze_taxi_zones`;
3. `silver.silver_trips`;
4. `silver.quarantine_trips`;
5. `ops.source_run_manifest`.

`schema_evolution_ddl()` returns only the three nullable fee additions.
`namespace_ddl()` and `table_ddl()` generate Glue-catalog Iceberg DDL beneath
the supplied warehouse URI.

Important contract details:

- Bronze and quarantine retain source-shaped columns;
- source trip and zone tables are month-partitioned with underscore metadata;
- Silver uses canonical `source_year/source_month`;
- Ops stores counts, snapshots, validation, failure, and publication evidence;
- all tables are Iceberg format version 2 with Snappy Parquet.

Primary verification: `tests/unit/test_iceberg_catalog.py`.

## 10. Publication contract

### `etl/publication/nyc_hvfhs.py`

**Purpose:** validate and serialize the durable publication document without
AWS.

Important symbols:

- `MANIFEST_VERSION` versions the JSON contract.
- `REQUIRED_GOLD_TABLES` fixes the six-table set.
- `TablePublication` represents name/location/count/snapshot.
- `publication_key()` creates a deterministic partitioned key.
- `build_publication_document()` validates and assembles evidence.
- `canonical_json()` sorts keys, removes insignificant whitespace, uses ASCII,
  and appends one newline.

Validation rejects:

- a missing Gold table;
- negative counts;
- missing Bronze/Silver/quarantine snapshots;
- missing Gold location or snapshot;
- unsuccessful/missing dbt invocation evidence.

Primary verification: `tests/unit/test_publication_and_rerun.py`.

## 11. Glue entrypoints

Glue files import `awsglue` and Spark and therefore are remote entrypoints.
Tests inspect them statically or exercise their pure dependencies; local unit
tests do not claim to execute Glue.

### `etl/spark_jobs/initialize_nyc_iceberg_tables.py`

Required argument: `JOB_NAME`, `WAREHOUSE_URI`.

Optional arguments: catalog/database overrides and
`APPLY_2025_EVOLUTION`.

Execution:

1. create GlueContext/Job;
2. create mapped namespaces;
3. create every `TABLE_SPECS` table;
4. optionally execute only approved evolution DDL;
5. commit.

### `etl/spark_jobs/nyc_bronze_ingestion.py`

Required arguments:

- `JOB_NAME`;
- `SOURCE_URI`, `SOURCE_YEAR`, `SOURCE_MONTH`;
- `SOURCE_CHECKSUM`, `SOURCE_SIZE_BYTES`;
- `INGESTION_RUN_ID`;
- `TAXI_ZONE_URI`, `TAXI_ZONE_CHECKSUM`.

Key helpers:

- `_optional_arg()` reads Glue optional flags;
- `_table()` resolves catalog/database overrides;
- `_verify_landed_object()` verifies S3 hash metadata and optional size;
- `_merge_manifest()` upserts state and clears stale downstream evidence when
  a Bronze replay starts;
- `_may_process()` blocks identity change or skips completed identical source.

`main()` sets UTC, validates source/run identity, verifies both S3 objects,
loads one Parquet file, validates schema, generates identities, writes Bronze
and zones with partition replacement, captures a snapshot, and updates Ops.

Failure handling truncates messages to the manifest field boundary and
re-raises so Airflow sees a failed task.

### `etl/spark_jobs/nyc_great_expectations_checkpoint.py`

Required arguments: `JOB_NAME`, year, month, run ID.

Key helpers:

- `_suite(year)` creates required-column expectations;
- `_persist()` writes pass/block summary and failure metadata.

`main()` requires `bronze_published`, filters the exact run, constructs an
ephemeral GX Spark batch, validates, checks non-empty, persists state, and
raises on block.

### `etl/spark_jobs/nyc_silver_transform.py`

Required arguments: `JOB_NAME`, year, month, run ID.

`main()`:

- requires GX pass or a valid Silver-stage retry;
- confirms policy version;
- resolves zone IDs;
- detects cross-month and within-batch exact duplicates;
- applies the shared reason expression;
- selects/derives canonical Silver columns;
- calculates both counts from the classified frame;
- replaces month partitions;
- captures snapshots and updates Ops.

The current 2025 column branch is post-baseline code and remains
**NOT VERIFIED** in Glue. Any change to it must retain the rule that 2024 does
not invent a source value and 2025 uses the evolved nullable column.

### `etl/spark_jobs/nyc_quality_checkpoint.py`

The historical filename says “quality checkpoint”; its active role is
reconciliation.

It requires `silver_published`, counts month-scoped Silver, quarantine, and
fact rows, calculates four named differences, and changes state to
`reconciled` only when all differences equal zero.

### `etl/spark_jobs/nyc_publish_manifest.py`

Required arguments: `JOB_NAME`, year, month, run ID, `DBT_RESULT_URI`.

Key helpers:

- `_snapshot_id()` reads the current Iceberg snapshot metadata table;
- `_location()` extracts table location from extended describe output.

`main()` requires `reconciled`, collects all six Gold publications, reads dbt
results from S3, constructs canonical JSON, writes it with SHA-256 object
metadata, then conditionally updates Ops to `published`.

## 12. Airflow DAG module

### `etl/dags/nyc_hvfhs_monthly_dag.py`

**Purpose:** define both manual Airflow 3 DAGs and no transformations.

Module constants:

- `MONTHLY_DAG_ID`;
- `BACKFILL_DAG_ID`;
- `DEFAULT_ARGS`.

Pure helpers:

- `_prepare_month()` reads Airflow Variables, builds source/audit facts, and
  returns the XCom payload;
- `_monthly_params()` and `_backfill_params()` define validated UI/API params;
- nested `_prepare_backfill()` serializes exactly four requests.

Monthly operators:

| Task | Operator | Remote target |
| --- | --- | --- |
| `prepare_month` | `PythonOperator` | local runner Python |
| `bronze_ingestion` | `EmrServerlessStartJobOperator` | Bronze EMR Serverless job |
| `great_expectations_checkpoint` | `EmrServerlessStartJobOperator` | GX EMR Serverless job |
| `silver_transform` | `EmrServerlessStartJobOperator` | Silver EMR Serverless job |
| `dbt_build` | Cosmos `DbtTaskGroup` Watcher | dbt-redshift build/tests and durable result callback |
| `dbt_result_artifact` | `PythonOperator` | require the callback-uploaded dbt result before reconciliation |
| `reconciliation` | `EmrServerlessStartJobOperator` | reconciliation EMR Serverless job |
| `publication_manifest` | `EmrServerlessStartJobOperator` | publication EMR Serverless job |
| `athena_smoke` | `BashOperator` | Python Athena verifier when enabled |

The four backfill trigger operators set `wait_for_completion=True` and are
chained, so month N+1 does not start until month N finishes.

Primary verification: `tests/unit/test_nyc_airflow_dag.py` stubs Airflow
modules, imports the DAG, and asserts exact task topology and properties.

### `etl/orchestration/nyc_hvfhs_cosmos.py`

**Purpose:** make Cosmos' temporary dbt working directory safe for the
publication contract.

- `archive_dbt_run_results()` is the Watcher producer callback. It reads the
  complete `target/run_results.json`, accepts only non-empty all-success/pass
  results with an invocation ID, then uploads it with SHA-256 metadata.
- `require_dbt_result_artifact()` is the explicit downstream handoff. It heads
  the deterministic S3 object and returns its URI only when its size and
  checksum metadata are present.
- `tests/unit/test_nyc_cosmos_dbt_artifacts.py` proves the callback/verification
  pair without AWS.

## 13. dbt project

### `etl/dbt_project/dbt_project.yml`

Declares project/profile names, default month variables, model paths, and
default Redshift table materialization.

### `etl/dbt_project/profiles.yml`

Two targets:

- `redshift`: Redshift Serverless IAM authentication, environment-provided
  endpoint/workgroup/account/region, and local `gold` schema;
- `ci`: non-routable database credentials used only with introspection and
  cache population disabled for credential-independent parse/compile.

### `models_nyc/sources.yml`

Declares Bronze zones and Silver trips. Source tests require non-null zone IDs
and unique/non-null exact identity fields and timestamps.

### Dimensions

`dimensions/dim_date.sql`

- distinct non-null Silver pickup dates;
- integer `yyyyMMdd` key and calendar attributes.

`dimensions/dim_operator.sql`

- distinct non-null Silver operator codes.

`dimensions/dim_zone.sql`

- groups repeated month-scoped Bronze lookup rows by `LocationID`;
- uses `max` for stable lookup attributes.

### Fact

`facts/fct_trips.sql`

- incremental Iceberg merge;
- unique key `row_id`;
- partitioned by source year/month;
- selects only requested month from Silver;
- passes through identity, timestamps, zones, measures, flags, derivations,
  and run metadata;
- outputs null fee for 2024 and the evolved source value for 2025;
- disables snapshot expiration behavior.

### Marts

`marts/mart_hourly_zone_demand.sql`

- grain: source month + pickup date + hour + pickup zone;
- count, total miles, average fare, total driver pay.

`marts/mart_operator_metrics.sql`

- grain: source month + operator;
- trip count plus fare/pay/tip/mileage aggregates.

### Tests

`models_nyc/schema.yml` declares all model uniqueness, not-null, and
relationship tests.

`tests/fct_trips_reconciles_to_silver.sql` returns a row only when complete
fact and Silver counts differ; any returned row fails dbt build.

`tests/unit/test_dbt_gold_contract.py` statically locks exactly six SQL files,
grains, identity keys, tests, and Glue Iceberg settings.

## 14. Athena modules

### `athena/query_runner.py`

`AthenaQueryError` represents failed, cancelled, or timed-out query execution.

`AthenaQueryResult` retains:

- execution ID;
- columns and rows;
- scanned bytes and engine milliseconds;
- result location;
- database, workgroup, and terminal state.

`AthenaQueryRunner.run()` validates required inputs, supplies execution
parameters and optional idempotency token, polls status, stops a timeout, and
delegates result pagination.

`_results()` removes the first-page header and preserves null cell values.

### `athena/verify_gold.py`

`EXPECTED_GOLD_COLUMNS` locks the required catalog surface.

`verify_gold_catalog()` rejects missing and extra Gold tables and missing
columns.

`verify_gold_smoke()` executes the parameterized SQL and verifies result
shape, non-empty unique exact identities, timestamp bounds, optional expected
count, and scan bound.

`main()` is the Airflow-callable CLI and prints query evidence on PASS.

### `athena/sql/`

- `gold_smoke.sql`: month-filtered fact integrity.
- `mart_hourly_zone_demand.sql`: month-filtered top 25.
- `iceberg_history.sql`: newest 100 fact history/snapshot records.
- `time_travel.sql.tmpl`: validated identifiers plus bound snapshot parameter,
  maximum 100 rows.

Primary verification:

- `tests/unit/test_athena_runner.py`;
- `tests/unit/test_athena_sql.py`;
- `tests/contract/test_athena_smoke.py`.

## 15. Source and release scripts

### `scripts/fetch_source.py`

Downloads official files, checks advertised size and free space, uses a
temporary suffix, and reports local size/hash. Network and real data are not
used by credential-independent tests.

### `scripts/upload_release_dataset.py`

`UploadObject` describes one planned object. `upload_plan()` creates the trip
and lookup plans. `_upload_immutable()` implements S3 idempotence and
verification. `main()` prints both object evidence and Airflow Variables;
mutation requires explicit `--execute`.

### `scripts/package_spark_jobs.py`

Builds deterministic Glue code and runtime metadata. Terraform reads the
resulting file, so package creation precedes Terraform plan/validate.

### `scripts/run_e2e.py`

Creates one-month or four-month Airflow CLI command arrays. `--execute` is
reserved and intentionally blocked.

### `scripts/run_smoke.ps1` and `scripts/run_release.ps1`

Build the Glue package and print bounded command plans. They do not contact AWS
or trigger Airflow.

### `scripts/bootstrap_airflow_runner.sh`

Rejects static key environment variables and prepares Airflow/project
directories. It does not install or start the final reviewed Airflow image.

### `scripts/reconcile_outputs.py`

A lightweight JSON evidence validator. It requires structural validation
success, non-negative counts, Bronze classification equality, and
Gold-to-Silver equality. It is not the remote EMR Serverless reconciliation job.

### `scripts/verify_monthly_rerun.py`

Compares two retained evidence JSON documents for deterministic identity,
counts, row IDs, and reason distributions.

### `scripts/verify_schema_evolution.py`

Checks retained snapshot/count/nullability evidence for the sole advanced
2025 exercise. It does not execute schema evolution.

### `scripts/teardown.ps1`

Creates a bounded targeted destroy plan. It deliberately contains no apply and
excludes canonical storage/databases.

### `scripts/verify_teardown.py`

Uses read-only AWS calls to check the expected retained-data state after a
separately approved destroy-plan apply.

### `scripts/check_repository_hygiene.py`

Uses Git’s tracked-file list to reject data, environments, Terraform
state/plans, caches, Parquet, and obvious AWS keys. It also ensures required
deployment scripts are not accidentally ignored.

## 16. Terraform files

### `terraform/main.tf`

Pins Terraform/AWS provider ranges, configures default tags, reads current
account identity, and exports bucket, warehouse, databases, role, job names,
runner profile, and publication prefix.

### `terraform/variables.tf`

Defines region/environment/project naming, required private bucket name,
prefixes, Athena scan cutoff, Glue worker bounds, package path/key, and
optional runner settings.

### `terraform/s3.tf`

Creates the protected canonical bucket with versioning, AES256 encryption, and
full public-access block.

### `terraform/glue_catalog.tf`

Creates the retained Glue Catalog databases. Bronze, Silver, and Ops remain
active Iceberg namespaces; the legacy Gold catalog database is not a dbt
target.

### `terraform/emr_serverless.tf`

Defines common Iceberg Spark arguments, uploads the package and six scripts,
and creates one persistent EMR Serverless Spark application:

- initialize;
- Bronze;
- Silver;
- reconciliation (resource label remains `quality`);
- Great Expectations;
- publication.

Airflow submits each script to that application; auto-start and auto-stop are
application settings, not per-DAG lifecycle operations.

### `terraform/redshift_serverless.tf`

Creates one namespace and workgroup, a Spectrum role limited to upstream
Bronze/Silver S3 and Glue reads, the `bronze_external` and `silver_external`
schemas, and the local managed `gold` schema. Terraform manages the namespace
admin password in Secrets Manager; dbt itself uses IAM authentication.

### `terraform/iam.tf`

Defines service trust and least-scope policies for EMR Serverless, Redshift
Spectrum, the Airflow runner, S3 artifact handoffs, and bounded Athena access.

### `terraform/athena.tf`

Creates the enforced workgroup, existing-bucket results path, metrics, and
per-query byte cutoff.

### `terraform/airflow_runner.tf`

Creates the temporary EC2 runner only when an AMI is supplied. It requires a
subnet, uses the instance profile, encrypts the root volume, and requires
IMDSv2.

## 17. Runtime image and dependency files

### `Dockerfile.airflow`

Starts from Airflow 3.3/Python 3.12, installs pinned runner dependencies, copies
ETL/Athena/scripts, sets `PYTHONPATH`, and points Airflow to the repository DAG.
Spark transformations still run remotely.

### `requirements-airflow.txt`

Pins the reviewed orchestration runtime: Airflow/providers, Boto3, dbt-core,
dbt-redshift, and Great Expectations.

### `requirements-ci.txt`

Pins the credential-independent CI environment, including the same
dbt-redshift adapter used by the Airflow image.

### `requirements.txt`

Broad developer dependency ranges. CI and Airflow use their narrower pinned
files for repeatability.

## 18. Test-suite map

| Test file | Main proof |
| --- | --- |
| `test_nyc_hvfhs_source.py` | filenames, schema, immutable decisions, fixtures |
| `test_nyc_identity_contract.py` | Python/Spark golden-vector parity |
| `test_nyc_hvfhs_transform.py` | Bronze fidelity, classification, rerun |
| `test_nyc_quality_priority.py` | exact first-reason ordering |
| `test_nyc_manifest_and_ge.py` | gate/state/retry semantics |
| `test_nyc_phase5_contracts.py` | checkpoint, audit, four-month bounds |
| `test_iceberg_catalog.py` | table schemas, partitions, evolution |
| `test_nyc_glue_phase_a_contract.py` | remote-job static safety contracts |
| `test_nyc_airflow_dag.py` | import and exact topology |
| `test_dbt_gold_contract.py` | exactly six models, grains, identities |
| `test_publication_and_rerun.py` | complete publication, canonical serialization, and evidence |
| `test_athena_runner.py` | polling, pagination, failures, timeout |
| `test_athena_sql.py` | exactly four bounded read-only statements |
| `test_athena_smoke.py` | catalog/result/scan contract |
| `test_source_staging_scripts.py` | upload idempotence and fetch capacity |
| `test_phase_c_deployment.py` | deterministic package, Terraform/release scope |

Tests under `tests/unit` and `tests/contract` are credential-independent.
Their pass does not convert any AWS item from **NOT VERIFIED** to verified.

## 19. Safe change routes

When changing behavior, follow the owner:

| Desired change | Correct owner | Also update |
| --- | --- | --- |
| exact identity fields/format | identity contract | golden vectors, Spark parity, schemas/docs |
| row validation priority | quality contract | pure transform, Spark tests, reason docs |
| required source fields | source contract | GX, identity if applicable, tests |
| table schema/partition | Iceberg catalog | Glue selection, dbt sources, Terraform/runtime evidence |
| orchestration order | DAG | topology test, deployment guide |
| Gold grain | dbt SQL/schema | reconciliation, Athena, tests |
| publication evidence | publication contract | Glue adapter, tests, evidence template |
| query surface | Athena SQL/verifier | exactly-four/read-only tests, IAM if scope changes |

Do not duplicate a locked semantic in a new “helper” module. Change the source
of truth and every adapter/test that proves parity.

## Learning task

Choose one valid fixture row and one invalid fixture row. Write down the exact
function or Spark expression that touches each row from `bronze_records` or
Bronze Glue ingestion through `fct_trips` or quarantine. Include the identity
fields, first reason decision, partition columns, and final publication
evidence.

## Teach-back questions

1. Which modules are semantic sources of truth and which modules are remote
   adapters, and why is that separation useful?
2. If Python and Spark generate different `row_id` values, which tests and
   downstream components fail or become unsafe?
3. Why does `nyc_publish_manifest.py` consume a callback-uploaded S3
`run_results.json` instead of trusting that the Cosmos `dbt_build` group once
reported success?
