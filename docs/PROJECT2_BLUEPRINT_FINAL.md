# PROJECT 2 — JUNIOR-FIRST IMPLEMENTATION BLUEPRINT

Repository: `mtoanng/Spark-Iceberg-DuckDB-Lakehouse`

Target name: **NYC High-Volume Ride-Hailing Lakehouse**

Status: **Final architecture, junior-first delivery plan; Athena migration recorded 2026-07-21**

This file is the source of truth for Project 2. The final platform represents durable batch and lakehouse architecture, but the first release must remain small enough to complete, run, and explain as a junior Data Engineer.

---

## 1. Goal

Build a monthly batch lakehouse from official NYC TLC High Volume For-Hire Vehicle trip records.

The first mandatory path is:

```text
one monthly Parquet file plus Taxi Zone lookup
-> S3 Landing
-> Airflow 3
-> Glue/PySpark Bronze ingestion
-> Iceberg Bronze
-> mandatory Great Expectations checkpoint
-> Glue/PySpark Silver transformation
-> Iceberg Silver and quarantine
-> dbt-glue Gold
-> publication manifest
-> Amazon Athena analytical serving
```

Then add:

```text
Airflow 3 orchestration
mandatory Great Expectations and publication reconciliation
idempotent reruns
four-month backfill
Terraform and CI
```

Advanced Iceberg maintenance and 2025 schema evolution are retained as future extensions, not junior MVP blockers.

There is no machine learning, recommendation system, MongoDB, unrestricted SQL API, or frontend application.

---

## 2. Dataset

Use NYC TLC monthly **High Volume For-Hire Vehicle (HVFHV)** Parquet files plus the Taxi Zone lookup.

### Junior scope

- MVP: one 2024 month, default `2024-01`.
- Junior complete: four consecutive 2024 months.
- Optional scale run: all of 2024.
- Advanced schema evolution: one 2025 month containing the new congestion-fee field.

Do not commit source monthly files to Git.

Commit deterministic fixtures containing:

- valid trips;
- one duplicate;
- one invalid pickup/drop-off timestamp;
- one invalid zone;
- one negative metric;
- a minimal 2025 schema example.

Create `docs/DATASET_NOTES.md` with exact URLs, file sizes, checksums, schemas, and selected months.

---

## 3. Final architecture

```text
NYC TLC monthly HVFHV Parquet
+ Taxi Zone lookup
        |
        v
S3 Landing
        |
        v
Airflow 3 — manually triggered junior DAG
        |
        v
AWS Glue / PySpark
        |
        +------------------> Iceberg Bronze
        |                    source-faithful + ingestion metadata
        |
        v
Mandatory Great Expectations checkpoint
        |
        v
Glue/PySpark Silver transformation
        |
        +------------------> Iceberg Silver
        |                    validated trips
        +------------------> Iceberg quarantine
        |
        v
dbt-glue Gold
  - dim_operator
  - dim_zone
  - dim_date
  - fct_trips
  - mart_hourly_zone_demand
  - mart_operator_metrics
Publication manifest
        |
        v
Amazon Athena analytical serving
```

Advanced extensions:

```text
2025 schema evolution
Iceberg file metrics and compaction
snapshot expiration and time travel
larger backfills
```

---

## 4. Technology boundaries

### Required core

- Python and PySpark.
- AWS Glue and Glue Data Catalog.
- S3.
- Apache Iceberg.
- dbt-glue.
- Great Expectations checkpoint between Bronze and Silver.
- Publication manifest for validated Gold tables and counts.
- Amazon Athena read-only analytical serving.

### Required for the full junior project

- Airflow 3 manually triggered DAG.
- Mandatory Great Expectations checkpoint between Bronze and Silver.
- Terraform for S3, Glue, Catalog, and IAM resources used.
- CI for Python, DAG, dbt, Great Expectations, Athena SQL/runner contracts, and Terraform.
- Idempotent four-month processing.

### Advanced extension

- 2025 schema evolution.
- Conditional compaction.
- Snapshot expiration and orphan-file dry run.
- Full-year backfill and deeper recovery testing.

### Do not add

- ML, recommendations, MongoDB, PostgreSQL serving, ClickHouse, ScyllaDB, Redis, Elasticsearch, Trino, Kubernetes, dashboard application, or unrestricted `/query` endpoint.
- Weather, traffic, map polygons, demographic enrichment, or route optimization.

---

## 5. Simple data model

### 5.1 Bronze

#### `bronze_hvfhs_trips`

Preserve source columns plus:

```text
_source_file
_source_year
_source_month
_source_checksum
_ingestion_run_id
_ingested_at
```

Bronze must remain source-faithful. Do not apply business aggregations here.

#### `bronze_taxi_zones`

Taxi Zone lookup plus ingestion metadata.

### 5.2 Silver

#### `silver_trips`

Keep only fields needed by the project:

```text
trip_id
operator_code
request_datetime
pickup_datetime
dropoff_datetime
pickup_zone_id
dropoff_zone_id
trip_miles
trip_time_seconds
passenger_fare
tolls
sales_tax
tips
driver_pay
shared_request_flag
shared_match_flag
source_year
source_month
ingestion_run_id
```

Derive:

```text
trip_duration_minutes
pickup_date
pickup_hour
```

Basic validity rules:

- pickup and drop-off timestamps exist;
- drop-off is not before pickup;
- zone IDs resolve or are quarantined;
- distance, duration, fare, and driver pay are non-negative;
- duplicate `trip_id` is not loaded twice.

Invalid rows go to `quarantine_trips` with a reason code.

### 5.3 Gold

Keep the Gold layer small.

Dimensions:

```text
dim_operator
dim_zone
dim_date
```

Fact:

```text
fct_trips
```

Marts:

```text
mart_hourly_zone_demand
mart_operator_metrics
```

Do not create more marts until these are tested and used by the bounded Athena query pack.

---

## 6. Idempotency and quality

### Source manifest

For every source month record:

```text
source_year
source_month
source_uri
source_checksum
source_size_bytes
status
first_seen_at
processed_at
ingestion_run_id
```

Rules:

- same checksum without `force=true` must not duplicate canonical rows;
- changed checksum requires explicit handling;
- every run has a stable run ID;
- Bronze, Silver, quarantine, and Gold row counts are recorded;
- reconciliation must explain rejected rows.

### Mandatory Great Expectations checkpoint

The checkpoint runs after month-scoped Bronze publication and before Silver.
It is blocking: a failed expectation fails the Airflow task and prevents the
Silver task from starting. It validates source schema, required timestamps,
timestamp ordering, non-negative measures, and resolvable Taxi Zone IDs.

Great Expectations does not silently remove invalid records. Bronze remains
source-faithful; the Silver transformation applies the same rule contract and
writes invalid rows to `quarantine_trips` with deterministic reason codes.
Therefore the checkpoint protects the transformation boundary while quarantine
preserves row-level evidence. The post-Gold quality/reconciliation task is a
separate publication check, not a substitute for this checkpoint.

### Publication manifest

After dbt-glue succeeds, publish a durable manifest containing the source
identity, run ID, Gold table locations, Iceberg snapshot IDs when available,
row counts, and validation status. Athena reads only a manifest marked
validated.

### Athena contract

Athena is the bounded read-only serving layer over Glue-cataloged Gold Iceberg
tables. The minimal scope is exactly:

1. one Gold smoke query;
2. one representative business mart query;
3. one Iceberg history/snapshots metadata query;
4. one parameterized version-travel template.

Terraform creates one workgroup using an existing project-bucket result prefix,
SSE-S3, CloudWatch query metrics, a configurable per-query scan cutoff, and one
least-privilege Gold/Glue-read and result-prefix-write policy. Python provides
one generic Boto3 runner and one minimal Gold smoke verifier. Lake Formation,
customer-managed KMS, a dedicated result bucket, dashboards, alarms, evidence
export, and a generic query-testing framework are explicitly deferred.

---

## 8. Junior-first milestones

## Milestone A — Runnable lakehouse core

Deliver:

1. one-month fixture and source adapter;
2. minimal Terraform or clearly documented manually created dev resources;
3. Glue/PySpark Bronze ingestion;
4. Silver validation and quarantine;
5. Iceberg tables in Glue Catalog;
6. dbt Gold dimensions, fact, and two marts;
7. dbt tests;
8. Athena smoke query, business mart query, history/snapshots query, and time-travel template;
9. README with exact run order.

Acceptance criteria:

- one selected month reaches Bronze, Silver, and Gold;
- invalid fixture rows appear in quarantine with reason codes;
- rerunning the same fixture does not duplicate `fct_trips`;
- Gold counts reconcile with valid Silver rows;
- Athena SQL and Boto3 runner contracts pass credential-independent tests;
- all credential-independent checks pass.

Milestone A is sufficient for the first resume-ready release.

## Milestone B — Complete junior platform

Implement after Milestone A.

Deliver:

1. Airflow 3 manually triggered DAG;
2. four-month sequential backfill;
3. source manifest and run audit;
4. one Great Expectations checkpoint;
5. Terraform for the required AWS resources;
6. CI;
7. retry/clear/rerun experiment;
8. cost and teardown guide.

Acceptance criteria:

- the DAG runs one month from parameter input;
- four months process without duplicate canonical rows;
- a failed task can be cleared and rerun safely;
- Terraform validates and resources are destroyable;
- evidence includes Airflow/Glue logs, Great Expectations results, Iceberg table snapshots, dbt test output, publication manifest, and Athena results.

Milestone B is the junior Definition of Done.

## Milestone C — Advanced immortal extension

Optional until the junior project is complete.

Deliver:

1. ingest one 2025 month and evolve the Iceberg schema;
2. publish exact snapshot IDs;
3. retain exact snapshot IDs and keep a manual Athena version-travel query template;
4. collect file metrics;
5. compact only when thresholds are exceeded;
6. expire snapshots safely;
7. perform orphan-file dry run;
8. optional full-2024 backfill.

Milestone C must never block Milestone A or B.

---

## 9. Implementation phases for Codex

Codex must implement one phase per run.

### Phase 0 — Inventory and freeze baseline

- inspect existing Instacart/ML/MongoDB code;
- record what builds;
- create a migration map;
- do not delete working paths before replacements exist.

### Phase 1 — Dataset adapter, fixtures, and contracts

- add NYC TLC source definitions;
- add 2024 and minimal 2025 fixtures;
- implement source manifest model and tests.

Student learning task: explain source grain and the idempotency key.

### Phase 2 — Bronze and Silver

- implement one-month Bronze ingestion;
- implement Silver validation, deduplication, and quarantine;
- add reconciliation tests.

Student learning task: decide what belongs in Bronze versus Silver.

### Phase 3 — Iceberg and dbt Gold

- create Iceberg tables/catalog configuration;
- add three dimensions, one fact, and two marts;
- add dbt tests.

Student learning task: explain the grain of `fct_trips` and each mart.

### Phase 4 — Athena serving contract and MVP documentation

- define the four bounded Athena SQL artifacts;
- add the generic Boto3 runner and minimal Gold smoke verifier contract;
- remove active ML/MongoDB/query-API documentation;
- produce Milestone A README.

Student learning task: explain why Athena is a read-only serving layer while
Glue Catalog and Iceberg remain canonical.

### Phase 5 — Airflow, quality, four-month rerun

- add Airflow 3 DAG with `year`, `month`, and `force` parameters;
- add the mandatory Great Expectations checkpoint between Bronze and Silver;
- keep post-Gold reconciliation separate from Great Expectations;
- verify retry, clear, and idempotent rerun;
- process four months sequentially.

Student learning task: explain retry versus rerun versus backfill.

### Phase 6 — Terraform, CI, cloud evidence, teardown

- finalize minimal Terraform;
- add CI;
- run bounded cloud demo;
- capture evidence and teardown.

Student learning task: explain which resources are ephemeral and which data is canonical.

### Phase 7 — Optional Iceberg lifecycle extension

- schema evolution;
- snapshot manifest and pinning;
- compaction and retention safety.

Student learning task: explain snapshots, compaction, and why orphan deletion starts as dry run.

---

## 10. Resource-aware deployment

The laptop is a thin client.

### Laptop

- edit code;
- run pure-function and fixture tests;
- run dbt parse/compile when possible;
- run Athena SQL/runner contract tests with mocks;
- validate Terraform;
- do not run full Spark or Airflow locally.

### Remote development

Use Codespaces or another disposable environment for:

- repository-wide tests;
- Airflow DAG import tests;
- dbt validation;
- small Spark fixture tests if needed.

### AWS trial/credits

Use AWS Glue, S3, and Glue Catalog only after code and smoke scripts are ready.

Start with one month, then four months. Do not run a full year until the junior Definition of Done passes.

---

## 11. Required tests

Minimum junior test set:

- source manifest and checksum behavior;
- Bronze ingestion metadata;
- Silver validation and quarantine;
- deterministic `trip_id` and deduplication;
- reconciliation counts;
- dbt uniqueness, not-null, and relationship tests;
- Great Expectations checkpoint contract results;
- Athena fixed-scope query/runner expected results;
- Airflow DAG import/structure test;
- idempotent rerun test;
- Terraform `fmt -check` and `validate`.

One controlled failure or rerun experiment is required per phase after Phase 1.

---

## 12. Junior Definition of Done

The project is complete for the junior stage when:

1. Milestone A and B pass.
2. One month and a four-month sequence have real evidence.
3. The same source can be rerun without duplicate canonical rows.
4. Bronze, Silver, quarantine, Gold, publication manifest, and Athena smoke results reconcile.
5. Airflow can retry or clear a failed task safely.
6. Terraform resources are destroyable.
7. README claims only implemented and verified features.
8. The user can explain ingestion grain, idempotency, Bronze/Silver boundaries, Great Expectations blocking behavior, fact grain, Iceberg snapshots, Airflow reruns, publication manifests, and Athena's role.

2025 schema evolution and Iceberg lifecycle maintenance are advanced extensions, not junior completion requirements.

---

## 13. Target repository structure

```text
AGENTS.md
README.md
pyproject.toml
etl/
  dags/
    nyc_hvfhs_lakehouse_dag.py
  glue_jobs/
    bronze_ingestion.py
    silver_transform.py
  quality/
  dbt_project/
athena/
  query_runner.py
  verify_gold.py
  sql/
    gold_smoke.sql
    mart_hourly_zone_demand.sql
    iceberg_history.sql
    time_travel.sql.tmpl
infra/
  terraform/
scripts/
  fetch_source.py
  prepare_manifest.py
  upload_release_dataset.py
  run_fixture_demo.py
  verify_reconciliation.py
  verify_teardown.py
  teardown_demo.sh
tests/
  fixtures/
  unit/
  contract/
docs/
  PROJECT2_BLUEPRINT_FINAL.md
  DATASET_NOTES.md
  learning/
legacy/
```

Advanced Iceberg maintenance files may be added only in Phase 7. Athena is the
only active analytical serving path; no alternate query-engine directory is
part of the target structure.

---

## 14. Codex rules

- Read `AGENTS.md` and this blueprint before editing.
- Implement exactly one phase.
- Complete Milestone A before adding advanced infrastructure.
- Prefer a small working diff over a broad rewrite.
- Keep the previous runnable path until its replacement passes.
- Never provision AWS resources without explicit approval.
- Never claim idempotency, schema evolution, snapshot pinning, performance, or recovery without evidence.
- End each run with changed files, commands, results, blockers, one student learning task, and three teach-back questions.

---

## 15. Deployment-preservation contract

The following responsibilities remain mandatory even when their cloud execution
is not verified:

- **Durable source manifests:** persist one immutable row per source month with
  URI, checksum, size, status, stable run ID, timestamps, and Bronze/Silver/
  quarantine/Gold counts.
- **Retry-safe writes:** a task retry or deterministic monthly rerun may not
  duplicate canonical Bronze, Silver, quarantine, Gold, or publication rows.
  A changed checksum is blocked until an explicit replacement workflow exists.
- **Namespace/catalog wiring:** one documented Glue Data Catalog name and
  database/namespace mapping must be consumed consistently by Glue, Iceberg,
  dbt-glue, the publication manifest, and Athena.
- **Instance-profile authentication:** the temporary Airflow/query runner uses
  an EC2 instance profile and the AWS SDK default credential chain. No static
  keys are stored in Terraform, user data, `.env.cloud.example`, or DAG code.
- **Deployment packaging:** every imported Glue Python module, dbt project,
  Athena SQL artifact, and Airflow DAG dependency is included in the reviewed
  deployment bundle or fetched from the pinned repository revision.
- **E2E scripts:** smoke and release profiles must upload only manifest-pinned
  official sources, run one month before the four-month release, and reconcile
  source, Bronze, Silver, quarantine, Gold, manifest, and Athena outputs.
- **Teardown verification:** teardown must verify the temporary runner, Glue
  jobs, IAM role/profile, workgroup, and non-canonical temporary prefixes are
  absent. Canonical data is never recursively deleted by default.

These are code and documentation obligations, not claims that AWS has run.

## 16. Release gates

### CODEBASE-READY

This gate is satisfied only when the repository can be reviewed from a fresh
checkout and:

1. the active architecture and all active docs use the path in Section 3;
2. Great Expectations is an explicit blocking Bronze-to-Silver checkpoint;
3. source manifests, retry-safe write contracts, namespace wiring, instance-
   profile authentication, deployment packaging, E2E scripts, and teardown
   verification are present;
4. the minimal Athena Terraform, SQL, Boto3 runner, and smoke verifier scope
   is implemented and tested without credentials;
5. CI, Python/unit/fixture tests, dbt parse, Airflow topology checks, SQL
   checks, and Terraform validation pass;
6. no secret, state, saved plan, production source file, or obsolete serving
   path is active or tracked;
7. documentation says AWS execution is **NOT VERIFIED**.

### DEPLOYMENT-VERIFIED

This gate is separate and remains **NOT VERIFIED** until a bounded approved AWS
run retains evidence for:

1. Terraform plan and apply outputs for the temporary stack;
2. instance-profile authentication from the Airflow/query runner;
3. official source upload and checksum/size manifest;
4. Bronze ingestion, Great Expectations pass/block behavior, Silver quarantine,
   dbt-glue Gold publication, and manifest status;
5. Athena Gold smoke/business/history queries, query IDs, scanned bytes, and
   result locations;
6. controlled retry, clear, deterministic rerun, and four-month sequence;
7. reconciliation of source, accepted, rejected, Gold, manifest, and Athena
   results;
8. teardown output proving temporary infrastructure is gone.

A passing Terraform plan, a running service, or a successful isolated query is
not deployment verification.

## 17. Migration and change log

This documentation migration changes the active architecture only; it does not
delete or move implementation artifacts.

| Former active item | Athena-era replacement | Disposition |
| --- | --- | --- |
| DuckDB consumer and five fixed queries | Athena Gold smoke, business mart, history/snapshots, and version-travel template | Reclassify as historical/conflicting; remove only in a separately approved cleanup after Athena tests pass. |
| Phase 4 DuckDB consumer phase | Athena serving-contract phase | Blueprint terminology migrated; Phase 4 report remains historical. |
| DuckDB smoke tests and Gold fixture | Mocked Boto3 runner and minimal Gold smoke-verifier tests | Replacement required; no code change in this documentation pass. |
| Post-Gold equivalent quality checkpoint | Mandatory Great Expectations checkpoint before Silver plus a separately named post-Gold publication reconciliation | Preserve useful assertions, but do not call them the Great Expectations checkpoint. |
| “latest validated snapshot” consumer wording | Durable publication manifest consumed by Athena | Manifest must identify table locations, status, counts, and snapshot IDs when available. |
| Local DuckDB cache and Iceberg scan path | Athena workgroup with existing-bucket results prefix and SSE-S3 | No dedicated results bucket, KMS key management, or Lake Formation in this scope. |
| DuckDB role in learning tasks and Definition of Done | Athena bounded-serving role and Glue Catalog/Iceberg canonical role | Active teach-back language migrated; historical reports are not rewritten. |

Deferred by explicit decision: Lake Formation, customer-managed KMS, dedicated
Athena results bucket, dashboards, alarms, evidence exporter, generic query
testing framework, large query libraries, automated time-travel verification,
and all real AWS execution evidence.
