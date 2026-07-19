# PROJECT 2 — JUNIOR-FIRST IMPLEMENTATION BLUEPRINT

Repository: `mtoanng/Spark-Iceberg-DuckDB-Lakehouse`

Target name: **NYC High-Volume Ride-Hailing Lakehouse**

Status: **Final architecture, junior-first delivery plan**

This file is the source of truth for Project 2. The final platform represents durable batch and lakehouse architecture, but the first release must remain small enough to complete, run, and explain as a junior Data Engineer.

---

## 1. Goal

Build a monthly batch lakehouse from official NYC TLC High Volume For-Hire Vehicle trip records.

The first mandatory path is:

```text
one monthly Parquet file
-> S3 landing
-> AWS Glue/PySpark
-> Iceberg Bronze and Silver
-> dbt Gold
-> DuckDB read-only analytics
```

Then add:

```text
Airflow 3 orchestration
basic data quality
idempotent reruns
three-month backfill
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
        +------------------> Iceberg Silver
                             validated trips + quarantine
        |
        v
dbt-glue Gold
  - dim_operator
  - dim_zone
  - dim_date
  - fct_trips
  - mart_hourly_zone_demand
  - mart_operator_metrics
        |
        v
Basic quality gates
        |
        v
Publication manifest
        |
        v
DuckDB read-only analytical consumer
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
- DuckDB read-only consumer.

### Required for the full junior project

- Airflow 3 manually triggered DAG.
- Basic Great Expectations checkpoint or equivalent explicit quality gate.
- Terraform for S3, Glue, Catalog, and IAM resources used.
- CI for Python, DAG, dbt, DuckDB queries, and Terraform.
- Idempotent three-month processing.

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

Do not create more marts until these are tested and used by DuckDB queries.

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

### Quality gates

Junior quality gates are intentionally limited:

- source file exists and schema contains required columns;
- primary timestamps are non-null;
- valid trip time ordering;
- non-negative numeric metrics;
- zone relationships resolve;
- `fct_trips.trip_id` is unique and not null;
- Gold row count reconciles with valid Silver rows.

One Great Expectations checkpoint is enough for the junior version. dbt tests cover Gold models.

---

## 7. DuckDB contract

DuckDB is not the source of truth.

It must:

- read the published Iceberg Gold tables;
- execute a fixed query pack;
- never mutate canonical Iceberg data;
- optionally use a local cache only for offline demonstration.

Required queries:

1. hourly pickups by zone;
2. operator trip count and average fare;
3. top pickup zones for the selected month;
4. basic fare and driver-pay reconciliation;
5. `EXPLAIN ANALYZE` for one filtered query.

The junior version may read the latest validated snapshot. Exact snapshot pinning is added in the advanced extension.

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
8. DuckDB fixed query pack;
9. README with exact run order.

Acceptance criteria:

- one selected month reaches Bronze, Silver, and Gold;
- invalid fixture rows appear in quarantine with reason codes;
- rerunning the same fixture does not duplicate `fct_trips`;
- Gold counts reconcile with valid Silver rows;
- all DuckDB queries return expected fixture results;
- all credential-independent checks pass.

Milestone A is sufficient for the first resume-ready release.

## Milestone B — Complete junior platform

Implement after Milestone A.

Deliver:

1. Airflow 3 manually triggered DAG;
2. three-month sequential backfill;
3. source manifest and run audit;
4. one Great Expectations checkpoint;
5. Terraform for the required AWS resources;
6. CI;
7. retry/clear/rerun experiment;
8. cost and teardown guide.

Acceptance criteria:

- the DAG runs one month from parameter input;
- three months process without duplicate canonical rows;
- a failed task can be cleared and rerun safely;
- Terraform validates and resources are destroyable;
- evidence includes Airflow/Glue logs, Iceberg table snapshots, dbt test output, and DuckDB results.

Milestone B is the junior Definition of Done.

## Milestone C — Advanced immortal extension

Optional until the junior project is complete.

Deliver:

1. ingest one 2025 month and evolve the Iceberg schema;
2. publish exact snapshot IDs;
3. query a pinned snapshot with DuckDB when supported by the chosen catalog path;
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

### Phase 4 — DuckDB consumer and MVP documentation

- add fixed queries and `EXPLAIN ANALYZE`;
- add smoke test;
- remove active ML/MongoDB/query-API documentation;
- produce Milestone A README.

Student learning task: explain why DuckDB is a consumer, not the warehouse.

### Phase 5 — Airflow, quality, three-month rerun

- add Airflow 3 DAG with `year`, `month`, and `force` parameters;
- add one quality checkpoint;
- verify retry, clear, and idempotent rerun;
- process three months sequentially.

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
- run DuckDB query tests;
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

Start with one month, then three months. Do not run a full year until the junior Definition of Done passes.

---

## 11. Required tests

Minimum junior test set:

- source manifest and checksum behavior;
- Bronze ingestion metadata;
- Silver validation and quarantine;
- deterministic `trip_id` and deduplication;
- reconciliation counts;
- dbt uniqueness, not-null, and relationship tests;
- DuckDB fixed-query expected results;
- Airflow DAG import/structure test;
- idempotent rerun test;
- Terraform `fmt -check` and `validate`.

One controlled failure or rerun experiment is required per phase after Phase 1.

---

## 12. Junior Definition of Done

The project is complete for the junior stage when:

1. Milestone A and B pass.
2. One month and a three-month sequence have real evidence.
3. The same source can be rerun without duplicate canonical rows.
4. Bronze, Silver, quarantine, Gold, and DuckDB results reconcile.
5. Airflow can retry or clear a failed task safely.
6. Terraform resources are destroyable.
7. README claims only implemented and verified features.
8. The user can explain ingestion grain, idempotency, Bronze/Silver boundaries, fact grain, Iceberg snapshots, Airflow reruns, and DuckDB's role.

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
consumer/
  duckdb_consumer.py
  queries/
infra/
  terraform/
scripts/
  fetch_source.py
  run_fixture_demo.py
  verify_reconciliation.py
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

Advanced Iceberg maintenance files may be added only in Phase 7.

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
