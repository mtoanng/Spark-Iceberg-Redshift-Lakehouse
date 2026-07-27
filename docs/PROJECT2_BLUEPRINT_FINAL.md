# Project 2 final blueprint

## Objective

Build a replayable, snapshot-governed monthly NYC TLC HVFHV lakehouse. The
first controlled AWS release proves one immutable month, correct canonical
rows, safe task retry/rerun, durable publication evidence, and one bounded
Athena query.

```text
official monthly HVFHV Parquet + Taxi Zone CSV
-> S3 landing
-> Airflow 3
-> Glue 4.0/PySpark Bronze Iceberg
-> structural Great Expectations gate
-> Glue 4.0/PySpark Silver Iceberg + quarantine
-> Cosmos `DbtTaskGroup` -> dbt-glue Gold Iceberg
-> reconciliation
-> snapshot-aware publication JSON
-> Athena bounded read-only serving
```

Glue Data Catalog is canonical metadata; Iceberg on S3 is canonical data.

## Source and identity

One logical month is immutable and identified by source URI, SHA-256, byte
size, year, and month. A changed object for an existing month is blocked,
including with `force=true`. The same immutable source has one stable run ID.

Identity is versioned:

- `row_id`: SHA-256 of policy version plus every ordered canonical source field
  declared for the source year. Nulls, timestamps, integers, and decimals have
  explicit canonical representations. It excludes ingestion and derived fields.
- `business_trip_key`: smaller probable-trip key for investigation. A fare,
  tip, or flag correction retains the business key but changes `row_id`.
- `identity_policy_version`: stored in Bronze, Silver, quarantine, fact, run
  manifest, and publication artifact.

The only policy implementation is `etl/contracts/nyc_hvfhs_identity.py`.
Python and Spark must pass the same pinned golden vectors. The 2025 policy adds
`cbd_congestion_fee`; 2024 does not invent a value.

## Layer contracts

Landing verifies S3 metadata checksum and byte size before Spark reads.

Bronze preserves source fields and adds source URI/file, year/month, checksum,
run ID, ingestion timestamp, and the three identity fields. It replaces only
the requested month partition and records count plus snapshot ID.

Great Expectations remains a separate blocking Glue task. It checks only
year-specific required columns, non-empty requested month, and identity inputs.
It persists a concise summary and blocks Silver on failure.

Silver owns timestamp ordering, zone resolution, numeric presence/non-negative
rules, exact `row_id` deduplication, deterministic reason priority, typed
canonical fields, and derived date/hour/duration. It classifies every Bronze
row once and replaces only requested month partitions.

Gold remains exactly:

```text
dim_date
dim_operator
dim_zone
fct_trips
mart_hourly_zone_demand
mart_operator_metrics
```

`fct_trips` is one row per canonical Silver `row_id`; dbt incremental Iceberg
merge uses `row_id`.

## Orchestration and retry

Airflow 3 keeps this topology:

```text
prepare_month
-> bronze_ingestion
-> great_expectations_checkpoint
-> silver_transform
-> Cosmos `dbt_build` task group
-> dbt_result_artifact
-> reconciliation
-> publication_manifest
-> athena_smoke
```

The DAG is manual with `year`, `month`, and `force`. Cosmos Watcher mode runs
one full `dbt build` producer and renders model-level watchers. Its producer
callback uploads the complete `run_results.json` to the deterministic
publication prefix; `dbt_result_artifact` verifies that object before
reconciliation. Airflow retry, manual task
clear, and deterministic monthly rerun reuse immutable source identity and
replace month-scoped outputs. `force` never accepts changed content. Four
consecutive months remain a fixed companion DAG, not a generic framework.

## Manifest and publication

The operational Iceberg manifest stores source identity, stable run ID,
identity policy, status, validation, Bronze/Silver/quarantine/Gold counts,
layer snapshot IDs, failure stage/message, timestamps, publication status, and
artifact URI.

After dbt and reconciliation, publication resolves all six Gold table
locations, row counts, and Iceberg snapshots. It writes deterministic JSON to
the existing project bucket before changing the operational row to
`published`. Missing required snapshot metadata blocks publication.

## Athena

Exactly four artifacts remain: Gold smoke, business mart, Iceberg
history/snapshots, and parameterized version travel. The runner records query
ID, database, workgroup, state, result location, scanned bytes, and engine
time. Queries are read-only, bounded, partition-filtered where applicable, and
subject to the workgroup scan cutoff.

## Single advanced semantic

After the 2024 baseline, run the initializer with the explicit 2025 evolution
flag to add nullable `cbd_congestion_fee` to Bronze, Silver, and quarantine.
Ingest one 2025 month, retain the new snapshots, verify the current query reads
both years, and run Athena version travel against the retained 2024 snapshot.
No compaction, expiration, orphan deletion, partition evolution, or lifecycle
automation is in scope.

## Deployment boundary

Keep Glue 4.0, Terraform S3/Glue/Catalog/IAM/Athena resources, and the optional
temporary EC2 Airflow runner with instance profile. Cosmos is limited to the
in-process dbt-glue `DbtTaskGroup`; do not add MWAA, EKS, Lake Formation, KMS
management, dashboards, alarms, or extra buckets.

`CODEBASE-READY` requires all credential-independent contracts to pass.
`DEPLOYMENT-VERIFIED` remains `NOT VERIFIED` until a real bounded AWS run
retains source, task, count, snapshot, dbt, Athena, retry/rerun, and teardown
evidence.
