# Phase 2 Report — Bronze and Silver

Date: 2026-07-19

## Phase implemented

Phase 2 only. The user's request explicitly overrides the Phase 1 gate. This
phase implements source-faithful Bronze metadata, Silver validation,
deduplication, quarantine reason codes, and count reconciliation. It does not
create an Iceberg catalog/table, dbt Gold model, DuckDB consumer, Airflow DAG,
or cloud resource.

## Important contract correction

The correct official file prefix is **`fhvhv_tripdata_YYYY-MM.parquet`**, as
specified by the current `AGENTS.md` and confirmed by the locally staged file
names. The earlier `hvfvh` spelling in Phase 1 fixture filenames was corrected
to `fhvhv`; no production data file was renamed or changed.

## Files changed

- `etl/sources/nyc_hvfhs.py` — stable run ID now uses the correct `fhvhv`
  prefix.
- `etl/transforms/__init__.py`
- `etl/transforms/nyc_hvfhs.py` — pure, fixture-testable Bronze metadata,
  Silver validation, quarantine, deduplication, and reconciliation contract.
- `etl/glue_jobs/nyc_bronze_ingestion.py` — remote-only Glue Bronze entry
  point for trips and Taxi Zone lookup metadata.
- `etl/glue_jobs/nyc_silver_transform.py` — remote-only Glue Silver/
  quarantine entry point using matching reason-code logic.
- `tests/unit/test_nyc_hvfhs_transform.py`
- `tests/fixtures/nyc_hvfhs/fhvhv_tripdata_2024-01.fixture.json` — renamed
  from the incorrect `hvfvh` spelling.
- `tests/fixtures/nyc_hvfhs/fhvhv_tripdata_2025-01.fixture.json` — renamed
  from the incorrect `hvfvh` spelling.
- `tests/unit/test_nyc_hvfhs_source.py` and `docs/DATASET_NOTES.md` — aligned
  to the corrected source filename and local staging observation.
- `docs/PHASE_2_REPORT.md`

## Implemented behavior

- Bronze copies every source field unchanged and appends only `_source_file`,
  `_source_year`, `_source_month`, `_source_checksum`, `_ingestion_run_id`,
  and `_ingested_at`.
- Silver maps source fields to the locked trip contract, derives duration,
  pickup date, and pickup hour, and uses the Phase 1 source fingerprint as
  `trip_id`.
- Invalid rows are retained in quarantine with one deterministic reason code:
  missing/invalid timestamp, invalid time order, invalid/unknown zone,
  invalid numeric metric, negative metric, missing identity field, or duplicate
  trip ID.
- Every Bronze row must reconcile to either one Silver row or one quarantine
  row.
- The local rerun contract supplies existing canonical trip IDs; an already
  accepted trip cannot be added again.

## Commands and results

Expected behavior: pure-Python code and fixture tests should pass; Parquet
footer inspection should validate schema without reading all rows; no laptop
Spark or cloud process should start.

| Command | Result |
| --- | --- |
| `.\.venv\Scripts\python.exe -m compileall -q etl\sources etl\transforms etl\glue_jobs\nyc_bronze_ingestion.py etl\glue_jobs\nyc_silver_transform.py tests\unit\test_nyc_hvfhs_source.py tests\unit\test_nyc_hvfhs_transform.py` | PASS (exit 0). |
| `.\.venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\unit\test_nyc_hvfhs_source.py tests\unit\test_nyc_hvfhs_transform.py -q` | PASS (exit 0): 14 passed. |
| `pyarrow.parquet.ParquetFile(...).schema.names` plus `validate_trip_schema(...)` for local `data/fhvhv_tripdata_2024-*.parquet` files | PASS (exit 0): 3 files found; Jan 19,663,930 rows, Feb 19,359,148 rows, Mar 21,280,788 rows; all required 2024 columns present. Only Parquet footers were read. |
| `.\.venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\unit\test_nyc_hvfhs_transform.py -k rerun -q` | PASS (exit 0): 1 passed, 3 deselected. Controlled rerun experiment verified no already accepted trip is added. |

## Verified acceptance criteria

- One-month Bronze source metadata behavior is tested with deterministic
  fixtures.
- Bronze preserves raw source fields and adds exactly the required metadata.
- Fixture rows cover valid, duplicate, invalid time ordering, invalid zone, and
  negative metric outcomes.
- Silver validation, deduplication, quarantine reason codes, derived fields,
  and count reconciliation are tested.
- Local Parquet footer/schema inspection confirms the three currently staged
  2024 files are the expected source shape.
- A local rerun cannot add an already accepted canonical trip.

## NOT VERIFIED

- SHA-256 checksums and official provenance of locally staged monthly files.
- A fourth staged 2024 month; only January through March were present at
  inspection time.
- S3 landing, AWS Glue execution, Glue Catalog, Iceberg table creation,
  remote PySpark behavior, and physical writes to Bronze/Silver/quarantine.
- End-to-end idempotency against Iceberg, including concurrent/recovered runs.
- Airflow deployment, dbt Gold, DuckDB consumption, Terraform deployment,
  schema evolution, snapshot pinning, performance, recovery, costs, and
  teardown.

## Blockers and risks

- The Glue jobs reference Iceberg tables that are intentionally not provisioned
  until Phase 3; remote execution must not be attempted yet.
- The local test proves row-contract behavior, not Spark/Glue semantics.
- The source manifest is still an in-memory Phase 1 contract; durable audit
  persistence is scheduled later.
- The inherited legacy Instacart suite and tooling failures remain outside this
  phase's diff.
- This report's `NOT VERIFIED` items require an explicit override before
  starting Phase 3.

## Cleanup Report

Removed:

- None.

Archived:

- None.

Kept:

- Legacy Instacart Glue jobs, dbt project, ML/MongoDB/API code, Terraform, and
  documentation.

Reason:

The new NYC Glue entry points have local contract evidence but no remote
Iceberg execution evidence. The blueprint requires the previous runnable path
to remain until its replacement passes. These legacy paths are retained only as
temporary migration dependencies and remain scheduled for replacement/cleanup
in their corresponding later phases.

## Student learning task

Core decision: **Bronze versus Silver boundary**. Explain why a negative
`driver_pay` row must be preserved in Bronze with source metadata but sent to
Silver quarantine rather than silently dropped or corrected in Bronze. Manually
verify this by comparing `bronze_records` and `_reason_code` in
`etl/transforms/nyc_hvfhs.py`.

## Teach-back questions

1. Which columns may Bronze add to a raw HVFHV record, and why are business
   rules excluded from that layer?
2. Why is a duplicate trip quarantined instead of simply discarded?
3. What does a reconciliation of 5 Bronze rows = 1 Silver row + 4 quarantine
   rows prove, and what does it not prove?

## Failure/rerun experiment

The controlled rerun supplied the accepted `trip_id` set from the first run to
the same fixture input. The second run produced zero new Silver rows and
quarantined the repeated valid trip as `DUPLICATE_TRIP_ID`. This verifies the
pure local contract only; Iceberg idempotency remains NOT VERIFIED.

## Later-phase confirmation

No Phase 3 or later work was started.
