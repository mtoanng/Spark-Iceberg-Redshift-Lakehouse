# Phase 1 Report — Dataset Adapter, Fixtures, and Contracts

Date: 2026-07-19

## Phase implemented

Phase 1 only. The prior Phase 0 gate is explicitly overridden by the request
to implement this phase. This phase adds the NYC TLC HVFHV source contract,
deterministic fixtures, and an in-memory source-manifest model. It does not
read S3, start Spark or Airflow, write Bronze/Silver/Iceberg, run dbt, or
provision cloud resources.

## Files changed

- `.gitignore` — permits only deterministic fixture JSON, CSV, and Parquet
  files under `tests/fixtures/`.
- `etl/sources/__init__.py`
- `etl/sources/nyc_hvfhs.py` — official monthly filename/URI contract,
  2024/2025 schema requirements, local SHA-256 source inspection,
  deterministic source-trip fingerprint, and manifest decisions.
- `tests/fixtures/nyc_hvfhs/fhvhv_tripdata_2024-01.fixture.json`
- `tests/fixtures/nyc_hvfhs/fhvhv_tripdata_2025-01.fixture.json`
- `tests/fixtures/nyc_hvfhs/taxi_zone_lookup.fixture.csv`
- `tests/unit/test_nyc_hvfhs_source.py`
- `docs/DATASET_NOTES.md`
- `docs/PHASE_1_REPORT.md`

## Contract decisions

- The only production trip source is the official monthly
  `fhvhv_tripdata_YYYY-MM.parquet` object; default month is `2024-01`.
- The source grain is one raw HVFHV trip record. Fixtures are deliberately
  source-shaped, small JSON/CSV test inputs and are not NYC TLC data files.
- The candidate `trip_id` is a SHA-256 fingerprint over stable raw trip fields:
  license, dispatching base, request/pickup/drop-off times, pickup/drop-off
  zone IDs, miles, trip time, base fare, and driver pay. It includes no
  derived values or ingestion metadata.
- Manifest identity records year, month, URI, SHA-256, size, status,
  timestamps, and run ID. An already processed identical source is skipped;
  `force=True` permits an intentional retry of that exact object; a changed
  checksum is blocked pending an explicit later workflow.
- `cbd_congestion_fee` is required by the adapter for 2025+ fixture schema
  detection only. No schema evolution was implemented.

## Commands and results

Expected behavior before running checks: the pure-Python source contract and
fixtures should compile and all Phase 1 tests should pass; rerunning the
manifest tests should produce the same decisions without credentials or cloud
access.

| Command | Result |
| --- | --- |
| `curl.exe --head --location --silent --show-error <official-HVFHV-2024-01-URI>` | HTTP 403; production monthly size/checksum remain NOT VERIFIED. No source data was downloaded. |
| `curl.exe --head --location --silent --show-error <taxi-zone-lookup-URI>` | HTTP 200; `Content-Length: 12331`, ETag `c6064b7c144c716450641f769659d178`. The ETag is not accepted as the required SHA-256. |
| `.\.venv\Scripts\python.exe -m compileall -q etl\sources tests\unit\test_nyc_hvfhs_source.py` | PASS (exit 0). |
| `.\.venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\unit\test_nyc_hvfhs_source.py -v` | PASS (exit 0): 10 passed. |
| `.\.venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\unit\test_nyc_hvfhs_source.py -k "processed_identical_source_is_skipped_without_force or changed_checksum_is_blocked_even_when_forced" -v` | PASS (exit 0): 2 passed, 8 deselected. This is the controlled rerun experiment. |
| `Get-FileHash tests\fixtures\nyc_hvfhs\* -Algorithm SHA256` | PASS: fixture sizes and SHA-256 values match the pinned values in `docs/DATASET_NOTES.md` and the unit test. |
| `git check-ignore -q tests/fixtures/nyc_hvfhs/fhvhv_tripdata_2024-01.fixture.json` | PASS: exit 1 means the fixture is intentionally **not** ignored and can be tracked. |

No full Spark, Airflow Compose, Docker Compose, AWS stack, or Terraform apply
was run, in accordance with the laptop resource constraint.

## Verified acceptance criteria

- Official NYC TLC monthly HVFHV and Taxi Zone source definitions are present.
- The default source month is 2024-01 and monthly URI construction is tested.
- Deterministic 2024 fixture includes a valid trip, duplicate, invalid time
  order, invalid zone, and negative metric.
- Minimal 2025 fixture includes `cbd_congestion_fee` and passes its declared
  source schema contract.
- Fixture byte sizes and SHA-256 checksums are pinned and tested.
- Source manifest rules for same-checksum skip, forced retry, and checksum
  change blocking are tested.
- The `trip_id` fingerprint is stable for identical source records.

## NOT VERIFIED

- Official 2024-01 HVFHV object size and SHA-256 checksum; the source endpoint
  rejected the metadata request and the file was not downloaded.
- S3 landing, AWS Glue, Glue Catalog, Iceberg, Airflow deployment, and cloud
  execution.
- Actual Parquet read compatibility and production-source schema validation in
  Spark/Glue.
- Bronze metadata writes, Silver validation/quarantine, reconciliation, and
  end-to-end idempotent writes.
- dbt, DuckDB, Terraform deployment, schema evolution, snapshot pinning,
  performance, recovery, costs, and teardown.

## Blockers and risks

- A changed source checksum currently blocks processing by design; the reviewed
  replacement/audit workflow belongs to a later phase.
- Production-source metadata is incomplete until a controlled source download
  records SHA-256 and size.
- The inherited legacy warehouse suite still fails at collection because it
  imports `warehouse.sql_validator`, while the implementation is under
  `warehouse/parser/sql_validator.py`.
- The inherited legacy pipeline check, Terraform formatting check, and dbt
  parse issue from Phase 0 remain outside this phase's diff.
- `NOT VERIFIED` cloud and later-phase items mean the next phase requires an
  explicit user override under the Phase Gate Rule.

## Cleanup Report

Removed:

- None.

Archived:

- None.

Kept:

- Legacy Instacart, ML, MongoDB, API, dbt, Glue, Terraform, and documentation
  paths.

Reason:

The current blueprint requires the previous runnable path to remain until its
replacement passes. Phase 1 supplies only a source contract and cannot safely
replace those pipelines. They are kept as temporary migration dependencies, not
for historical preservation; their replacement/cleanup is scheduled by the
later blueprint phases.

## Student learning task

Core decision: **source grain and idempotency key**. Explain why one raw HVFHV
trip record is the Bronze source grain, then explain why the fingerprint does
not use `source_file`, ingestion timestamp, or a derived duration. Manually
verify this by reading `TRIP_IDENTITY_COLUMNS` in `etl/sources/nyc_hvfhs.py`
and identifying what would happen to the identifier on a rerun of the same
file.

## Teach-back questions

1. Why is an HVFHV monthly file a collection of trip records rather than one
   source record for the month?
2. What is the difference between an identical checksum with `force=True` and
   a changed checksum with `force=True` in this manifest contract?
3. Why must ingestion metadata be excluded from the source-trip fingerprint?

## Failure/rerun experiment

The targeted manifest rerun test was executed after the full suite. It verified
two controlled outcomes: a processed identical fixture is skipped unless
forced, and a modified fixture remains blocked even when forced. Both runs
passed without cloud access or a stateful write.

## Later-phase confirmation

No Phase 2 or later work was started.
