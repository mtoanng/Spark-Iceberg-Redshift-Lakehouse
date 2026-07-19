# Phase 3 Report — Iceberg and dbt Gold

Date: 2026-07-19

## Phase implemented

Phase 3 only. The user's request explicitly overrides the Phase 2 gate. This
phase defines the Bronze/Silver/quarantine Iceberg catalog contract, adds a
remote-only Glue initializer, and replaces the active Instacart dbt graph with
the locked NYC Gold graph: three dimensions, one fact, and two marts.

No DuckDB consumer, fixed query pack, Airflow DAG, quality checkpoint,
publication manifest, CI, cloud deployment, or Phase 4 documentation was
implemented.

## Files changed

- `etl/iceberg/__init__.py`
- `etl/iceberg/catalog.py`
- `etl/glue_jobs/initialize_nyc_iceberg_tables.py`
- `etl/dbt_project/dbt_project.yml`
- `etl/dbt_project/profiles.yml`
- `etl/dbt_project/packages.yml`
- `etl/dbt_project/README.md`
- `etl/dbt_project/models_nyc/sources.yml`
- `etl/dbt_project/models_nyc/schema.yml`
- `etl/dbt_project/models_nyc/dimensions/dim_operator.sql`
- `etl/dbt_project/models_nyc/dimensions/dim_zone.sql`
- `etl/dbt_project/models_nyc/dimensions/dim_date.sql`
- `etl/dbt_project/models_nyc/facts/fct_trips.sql`
- `etl/dbt_project/models_nyc/marts/mart_hourly_zone_demand.sql`
- `etl/dbt_project/models_nyc/marts/mart_operator_metrics.sql`
- `etl/dbt_project/tests/fct_trips_reconciles_to_silver.sql`
- `tests/unit/test_iceberg_catalog.py`
- `tests/unit/test_dbt_gold_contract.py`
- `legacy/dbt_instacart_models/` — archived former active model tree.
- `etl/dbt_project/package-lock.yml` — removed with the obsolete dbt-utils
  dependency.
- `docs/PHASE_3_REPORT.md`

## Iceberg catalog contract

The initializer declares exactly four upstream Iceberg v2 tables:

- `glue_catalog.bronze.bronze_hvfhs_trips`;
- `glue_catalog.bronze.bronze_taxi_zones`;
- `glue_catalog.silver.silver_trips`;
- `glue_catalog.silver.quarantine_trips`.

Bronze trips include the complete observed 2024 source schema plus the six
required ingestion metadata fields. Bronze trips, Silver trips, and quarantine
are partitioned by source year and month. Gold tables are not declared by the
initializer; dbt owns their creation as Iceberg tables.

## Gold grains

| Model | Grain |
| --- | --- |
| `dim_operator` | One row per operator code observed in valid Silver trips. |
| `dim_zone` | One row per TLC Taxi Zone ID. |
| `dim_date` | One row per pickup calendar date. |
| `fct_trips` | One row per validated, deduplicated Silver `trip_id`. |
| `mart_hourly_zone_demand` | One row per pickup date, pickup hour, and pickup zone. |
| `mart_operator_metrics` | One row per source year, source month, and operator. |

The fact retains natural foreign keys to the three dimensions and carries trip,
fare, tax, tip, and driver-pay measures. Both marts aggregate only from
`fct_trips`.

## Commands and results

Expected behavior: Iceberg and dbt static contracts should pass pure local
tests; `dbt parse` should discover exactly six models without credentials or a
Spark session; cloud build/test must remain unverified.

| Command | Result |
| --- | --- |
| `.\.venv\Scripts\python.exe -m compileall -q etl\iceberg etl\glue_jobs\initialize_nyc_iceberg_tables.py tests\unit` | PASS (exit 0). |
| `.\.venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\unit -q` | PASS (exit 0): 23 passed. This includes Phase 1–3 source, Bronze/Silver, reconciliation, Iceberg DDL, and dbt graph contract checks. |
| First `dbt parse --profiles-dir . --target local_parse --no-partial-parse` | FAILED (exit 2): the pre-existing inaccessible `dbt_packages/dbt_utils` cache was still auto-discovered. The project package path was redirected to `target/dbt_packages_local`; the cache itself was not modified. |
| Final `dbt parse --profiles-dir . --target local_parse --no-partial-parse` | PASS (exit 0), including a second run after legacy-model archival. |
| `dbt ls --profiles-dir . --target local_parse --resource-type model --output name` | PASS (exit 0): exactly `dim_date`, `dim_operator`, `dim_zone`, `fct_trips`, `mart_hourly_zone_demand`, and `mart_operator_metrics`. |
| Parsed `target/manifest.json` inspection | PASS: 6 project models, 32 data tests, and 2 sources. |
| `dbt compile --profiles-dir . --target local_parse` | STOPPED / TIMED OUT (exit 124): dbt-spark's session adapter unexpectedly started local Spark. The command host terminated it; a follow-up process check found no remaining Spark/Java process. Compile is not a safe laptop check for this profile. |
| `.\.venv\Scripts\dbt.exe --version` | PASS (exit 0): dbt-core 1.11.12 and dbt-spark 1.10.3 are installed; dbt-glue is not installed in this environment. |

Placeholder values used to render the unused Glue output during local parsing
were `arn:aws:iam::000000000000:role/local-parse-only` and
`s3://local-parse-only/gold`. They are not credentials, account evidence, or
deployment targets.

## Verified acceptance criteria

- The four required Bronze/Silver/quarantine Iceberg table schemas, bounded S3
  locations, partitions, format v2, and compression properties are defined and
  unit-tested.
- The active dbt project parses and contains exactly three dimensions, one
  fact, and two marts; no active ML or Instacart model remains.
- Every Gold model is configured as an Iceberg table.
- `fct_trips.trip_id`, dimension keys, mart grain keys, and required fields have
  declared uniqueness/not-null/relationship tests as appropriate.
- A singular dbt test declares Silver-to-fact row-count reconciliation.
- Both marts depend on `fct_trips`, and their grains are explicit and
  statically tested.
- The legacy Instacart model graph is no longer in the active dbt model path.

## NOT VERIFIED

- AWS Glue initializer execution, Glue Data Catalog namespaces/table entries,
  S3 Iceberg metadata, and physical Bronze/Silver/quarantine tables.
- dbt-glue parse, compile, run, build, and test; the adapter and credentials are
  unavailable locally.
- Gold row counts, uniqueness, not-null, and relationship test outcomes against
  real Silver data.
- Silver-to-`fct_trips` runtime reconciliation and end-to-end idempotency.
- Iceberg snapshots, schema evolution, snapshot pinning, performance,
  compaction, recovery, costs, and teardown.
- DuckDB, Airflow, CI, and any cloud deployment behavior.

## Blockers and risks

- The remote initializer and Phase 2 writers must agree on exact catalog names,
  IAM permissions, and S3 warehouse URI during a bounded cloud run.
- dbt-glue is listed in project requirements but is not installed in the active
  `.venv`; production dbt validation requires a disposable remote environment.
- `dbt compile` is not resource-safe with the local dbt-spark session target.
  Only `dbt parse` is approved on this laptop.
- Runtime dbt tests require provisioned Iceberg sources and therefore remain
  cloud-dependent.
- This report's failed/NOT VERIFIED items require an explicit user override
  before Phase 4 under the Phase Gate Rule.

## Cleanup Report

Removed:

- `etl/dbt_project/package-lock.yml` and the active dbt-utils dependency; the
  NYC models use no external macros.

Archived:

- `etl/dbt_project/models/` to `legacy/dbt_instacart_models/`, including the
  Instacart staging, dimensions, fact, analytics marts, ML feature mart, and
  source/test YAML.

Kept:

- The generated/inaccessible `etl/dbt_project/dbt_packages/dbt_utils/` cache was
  left untouched and bypassed via `packages-install-path`.
- Other legacy Instacart/ML/MongoDB/API documentation and application paths.

Reason:

The active dbt graph now conforms to the locked NYC architecture. The former
model graph is archived rather than destroyed because its replacement has
local parse evidence but no cloud build evidence. Broader active-documentation
cleanup is explicitly assigned to Phase 4, so it was not started here.

## Student learning task

Core decision: **fact and mart grain**. Explain why `fct_trips` must have one
row per valid `trip_id`, why `mart_hourly_zone_demand` groups by date + hour +
pickup zone, and why `mart_operator_metrics` also needs year and month rather
than grouping only by operator. Manually verify the three `Grain:` comments in
the corresponding SQL files and compare them with their unique grain keys.

## Teach-back questions

1. Which event does one `fct_trips` row represent, and which field enforces that
   identity?
2. Why would removing `pickup_date_key` from the hourly-zone mart change its
   meaning?
3. Why does a successful dbt parse prove graph validity but not prove that the
   Gold Iceberg tables or dbt tests work in AWS?

## Failure/rerun experiment

After the first parse exposed the stale package cache, the package path was
redirected and dbt parse passed. The parse was run again after archiving the
legacy model tree and produced the same six-model graph. This verifies stable
project discovery; it does not verify a data-writing rerun.

## Later-phase confirmation

No Phase 4 or later work was started.
