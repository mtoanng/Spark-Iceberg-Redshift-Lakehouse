# Phase 4 Report — DuckDB consumer and MVP documentation

Date: 2026-07-19

## Phase implemented

Phase 4 only. The requested `docs/PROJECT1_BLUEPRINT_FINAL.md` does not exist;
repository inspection found `docs/PROJECT2_BLUEPRINT_FINAL.md`, and `AGENTS.md`
names it as the source of truth. Phase 4 was therefore implemented from that
file. This request also explicitly overrides the failed/`NOT VERIFIED` gate in
the Phase 3 report.

The phase adds a read-only-boundary DuckDB consumer, the five required fixed
queries, deterministic Gold fixtures, a smoke/failure-rerun test, and a
Milestone A README. It does not add Airflow, Great Expectations, three-month
orchestration, Terraform replacement, or any Phase 5 work.

## Files changed

Added:

- `consumer/__init__.py`
- `consumer/duckdb_consumer.py`
- `consumer/queries/__init__.py`
- `consumer/queries/hourly_pickups_by_zone.sql`
- `consumer/queries/operator_trip_count_average_fare.sql`
- `consumer/queries/top_pickup_zones.sql`
- `consumer/queries/fare_driver_pay_reconciliation.sql`
- `consumer/queries/explain_filtered_trips.sql`
- `tests/fixtures/__init__.py`
- `tests/fixtures/gold_fixture.py`
- `tests/contract/__init__.py`
- `tests/contract/test_duckdb_smoke.py`
- `tests/unit/test_duckdb_consumer.py`
- `docs/PHASE_4_REPORT.md`

Replaced/updated:

- `README.md`
- `requirements.txt`

Archived under `legacy/`:

- `warehouse/`
- `Dockerfile.warehouse`
- `docker-compose.yml`
- `Makefile`
- `test_complete_pipeline.py`
- `.gitlab-ci.yml`
- `docs/DEVELOPMENT.md`
- `FIX_GLUE_ACCESS.md`
- `terraform/README.md`

## DuckDB contract

`DuckDBGoldConsumer.run()` accepts a `QueryName`, not SQL text. The five enum
members map to bundled reviewed SQL resources. Production configuration
requires exactly the six junior Gold table locations and creates views in an
in-memory DuckDB connection using `iceberg_scan`. Those views read the
canonical Iceberg tables; the consumer exposes no lakehouse write operation.

The local tests inject an in-memory Gold fixture rather than starting Spark or
reading the approximately 2 GB staged Parquet set. This stays within the
laptop resource contract.

## Commands and results

Expected behavior before verification: the fixed-query tests should return the
deterministic fixture values; arbitrary SQL and incomplete/unsafe Iceberg
configuration should fail; all earlier unit contracts should remain green;
dbt parse should keep the exact six-model Gold graph without starting Spark.

| Command | Result |
| --- | --- |
| `.\.venv\Scripts\python.exe -m pytest tests/unit/test_duckdb_consumer.py tests/contract/test_duckdb_smoke.py -q` | PASS: 9 passed. A cache-write permission warning occurred because this first targeted run did not disable pytest's cache; test execution was unaffected. |
| `.\.venv\Scripts\python.exe -m compileall -q consumer etl\sources etl\transforms tests` | PASS (exit 0). |
| `.\.venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\unit tests\contract -q` | PASS: 32 passed in 1.55 s. This includes all Phase 1–4 credential-independent fixture/unit/contract checks. |
| `dbt parse --profiles-dir . --target local_parse --no-partial-parse` with documented non-credential placeholders | PASS (exit 0) using dbt 1.11.12 and dbt-spark 1.10.3; no local Spark compile/build was run. |
| Active-doc scan for `instacart`, MongoDB, recommendation, FastAPI, query API, warehouse API, and Kaggle (excluding historical reports and the governing blueprint) | PASS: no obsolete active-documentation references. |
| Fixed-query mutation scan for `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `COPY`, `CREATE`, `DROP`, `ALTER`, `ATTACH`, `DETACH`, `CALL`, `PRAGMA`, and `INSTALL` | PASS: no mutation statement in the five query files. |
| `QueryName` count | PASS: exactly 5. |

The local `EXPLAIN ANALYZE` test asserts that DuckDB returns an analyzed
physical plan containing a table scan. It does not establish cloud or
production performance.

## Verified acceptance criteria

- Five fixed queries exist: hourly zone pickups, monthly operator metrics,
  monthly top pickup zones, fare/driver-pay reconciliation, and one filtered
  `EXPLAIN ANALYZE`.
- All five return the expected deterministic fixture results.
- The fixed-query API rejects a caller-provided SQL string.
- Iceberg registration requires exactly the six Gold contract tables and
  rejects unsafe S3 location strings.
- The smoke test crosses fixture Gold tables, the packaged SQL loader, named
  parameters, DuckDB execution, and result materialization.
- The active README documents the locked architecture, local checks, exact
  one-month run order, and current evidence limits.
- Active ML/MongoDB/unrestricted-query-service documentation and entry points
  replaced by this consumer have been archived.

## NOT VERIFIED

- AWS credentials, S3 uploads, Glue jobs, Glue Data Catalog objects, IAM, and
  any cloud deployment or teardown.
- Actual Bronze/Silver/Gold row counts, quarantine counts, physical Iceberg
  snapshots, and end-to-end reconciliation for `2024-01` or the four locally
  staged months.
- dbt-glue compile/run/build/test against AWS.
- DuckDB Iceberg extension loading, AWS credential-chain behavior, S3 reads,
  and query results against published Gold Iceberg tables.
- Idempotency, schema evolution, snapshot pinning, performance, compaction,
  recovery, costs, and full-year behavior.
- Airflow, Great Expectations, three-month reruns, and CI; these belong to
  later phases.

## Blockers and risks

- The actual Glue job names, bucket/prefixes, table locations, credentials, and
  source SHA-256 checksums are not available as verified evidence; README
  placeholders must be replaced only from a bounded cloud environment.
- The consumer's production Iceberg path cannot be credential-independently
  exercised. Its local evidence proves the query contract, not S3/catalog
  integration.
- Legacy Instacart DAG, ML, configuration, and Terraform implementation files
  still exist outside active documentation. They remain a cleanup risk until
  their blueprint replacements are implemented in their assigned later phases.
- The user reports four consecutive 2024 files in `data/`; Phase 4 did not scan
  or process them. Milestone A remains one month, while repeatable multi-month
  execution remains Phase 5.

## Cleanup Report

Removed:

- Active Kaggle, MongoDB, FastAPI, Uvicorn, Pydantic service, and SQL-validator
  dependency declarations from `requirements.txt`.
- Obsolete content in the root README.

Archived:

- The old `warehouse/` arbitrary-query/API/recommendation implementation and
  its tests.
- Its Docker Compose, warehouse Dockerfile, Makefile, full-pipeline test, and
  GitLab CI entry points under `legacy/instacart_service/`.
- Obsolete Instacart development, Glue-access, and Terraform documentation
  under `legacy/docs_instacart/`.

Kept:

- Historical Phase 0–3 reports and the governing blueprint.
- Existing legacy DAG/ML/configuration/Terraform code whose replacement is
  assigned to Phase 5 or Phase 6.
- The ignored local monthly Parquet files.

Reason:

The new fixed-query consumer passed before its service predecessor was
archived. Historical reports are evidence, and later-phase code was not
replaced during Phase 4. Large source files remain ignored and were not loaded
locally.

## Student learning task

Core decision: **DuckDB is a consumer, not the warehouse**. Open
`consumer/duckdb_consumer.py` and one bundled SQL file, then explain in your own
words why Iceberg remains canonical even though DuckDB executes the analytical
query. Manually verify that callers can select only a `QueryName` and cannot
submit SQL or write back to an Iceberg table.

## Teach-back questions

1. If DuckDB returns a result, where does the durable canonical Gold data still
   live, and why?
2. What security and maintenance problem would an unrestricted `run(sql)`
   method reintroduce?
3. Why does the passing in-memory `EXPLAIN ANALYZE` test not verify AWS Iceberg
   performance?

## Failure/rerun experiment

The smoke test first omits `source_month` from the operator query and asserts
DuckDB raises a missing-parameter error. It then reruns the same named query
with `source_year=2024` and `source_month=1` and verifies the ordered operator
results. This demonstrates a controlled consumer-query rerun after correcting
input; it does not claim pipeline or data-write idempotency.

## Later-phase confirmation

No Phase 5 or later implementation was started. In particular, no Airflow DAG,
Great Expectations checkpoint, multi-month rerun logic, Terraform replacement,
or CI pipeline was added.
