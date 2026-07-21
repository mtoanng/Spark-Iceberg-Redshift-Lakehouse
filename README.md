# NYC HVFHV Iceberg Lakehouse
<img width="1231" height="627" alt="image" src="https://github.com/user-attachments/assets/b272c3c1-77b4-4ed0-baf1-cfb1f3dfa153" />

An AWS lakehouse for the official NYC TLC High-Volume For-Hire Vehicle (HVFHV)
trip dataset. The pipeline is month-scoped and uses Amazon S3, AWS Glue,
Apache Iceberg, Great Expectations, dbt, Airflow 3, and Amazon Athena.

> **End-to-end data lakehouse with ML-powered product recommendations**  

---

```text
NYC TLC Parquet + Taxi Zone lookup
  -> S3 landing/reference
  -> Glue/PySpark Bronze + Iceberg
  -> Great Expectations promotion gate
  -> Glue/PySpark Silver + reason-coded quarantine
  -> dbt-glue Gold
  -> publication manifest
  -> read-only Athena query pack
```

Iceberg tables in S3 and the AWS Glue Data Catalog are the canonical storage
and catalog layers. Athena is the bounded analytical serving layer. Invalid
rows are retained in Silver quarantine with deterministic reason codes; they
are not silently discarded.

## Status

Closures A–C are implemented as static, credential-independent code and
contracts. Local verification currently includes:

- 59 passing unit and contract tests;
- Python compilation and focused lint/format checks;
- Athena runner, SQL, and Gold smoke contracts;
- deterministic Glue package validation; and
- Terraform format and validation checks.

AWS credentials, S3/Glue/Iceberg execution, remote Great Expectations,
Airflow scheduling, dbt-glue execution, Athena results, Terraform apply, and
teardown remain **not verified**. No cloud resources are created by the local
commands below.

## Quick start: local checks

Use the repository virtual environment. Local checks use fixtures and do not
start a local Spark or Airflow service.

```powershell
.\.venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\unit tests\contract -q
.\.venv\Scripts\python.exe -m compileall -q athena etl tests
.\.venv\Scripts\python.exe scripts\package_glue_jobs.py --output build\nyc_glue_jobs.zip --check

$env:GLUE_ROLE_ARN = 'arn:aws:iam::000000000000:role/local-parse-only'
$env:S3_GOLD_PATH = 's3://local-parse-only/gold'
Push-Location etl\dbt_project
..\..\.venv\Scripts\dbt.exe deps --profiles-dir .
..\..\.venv\Scripts\dbt.exe parse --profiles-dir . --target local_parse --no-partial-parse
Pop-Location
```

The `local_parse` target validates the dbt graph without cloud credentials.
Do not run local `dbt compile` or a local Spark/Airflow service unless you
intend to provide and manage those runtimes.

## Repository map

| Path | Purpose |
| --- | --- |
| `etl/sources` | Source and Taxi Zone contracts |
| `etl/glue_jobs` | Bronze, Silver, quality, and Iceberg Glue entrypoints |
| `etl/manifests` | Durable monthly run-manifest state machine |
| `etl/quality` | Great Expectations suites and checkpoint logic |
| `etl/dbt_project` | Six Iceberg Gold models and dbt tests |
| `etl/dags` | Manually triggered Airflow 3 monthly DAGs |
| `athena` | Read-only query runner and Gold smoke verifier |
| `terraform` | Bounded NYC-only AWS deployment definition |
| `scripts` | Packaging, release, reconciliation, smoke, and teardown helpers |
| `tests/unit`, `tests/contract` | Credential-independent verification |
| `docs` | Architecture, status, evidence, and operator runbooks |

Gold contains exactly `dim_date`, `dim_operator`, `dim_zone`, `fct_trips`,
`mart_hourly_zone_demand`, and `mart_operator_metrics`.

## Monthly execution model

An approved remote AWS run follows this order for each month:

1. Upload the exact source Parquet and Taxi Zone lookup files after recording
   their SHA-256 checksums and sizes.
2. Initialize the Iceberg tables once.
3. Run Bronze ingestion with the source URI, checksum, month, and run ID.
4. Run the mandatory Great Expectations checkpoint. A blocking failure
   persists `ge_blocked` and prevents Silver publication.
5. Run Silver validation/transformation and reconcile valid rows with
   reason-coded quarantine.
6. Run `dbt build --target glue` for the six Gold models.
7. Reconcile outputs, publish the validated manifest, and run the bounded
   Athena smoke/business/history query pack.

The Airflow DAG `nyc_hvfhs_monthly` exposes `year`, `month`, and `force`
parameters. `nyc_hvfhs_four_month_backfill` sequences four monthly runs.
`force` permits retrying an identical completed source; it does not permit a
different checksum to replace an existing source.

## Remote deployment and teardown

These are operator runbooks for an approved disposable AWS environment, not
evidence that AWS has been run:

- [Implementation status](docs/IMPLEMENTATION_STATUS.md)
- [Deployment runbook](docs/DEPLOYMENT_RUNBOOK.md)
- [Cloud demo and teardown runbook](docs/CLOUD_DEMO_RUNBOOK.md)
- [Cloud evidence template](docs/CLOUD_EVIDENCE_TEMPLATE.md)
- [Codebase index](docs/CODEBASE_INDEX.md)
- [Project blueprint](docs/PROJECT2_BLUEPRINT_FINAL.md)

Before any apply or teardown, review the plan, set a budget limit, confirm
data-retention requirements, and obtain explicit approval. Terraform is
configured with protected storage and does not implicitly delete canonical
data.

## Scope boundary

The active project is the NYC HVFHV lakehouse. The `legacy/` tree contains
archived Instacart-era code and documentation and is not part of the active
pipeline.
