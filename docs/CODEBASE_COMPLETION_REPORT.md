# Project 2 codebase closure — Phase A report

Date: 2026-07-21

## Phase implemented

Closure Phase A: source contracts and durable manifests, Bronze ingestion,
mandatory Great Expectations gating, Silver transformation/quarantine,
namespace/catalog wiring, deterministic monthly reruns, and reconciliation
contracts.

No Athena work was started. No Terraform apply/destroy or AWS access occurred.
The serving-layer replacement was deferred to the next closure phase.

## Files changed

- Added `etl/manifests/` and `etl/quality/nyc_hvfhs_ge.py`.
- Added `etl/glue_jobs/nyc_great_expectations_checkpoint.py`.
- Updated Bronze, Silver, Iceberg initializer/catalog, monthly Airflow DAG,
  requirements, and focused unit contracts.
- Added `docs/IMPLEMENTATION_STATUS.md` and this report.

## Verified acceptance criteria

- The durable run-manifest contract includes source URI, checksum, size, month,
  deterministic run ID, statuses/timestamps, row counts, GE result fields, and
  failure information.
- Bronze writes one source-month partition at a time and persists
  `bronze_published`; checksum/URI replacement is blocked and a completed
  identical run requires `force=true`.
- The DAG order is Bronze → Great Expectations → Silver → dbt → post-Gold
  reconciliation. Silver itself checks persisted `ge_passed` before writing.
- Blocking GE failure is persisted as `ge_blocked` and prevents canonical
  Silver publication.
- Row-level validity observations remain reason-coded Silver quarantine rows;
  GE does not silently drop or substitute them.
- Silver and quarantine use Iceberg partition replacement after pre-write
  reconciliation, with cross-month duplicate IDs quarantined.
- Glue Catalog identifier construction supports `CATALOG_NAME`,
  `BRONZE_DATABASE`, `SILVER_DATABASE`, and `OPS_DATABASE` consistently.

## Commands and results

| Command | Result |
| --- | --- |
| `python -m black --check` on Phase A modules/tests | PASS |
| `python -m flake8 --select=F` on Phase A modules/tests | PASS |
| `python -m compileall -q etl tests` | PASS; it reports an existing inaccessible dbt package directory but exits 0. |
| `python -m pytest -p no:cacheprovider tests\\unit tests\\contract -q` | PASS: 53 passed. |
| Installed Great Expectations suite construction in unit tests | PASS. |
| Constrained five-row `local[1]` PySpark fixture | PASS after setting `PYSPARK_PYTHON` to the virtual-environment interpreter. |
| Documentation link validation | PASS: all local Markdown links in active Phase A/closure documentation resolve. |

The initial PySpark probe was not a code failure: PySpark attempted to launch
missing `python3` on Windows. Re-running the same five-row fixture with
`PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` set to `.venv\\Scripts\\python.exe`
passed. No production Parquet data was read.

## NOT VERIFIED

- AWS credentials, S3, IAM, Glue Catalog, Glue job packaging/registration,
  Glue/PySpark runtime, physical Iceberg writes, and manifest persistence in
  AWS.
- Great Expectations execution on the remote month-scoped Spark batch,
  Airflow scheduler/task execution, retries/task clears, and real XCom values.
- Physical Iceberg idempotency/recovery after a remote failure, dbt-glue Gold,
  publication manifests, Terraform plan/apply/destroy, teardown, and Athena.

## Cleanup report

Removed: none.

Archived: none.

Kept: serving-layer cleanup, Terraform/deployment artifacts, the post-Gold
quality reconciliation job, and historical reports. These remain outside the
approved Phase A scope; the post-Gold job is now explicitly separate from the
mandatory GE gate.

## Learning contract

Core decision: Great Expectations is a promotion gate, while Silver quarantine
is row-level evidence preservation. A structural source failure blocks Silver;
individual bad rows must still have deterministic reason codes rather than
being silently removed.

Student task: trace one invalid source row and one missing-required-column
source through the manifest states. Explain why the former can reach quarantine
while the latter must stop at `ge_blocked`.

Teach-back questions:

1. Why is the source checksum part of the monthly retry decision?
2. Why must Silver reject a run without persisted `ge_passed` even if its own
   validation could run?
3. Why are GE observations not a replacement for quarantine reason codes?

Failure/rerun experiment: change a fixture checksum by one character and
confirm `retry_is_safe` rejects it; restore the checksum, complete the manifest
state machine, then show that only `force=true` permits an identical completed
month to reprocess.

## Later-phase confirmation

No Athena implementation, deployment-architecture expansion, Terraform apply,
or serving-layer cleanup was started.

---

## Closure Phase B addendum — Athena replacement

Date: 2026-07-21

The approved minimal Athena layer is implemented: one Terraform workgroup
using the existing bucket result prefix, SSE-S3, CloudWatch metrics and a scan
cutoff; one scoped query policy; four read-only SQL artifacts; one generic
Boto3 runner; and one minimal Gold smoke verifier. The approved retired local
consumer package, tests/fixture, and dependency were removed only after the
replacement contract tests passed.

Verified locally: mocked runner success/failure/cancellation/pagination/timeout
and token forwarding, smoke-verifier pass/failure cases, and SQL safety tests.
AWS queries, workgroup/IAM behavior, query results, scanned bytes, costs, and
Terraform apply remain **NOT VERIFIED**. No later-phase work was started.

### Closure Phase B commands

| Command | Result |
| --- | --- |
| `python -m black --check athena ...` | PASS for all Phase B files. |
| `python -m flake8 --select=F athena ...` | PASS. |
| `python -m compileall -q athena etl tests` | PASS; reports the pre-existing inaccessible dbt package directory but exits 0. |
| `python -m pytest -p no:cacheprovider tests\\unit tests\\contract -q` | PASS: 55 passed. |
| Athena runner/verifier and SQL focused tests | PASS: 11 passed, using mocked clients only. |
| `terraform fmt -check -recursive terraform` | PASS. |
| `terraform -chdir=terraform validate` | PASS, with the pre-existing ignored private-tfvars warning for `s3_raw_prefix`. |
| Active secret scan and documentation-link validation | PASS. |
| Active serving-engine reference scan | PASS: no active references; historical reports, legacy archives, and the blueprint migration map were excluded. |

### Phase B cleanup report

Removed: the approved retired local consumer package; its unit/contract tests
and in-memory fixture; and its dependency entry.

Archived: none.

Kept: all unrelated deployment code, legacy archives, ignored state/private
inputs, and historical reports. They require separate approval or operator
review before removal.

Reason: the approved Athena replacement passed its static and mocked contracts
before the narrow deletion list was applied.

### Phase B learning contract

Core decision: Athena is a bounded read-only query surface; Glue Catalog and
Iceberg/S3 remain the catalog and canonical data layers.

Student task: explain why the workgroup scan cutoff protects query cost but
cannot prove Gold correctness or replace the Gold smoke verifier.

Teach-back questions:

1. Why does the runner pass a stable client request token to Athena?
2. Why can the Athena policy write only the results prefix and not Gold data?
3. Why is the time-travel template manual rather than automatically verified?

Failure/rerun experiment: use the mocked runner test to return `FAILED` after
`start_query_execution`; confirm the exception retains the query ID and state
reason, then return `SUCCEEDED` and verify the same request token is forwarded.

---

## Closure Phase C addendum — deployment closure

Date: 2026-07-21

Implemented Terraform migration to NYC-only Glue Catalog namespaces and
packaged Glue jobs; deterministic package manifest and S3 artifact contract;
Airflow 3 image/import/environment configuration; optional IMDSv2 instance
profile runner; publication/Athena hooks; smoke, four-month release, E2E,
reconciliation, and guarded teardown scripts; and expanded CI validation.

Removed only tracked `terraform/tfplan`, the approved obsolete saved plan.
Terraform state, credentials, private tfvars, and generated plans remain
ignored. No AWS resources were created, queried, applied, or destroyed.

Checks: targeted deployment contracts 11 passed; full repository suite after
the final Phase C changes 59 passed; deterministic package build passed;
Terraform format/validation passed with the pre-existing ignored private
tfvars warning; PowerShell syntax passed; Bash syntax is NOT VERIFIED because
WSL has no installed distribution; dbt deps/parse ran, but local dbt compile
timed out while starting the Spark adapter. Hosted CI execution and AWS
runtime remain NOT VERIFIED.

### Phase C cleanup report

Removed: tracked obsolete `terraform/tfplan` only.

Archived: none.

Kept: ignored Terraform state/backup, credentials/private variables, generated
plans/cache, valid NYC/Athena/Glue/Airflow deployment code, legacy history,
and unrelated scripts.

Reason: state and credentials require operator review; all other deletions were
outside the approved Phase C target.

### Phase C learning contract

Core decision: an instance profile is the authentication boundary for the
temporary Airflow runner; packaging and environment configuration must never
embed static access keys.

Student task: trace how the Glue zip, `--extra-py-files`, catalog namespace
arguments, and EC2 profile connect without credentials in the repository.

Teach-back questions:

1. Why is a Terraform plan not evidence that the Airflow runner or Glue job ran?
2. Why must the Glue artifact hash and runtime manifest be deterministic?
3. Why does teardown generate a destroy plan and then perform read-only checks?

Failure/rerun experiment: alter one source file before packaging and confirm
the zip hash changes; restore it and confirm two builds are byte-identical.
