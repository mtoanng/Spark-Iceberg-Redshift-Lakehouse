# Deployment code status

Audit updated: 2026-07-21

Cloud status: **NOT VERIFIED**. No AWS query, Terraform apply/destroy, Glue
run, Airflow run, or dbt-glue build was performed in Closure Phases A or B.

## Current classifications

| Artifact | Classification | Status |
| --- | --- | --- |
| `etl/sources/`, `etl/manifests/`, `etl/transforms/` | VALID AND COMPLETE | Fixture-tested source identity, durable-manifest contract, Bronze/Silver/quarantine reconciliation, and rerun state machine. |
| `etl/quality/nyc_hvfhs_ge.py` and GE Glue job | VALID BUT INCOMPLETE | Mandatory pre-Silver contract is implemented and locally tested; Glue packaging/execution is unverified. |
| `etl/dags/nyc_hvfhs_monthly_dag.py` | VALID BUT INCOMPLETE | Stubbed topology proves Bronze → GE → Silver; deployed Airflow behavior is unverified. |
| `etl/iceberg/catalog.py` | VALID BUT INCOMPLETE | Upstream and operations DDL is defined; physical Glue Catalog tables are unverified. |
| `athena/` | VALID AND COMPLETE | Exactly four read-only SQL artifacts, generic Boto3 runner, and Gold smoke verifier have mocked/static tests. |
| `terraform/athena.tf` | VALID BUT INCOMPLETE | One workgroup uses the existing bucket prefix, SSE-S3, metrics, and scan cutoff; apply is unverified. |
| Athena policy in `terraform/iam.tf` | VALID BUT INCOMPLETE | One scoped policy limits actions to Gold reads, Glue metadata, the one workgroup, and result prefix; IAM evaluation is unverified. |
| `.env.cloud.example` | VALID AND COMPLETE | Non-secret Athena configuration names only; uses the default SDK credential chain. |
| `requirements.txt` and CI workflow | VALID AND COMPLETE | Active dependencies/checks include Boto3 and Great Expectations; the retired local query dependency is absent. |
| Glue packaging and temporary Airflow runner | VALID BUT INCOMPLETE | Required deployment-closure work remains outside these phases. |
| Legacy directories and ignored state/private files | STALE or CONFLICTS WITH FROZEN ARCHITECTURE | Retained intentionally because they were not approved for this cleanup pass. |

## Deferred by the approved minimal Athena scope

Lake Formation, customer-managed KMS, a dedicated results bucket, multiple
roles, dashboards, alarms, an evidence exporter, generic query testing, a
large query catalog, automated time-travel verification, and Athena writes are
not implemented.

## Verification boundary

Credential-independent tests prove code behavior only. AWS credentials,
instance-profile execution, S3 and Glue access, result encryption, scan
cutoff enforcement, metrics publication, query IDs, scanned bytes, costs,
Iceberg metadata/time-travel behavior, and teardown are **NOT VERIFIED**.

## Closure Phase C update

| Artifact | Classification | Status |
| --- | --- | --- |
| `terraform/*.tf` active module | VALID BUT INCOMPLETE | NYC-only S3, four Glue namespaces, Glue jobs, Athena, and optional Airflow runner/profile; no AWS apply performed. |
| `scripts/package_glue_jobs.py` | VALID AND COMPLETE | Deterministic zip, entrypoint/dependency/catalog manifest, and static test pass. |
| `Dockerfile.airflow`, `requirements-airflow.txt`, `.env.cloud.example` | VALID BUT INCOMPLETE | Airflow 3 image/import path and instance-profile environment contract are defined; image build/runtime unverified. |
| `scripts/run_smoke.ps1`, `scripts/run_release.ps1`, `scripts/run_e2e.py` | VALID AND COMPLETE | Unexecuted smoke and four-month command plans; local plan generation passes. |
| `scripts/reconcile_outputs.py` | VALID AND COMPLETE | Credential-independent manifest reconciliation contract. |
| `scripts/teardown.ps1`, `scripts/verify_teardown.py` | VALID BUT INCOMPLETE | Destroy-plan-only and read-only post-teardown checks; no teardown was run. |
| `.github/workflows/ci.yml` | VALID BUT INCOMPLETE | Adds package, shell, dbt, Python, GE, and Terraform validation; hosted execution is unverified. |
| `terraform/tfplan` | REMOVED | Approved tracked obsolete plan removed; ignored state/credentials/generated plans remain protected. |
