# Component map

This file maps active repository components to exact paths, entrypoints, functions/classes, inputs, outputs, configuration, IAM/resource dependencies, tests, deployment commands, verification commands, teardown behavior, and current verification status.

## End-to-end component table

| Component | Directory | Entrypoint | Important classes/functions | Input | Output | Configuration | IAM dependency | Infrastructure resource | Test | Deployment command | Verification command | Teardown behavior | Status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Source contract | `etl/sources` | `etl/sources/nyc_hvfhs.py` | `SourceFile`, `SourceManifestEntry`, `monthly_trip_filename`, `monthly_trip_uri`, `required_trip_columns`, `validate_trip_schema`, `inspect_local_source`, `canonical_trip_id`, `stable_run_id`, `manifest_decision` | Official `fhvhv_tripdata_YYYY-MM.parquet`, `taxi_zone_lookup.csv`, fixture files | Source identity, schema contract, deterministic run ID and trip ID | `DEFAULT_SOURCE_YEAR=2024`, `DEFAULT_SOURCE_MONTH=1`, source URI constants | None locally; S3 read in cloud through downstream jobs | S3 landing/reference objects | `tests/unit/test_nyc_hvfhs_source.py` | Source upload is planned by `scripts/run_smoke.ps1` and `scripts/run_release.ps1` | `.\.venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\unit\test_nyc_hvfhs_source.py -q` | Source files are canonical input; not deleted by default | Fixture-tested; AWS source upload NOT VERIFIED |
| Pure Bronze/Silver transform contract | `etl/transforms` | `etl/transforms/nyc_hvfhs.py` | `BronzeBatch`, `SilverBatch`, `Reconciliation`, `bronze_records`, `load_zone_ids`, `transform_silver`, `reconcile` | Python fixture rows and Taxi Zone fixture | Bronze rows, Silver rows, quarantine rows, reconciliation counts | Metadata columns in `METADATA_COLUMNS`; reason-code rules in `_reason_code` | None locally | Mirrors Iceberg Bronze/Silver tables | `tests/unit/test_nyc_hvfhs_transform.py` | Not deployed directly; logic mirrors Glue jobs | `.\.venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\unit\test_nyc_hvfhs_transform.py -q` | No teardown; pure local code | Fixture-tested |
| Run manifest state machine | `etl/manifests` | `etl/manifests/nyc_hvfhs.py` | `RunStatus`, `SourceRunManifest`, `retry_is_safe` | `SourceFile`, status transitions, counts, validation metadata | In-memory contract for `ops.source_run_manifest` rows | Run statuses: `discovered`, `bronze_published`, `ge_passed`, `ge_blocked`, `silver_published`, `failed` | None locally; Glue job writes need Glue/S3 permissions | `ops.source_run_manifest` Iceberg table | `tests/unit/test_nyc_manifest_and_ge.py`, `tests/unit/test_nyc_phase5_contracts.py` | Created by `initialize_nyc_iceberg_tables.py`; mutated by Glue jobs | `.\.venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\unit\test_nyc_manifest_and_ge.py tests\unit\test_nyc_phase5_contracts.py -q` | Canonical manifest data is not recursively deleted by default | Contract-tested; physical Iceberg state NOT VERIFIED |
| Iceberg catalog DDL | `etl/iceberg` | `etl/iceberg/catalog.py` | `TableSpec`, `TABLE_SPECS`, `namespace_ddl`, `table_ddl` | Warehouse URI and table specs | DDL for `bronze`, `silver`, and `ops` Iceberg tables | Default catalog `glue_catalog`; warehouse root from Terraform output `s3_warehouse_uri` | Glue service role must create/read/write Glue Catalog and S3 warehouse | `aws_glue_catalog_database.namespace`; Iceberg tables under S3 warehouse | `tests/unit/test_iceberg_catalog.py` | Glue job `aws_glue_job.initialize`; script `etl/glue_jobs/initialize_nyc_iceberg_tables.py` | `.\.venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\unit\test_iceberg_catalog.py -q` | Terraform destroy removes resources only after approval; protected bucket has `force_destroy=false` | DDL contract-tested; physical catalog NOT VERIFIED |
| Iceberg lifecycle extension | `etl/iceberg` | `etl/iceberg/lifecycle.py` | `SchemaEvolutionPlan`, `SnapshotReference`, `SnapshotManifest`, `plan_2025_hvfhs_schema_evolution`, `build_snapshot_manifest`, `pinned_snapshot_reference`, `should_compact`, `retention_dry_run`, `orphan_file_dry_run` | Snapshot IDs, file metrics, discovered paths | Schema evolution plan and dry-run lifecycle decisions | Phase 7 only | Glue/S3/Iceberg permissions in cloud | `aws_glue_job.schema_evolution`; Iceberg metadata files | `tests/unit/test_iceberg_lifecycle.py` | Terraform defines `aws_glue_job.schema_evolution` | `.\.venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\unit\test_iceberg_lifecycle.py -q` | Dry-run only in code; no orphan deletion by default | Contract-tested only; advanced execution NOT VERIFIED |
| Glue table initializer | `etl/glue_jobs` | `etl/glue_jobs/initialize_nyc_iceberg_tables.py` | `_optional_arg`, `main` | Glue args including `WAREHOUSE_URI` or default argument wiring | Namespaces and Iceberg tables | `CATALOG_NAME`, warehouse URI argument/defaults | Glue service role | `aws_glue_job.initialize` | `tests/unit/test_iceberg_catalog.py`, `tests/unit/test_phase_c_deployment.py` | `terraform apply` creates job; Airflow/operator run can call it manually | `.\.venv\Scripts\python.exe scripts\package_glue_jobs.py --output build\nyc_glue_jobs.zip --check` | Terraform destroy removes job after approval | Packaged; execution NOT VERIFIED |
| Glue Bronze ingestion | `etl/glue_jobs` | `etl/glue_jobs/nyc_bronze_ingestion.py` | `_optional_arg`, `_table`, `_merge_manifest`, `_may_process`, `main` | `SOURCE_URI`, `SOURCE_YEAR`, `SOURCE_MONTH`, `SOURCE_CHECKSUM`, `INGESTION_RUN_ID`, `TAXI_ZONE_URI`, `TAXI_ZONE_CHECKSUM`, optional `SOURCE_SIZE_BYTES`, `FORCE` | `bronze.bronze_hvfhs_trips`, `bronze.bronze_taxi_zones`, manifest `bronze_published` or `failed` | `CATALOG_NAME`, `BRONZE_DATABASE`, `OPS_DATABASE` | Glue service role: S3 landing/reference read, warehouse write, Glue Catalog access | `aws_glue_job.bronze`, `aws_s3_object.bronze_script` | `tests/unit/test_nyc_glue_phase_a_contract.py` | Airflow task `bronze_ingestion`; Terraform job `${var.project_name}-${var.environment}-bronze` | `.\.venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\unit\test_nyc_glue_phase_a_contract.py -q` | Canonical data retained; job removed by approved Terraform destroy | Static-tested; Glue execution NOT VERIFIED |
| Great Expectations Glue gate | `etl/glue_jobs`, `etl/quality` | `etl/glue_jobs/nyc_great_expectations_checkpoint.py` | `_suite`, `_persist`, `main`; local `expectation_suite`, `evaluate_fixture_ge_checkpoint` | Bronze month and Taxi Zone table | Manifest `ge_passed` or `ge_blocked`; failure raises before Silver | `SOURCE_YEAR`, `SOURCE_MONTH`, `INGESTION_RUN_ID`, `CATALOG_NAME`, `BRONZE_DATABASE`, `OPS_DATABASE`; Glue module `great-expectations==1.19.0` | Glue service role: read Bronze, write manifest | `aws_glue_job.great_expectations`, `aws_s3_object.great_expectations_script` | `tests/unit/test_nyc_manifest_and_ge.py`, `tests/unit/test_nyc_glue_phase_a_contract.py` | Airflow task `great_expectations_checkpoint`; Terraform job `${var.project_name}-${var.environment}-great-expectations` | `.\.venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\unit\test_nyc_manifest_and_ge.py -q` | No data deletion; failed gate prevents downstream Silver | Contract-tested; Glue GE execution NOT VERIFIED |
| Glue Silver/quarantine | `etl/glue_jobs` | `etl/glue_jobs/nyc_silver_transform.py` | `_optional_arg`, `_table`, `_fingerprint`, `main` | Month-scoped Bronze trips, Bronze zones, manifest `ge_passed` | `silver.silver_trips`, `silver.quarantine_trips`, manifest `silver_published` or `failed` | `SOURCE_YEAR`, `SOURCE_MONTH`, `INGESTION_RUN_ID`, `CATALOG_NAME`, `BRONZE_DATABASE`, `SILVER_DATABASE`, `OPS_DATABASE` | Glue service role: read Bronze/Silver, write Silver/quarantine/manifest | `aws_glue_job.silver`, `aws_s3_object.silver_script` | `tests/unit/test_nyc_glue_phase_a_contract.py`, `tests/unit/test_nyc_hvfhs_transform.py` | Airflow task `silver_transform`; Terraform job `${var.project_name}-${var.environment}-silver` | `.\.venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\unit\test_nyc_glue_phase_a_contract.py tests\unit\test_nyc_hvfhs_transform.py -q` | Canonical data retained; job removed by approved Terraform destroy | Static/fixture-tested; Glue execution NOT VERIFIED |
| Post-Gold quality checkpoint | `etl/glue_jobs`, `etl/quality` | `etl/glue_jobs/nyc_quality_checkpoint.py` | `_count_for_month`, `_assert_quality`, `main`; local `evaluate_fixture_checkpoint` | Bronze, Silver, quarantine, Gold fact for one month | Fails on unreconciled counts, duplicate Silver trip IDs, missing quarantine reasons, or fact/Silver mismatch | `SOURCE_YEAR`, `SOURCE_MONTH` | Glue service role: read Bronze/Silver/Gold | `aws_glue_job.quality`, `aws_s3_object.quality_script` | `tests/unit/test_nyc_phase5_contracts.py` | Airflow task `quality_checkpoint`; Terraform job `${var.project_name}-${var.environment}-quality` | `.\.venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\unit\test_nyc_phase5_contracts.py -q` | Read-only checkpoint; no data deletion | Contract-tested; Glue execution NOT VERIFIED |
| Airflow monthly/backfill DAGs | `etl/dags` | `etl/dags/nyc_hvfhs_monthly_dag.py` | `MONTHLY_DAG_ID`, `BACKFILL_DAG_ID`, `_prepare_month`, `_monthly_params`, `_backfill_params` | Manual `year`, `month`, `force`; Airflow Variables for landing URI, checksums, job names, manifest URI, Athena config | Ordered task execution and XCom audit payload | Airflow Variables: `nyc_landing_uri`, `nyc_taxi_zone_uri`, `nyc_hvfhs_YYYY_MM_sha256`, `nyc_hvfhs_YYYY_MM_size_bytes`, `nyc_bronze_job_name`, `nyc_great_expectations_job_name`, `nyc_silver_job_name`, `nyc_quality_checkpoint_job_name`, `nyc_project_root`, `nyc_manifest_uri`, `nyc_gold_row_count`, `glue_gold_database`, `athena_workgroup`, `athena_smoke_enabled` | AWS default connection using instance profile in cloud | Optional `aws_instance.airflow_runner`, `aws_iam_instance_profile.airflow_runner` | `tests/unit/test_nyc_airflow_dag.py` | Dockerfile `Dockerfile.airflow`; optional runner bootstrap `scripts/bootstrap_airflow_runner.sh` | `.\.venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\unit\test_nyc_airflow_dag.py -q` | Optional EC2 runner destroyed by Terraform after approval | Topology-tested with stubs; deployed Airflow NOT VERIFIED |
| Monthly run audit | `etl/orchestration` | `etl/orchestration/nyc_hvfhs_runs.py` | `MonthlyRunRequest`, `RunAudit`, `audit_for_source`, `sequential_backfill_requests` | `year`, `month`, `force`, `SourceFile` | Audit object and four-month request plan | Month bounds in class validation | None locally | Consumed by Airflow | `tests/unit/test_nyc_phase5_contracts.py` | Imported by Airflow DAG | `.\.venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\unit\test_nyc_phase5_contracts.py -q` | No teardown; pure local code | Contract-tested |
| dbt Gold project | `etl/dbt_project` | `etl/dbt_project/dbt_project.yml` | Models: `dim_operator`, `dim_zone`, `dim_date`, `fct_trips`, `mart_hourly_zone_demand`, `mart_operator_metrics` | `silver.silver_trips`, `bronze.bronze_taxi_zones` | Gold Iceberg tables in `gold` namespace | `profiles.yml` targets `glue` and `local_parse`; environment variables include `GLUE_ROLE_ARN`, `S3_GOLD_PATH` for parse | dbt-glue/Glue role for real build | Glue Catalog `gold`; S3 warehouse Gold paths | `tests/unit/test_dbt_gold_contract.py`, `etl/dbt_project/tests/fct_trips_reconciles_to_silver.sql` | Airflow task `dbt_build`: `dbt build --profiles-dir . --target glue` | From `etl\dbt_project`: `..\..\.venv\Scripts\dbt.exe parse --profiles-dir . --target local_parse --no-partial-parse` | Gold data retained by default; Terraform does not recursively delete canonical S3 | Static/dbt parse contract only; dbt-glue execution NOT VERIFIED |
| Publication manifest | `scripts` | `scripts/publish_publication_manifest.py` | CLI `main` | Manifest URI, source URI, run ID, source year/month, Gold row count, `--validated` | Publication manifest JSON | CLI args; Airflow Variable `nyc_manifest_uri` | In cloud, write permission to manifest URI | S3 manifest path under project bucket | `tests/unit/test_phase_c_deployment.py` checks deployment plan; `scripts/reconcile_outputs.py` handles manifest checks | Airflow task `publication_manifest` | `.\.venv\Scripts\python.exe scripts\reconcile_outputs.py <manifest>` when a manifest exists | Manifest retained as evidence/canonical publication state | Script exists; cloud publication NOT VERIFIED |
| Athena runner | `athena` | `athena/query_runner.py` | `AthenaQueryRunner`, `AthenaQueryResult`, `AthenaQueryError` | SQL text, database, workgroup, optional catalog, parameters | Query rows, query ID, scanned bytes, output location | AWS SDK default credential chain; region optional | IAM policy `aws_iam_role_policy.athena_gold_query` for query actions, Gold read, Glue metadata, result prefix write | `aws_athena_workgroup.gold_query` | `tests/unit/test_athena_runner.py` | Used by `athena/verify_gold.py`; optional Airflow `athena_smoke` | `.\.venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\unit\test_athena_runner.py -q` | Workgroup has `force_destroy=false`; results prefix not recursively deleted by default | Mock-tested; real Athena NOT VERIFIED |
| Athena smoke verifier | `athena` | `athena/verify_gold.py` | `GoldSmokeResult`, `verify_gold_smoke`, `main` | Year, month, database, workgroup, catalog, region | Pass/fail result for Gold smoke query | CLI args `--year`, `--month`, `--database`, `--workgroup`, `--catalog`, `--region` | Same as Athena runner | `aws_athena_workgroup.gold_query` | `tests/contract/test_athena_smoke.py` | Airflow task `athena_smoke` when enabled | `.\.venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\contract\test_athena_smoke.py -q` | Read-only; no data teardown | Mock-tested; real Athena NOT VERIFIED |
| Athena SQL pack | `athena/sql` | `gold_smoke.sql`, `mart_hourly_zone_demand.sql`, `iceberg_history.sql`, `time_travel.sql.tmpl` | SQL files only | Gold tables and Iceberg metadata | Read-only query results | Parameters/template placeholders constrained by tests | Athena workgroup and read-only Gold/Glue permissions | `aws_athena_workgroup.gold_query` | `tests/unit/test_athena_sql.py` | Called by Athena runner/verifier or operator manually | `.\.venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\unit\test_athena_sql.py -q` | Read-only | Static-tested; real query results NOT VERIFIED |
| Glue package | `scripts` | `scripts/package_glue_jobs.py` | CLI `main` | Python modules and Glue entrypoints | `build/nyc_glue_jobs.zip` | `--output`, `--check` | Upload requires S3 write in Terraform | `aws_s3_object.glue_package` | `tests/unit/test_phase_c_deployment.py` | `.\.venv\Scripts\python.exe scripts\package_glue_jobs.py --output build\nyc_glue_jobs.zip --check` | Same command with `--check` validates package | Build artifact can be deleted locally; S3 object destroyed by Terraform after approval | Static package check available |
| Terraform deployment | `terraform` | `terraform/main.tf` | Resources listed below | Variables in `terraform/variables.tf`, package zip, scripts, AWS credentials via environment/profile | S3, Glue Catalog, Glue jobs, IAM, Athena, optional Airflow runner | `terraform.tfvars` or CLI vars; no secrets committed | AWS caller permissions to create defined resources | See deployment-resource table | `tests/unit/test_phase_c_deployment.py`; Terraform fmt/validate | `terraform -chdir=terraform init`, `terraform -chdir=terraform plan`, approved `terraform -chdir=terraform apply` | `terraform fmt -check ...`; `terraform -chdir=terraform validate` | Approved `scripts/teardown.ps1` / `terraform destroy`; canonical data protected by `force_destroy=false` | Validation documented; apply/destroy NOT VERIFIED |
| CI workflow | `.github/workflows` | `.github/workflows/ci.yml` | Workflow jobs/steps | Fresh checkout | Python, dbt, package, Terraform validation results | GitHub Actions environment | GitHub runner; no AWS credentials for local checks | GitHub Actions | Referenced by `docs/CODEBASE_INDEX.md` | Push/PR triggers | Local equivalent commands in README | No teardown | Workflow exists; hosted run NOT VERIFIED in this turn |

## Deployment-layer explanation

### Terraform

Terraform lives in [terraform](../../terraform). Active files are:

- [terraform/main.tf](../../terraform/main.tf): AWS provider, default tags, caller identity, outputs.
- [terraform/variables.tf](../../terraform/variables.tf): region, environment, project name, S3 prefixes, Athena cutoff, Glue worker settings, Glue package path, optional Airflow runner inputs.
- [terraform/s3.tf](../../terraform/s3.tf): protected project S3 bucket with versioning, SSE-S3, and public access block.
- [terraform/glue_catalog.tf](../../terraform/glue_catalog.tf): Glue databases for canonical namespaces.
- [terraform/iam.tf](../../terraform/iam.tf): Glue service role, lakehouse access policy, Athena Gold query policy, optional Airflow runner role/profile and policy.
- [terraform/glue_jobs.tf](../../terraform/glue_jobs.tf): Glue package/script S3 objects and Glue jobs.
- [terraform/athena.tf](../../terraform/athena.tf): bounded Athena workgroup.
- [terraform/airflow_runner.tf](../../terraform/airflow_runner.tf): optional EC2 Airflow runner.

Important resources:

| Resource | Purpose |
| --- | --- |
| `aws_s3_bucket.lakehouse` | Canonical project bucket |
| `aws_s3_bucket_versioning.lakehouse` | Bucket versioning |
| `aws_s3_bucket_server_side_encryption_configuration.lakehouse` | SSE-S3 bucket encryption |
| `aws_s3_bucket_public_access_block.lakehouse` | Public access block |
| `aws_glue_catalog_database.namespace` | Glue namespaces |
| `aws_iam_role.glue_service` | Glue execution role |
| `aws_iam_role_policy.glue_lakehouse` | Glue S3/Catalog permissions |
| `aws_s3_object.glue_package` | Shared Glue Python zip |
| `aws_s3_object.initialize_script` | Initializer script object |
| `aws_s3_object.bronze_script` | Bronze script object |
| `aws_s3_object.silver_script` | Silver script object |
| `aws_s3_object.quality_script` | Quality checkpoint script object |
| `aws_s3_object.great_expectations_script` | Great Expectations script object |
| `aws_s3_object.schema_evolution_script` | Phase 7 schema evolution script object |
| `aws_glue_job.initialize` | Iceberg table initializer |
| `aws_glue_job.bronze` | Bronze ingestion |
| `aws_glue_job.silver` | Silver/quarantine transform |
| `aws_glue_job.quality` | Post-Gold reconciliation |
| `aws_glue_job.great_expectations` | Mandatory pre-Silver GE gate |
| `aws_glue_job.schema_evolution` | Phase 7 2025 schema evolution |
| `aws_athena_workgroup.gold_query` | Bounded Gold Athena workgroup |
| `aws_iam_role_policy.athena_gold_query` | Athena query/read/result policy |
| `aws_iam_role.airflow_runner` | Optional Airflow EC2 role |
| `aws_iam_instance_profile.airflow_runner` | Optional Airflow instance profile |
| `aws_instance.airflow_runner` | Optional temporary Airflow runner |

### S3 layout

The layout is variable-driven:

- `s3://${aws_s3_bucket.lakehouse.id}/${var.landing_prefix}` for monthly source files;
- `s3://${aws_s3_bucket.lakehouse.id}/${var.reference_prefix}` for Taxi Zone lookup;
- `s3://${aws_s3_bucket.lakehouse.id}/${var.warehouse_prefix}` for Iceberg data and metadata;
- `s3://${aws_s3_bucket.lakehouse.id}/${var.athena_results_prefix}` for Athena query results;
- `s3://${aws_s3_bucket.lakehouse.id}/tmp/` for Glue temporary files;
- `s3://${aws_s3_bucket.lakehouse.id}/${var.glue_package_s3_key}` for the packaged Python runtime.

Canonical data is not recursively deleted by default.

### Glue packaging

[scripts/package_glue_jobs.py](../../scripts/package_glue_jobs.py) creates `build/nyc_glue_jobs.zip` by default. Terraform uploads it as `aws_s3_object.glue_package`. Glue jobs receive it through `--extra-py-files`.

Package validation command:

```powershell
.\.venv\Scripts\python.exe scripts\package_glue_jobs.py --output build\nyc_glue_jobs.zip --check
```

### Glue Catalog

[terraform/glue_catalog.tf](../../terraform/glue_catalog.tf) creates Glue databases. [etl/iceberg/catalog.py](../../etl/iceberg/catalog.py) creates Iceberg DDL against `glue_catalog`. Glue, dbt-glue, publication state, and Athena all depend on consistent namespace wiring.

### Airflow deployment

[Dockerfile.airflow](../../Dockerfile.airflow) and [requirements-airflow.txt](../../requirements-airflow.txt) define the orchestration image. [scripts/bootstrap_airflow_runner.sh](../../scripts/bootstrap_airflow_runner.sh) is wired into the optional EC2 runner as `user_data`. The runner is disabled unless `airflow_runner_ami_id` is set.

Airflow must use instance-profile authentication through the default AWS SDK chain. Static AWS keys must not be committed or placed in DAG code.

### Role-based authentication

The repository uses IAM roles, not static keys:

- `aws_iam_role.glue_service` for Glue jobs;
- `aws_iam_role.airflow_runner` and `aws_iam_instance_profile.airflow_runner` for optional Airflow/query runner host;
- `aws_iam_role_policy.athena_gold_query` to limit Athena to Gold reads, Glue metadata, workgroup query actions, and result-prefix writes.

### Athena workgroup

[terraform/athena.tf](../../terraform/athena.tf) defines `aws_athena_workgroup.gold_query` with:

- name `${var.project_name}-${var.environment}-gold`;
- `force_destroy = false`;
- Athena engine version 3;
- enforced workgroup configuration;
- CloudWatch metrics enabled;
- `bytes_scanned_cutoff_per_query = var.athena_bytes_scanned_cutoff`;
- SSE-S3 result encryption;
- output location under the existing project bucket.

### Deployment and teardown scripts

Active deployment-related scripts:

- [scripts/package_glue_jobs.py](../../scripts/package_glue_jobs.py): deterministic package build/check.
- [scripts/sync_terraform_env.ps1](../../scripts/sync_terraform_env.ps1): Terraform environment sync helper.
- [scripts/run_smoke.ps1](../../scripts/run_smoke.ps1): smoke command plan.
- [scripts/run_release.ps1](../../scripts/run_release.ps1): four-month release command plan.
- [scripts/run_e2e.py](../../scripts/run_e2e.py): E2E command-plan generator.
- [scripts/reconcile_outputs.py](../../scripts/reconcile_outputs.py): manifest reconciliation check.
- [scripts/publish_publication_manifest.py](../../scripts/publish_publication_manifest.py): publication manifest writer.
- [scripts/teardown.ps1](../../scripts/teardown.ps1): guarded teardown.
- [scripts/verify_teardown.py](../../scripts/verify_teardown.py): read-only teardown verification.

Legacy/conflicting script names that exist but should not be treated as the active source path without cleanup:

- [scripts/download_kaggle_dataset.py](../../scripts/download_kaggle_dataset.py)
- [scripts/setup_kaggle.py](../../scripts/setup_kaggle.py)
- [scripts/explore_data_local.py](../../scripts/explore_data_local.py)
- [scripts/upload_to_s3.py](../../scripts/upload_to_s3.py)
- [scripts/validate_iceberg_tables.py](../../scripts/validate_iceberg_tables.py)

## Current verification commands

Repository-level local commands from [README.md](../../README.md):

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

Terraform validation:

```powershell
terraform fmt -check terraform\main.tf terraform\variables.tf terraform\s3.tf terraform\glue_catalog.tf terraform\iam.tf terraform\glue_jobs.tf terraform\athena.tf terraform\airflow_runner.tf
terraform -chdir=terraform validate
```

These commands are credential-independent except Terraform provider initialization/validation prerequisites. Passing them does not prove AWS deployment.

## Table and model names

Upstream Iceberg tables from [etl/iceberg/catalog.py](../../etl/iceberg/catalog.py):

- `bronze.bronze_hvfhs_trips`
- `bronze.bronze_taxi_zones`
- `silver.silver_trips`
- `silver.quarantine_trips`
- `ops.source_run_manifest`

Gold dbt models:

- `dim_operator`
- `dim_zone`
- `dim_date`
- `fct_trips`
- `mart_hourly_zone_demand`
- `mart_operator_metrics`

Glue job Terraform resources:

- `aws_glue_job.initialize`
- `aws_glue_job.bronze`
- `aws_glue_job.silver`
- `aws_glue_job.quality`
- `aws_glue_job.great_expectations`
- `aws_glue_job.schema_evolution`

Airflow DAG IDs:

- `nyc_hvfhs_monthly`
- `nyc_hvfhs_four_month_backfill`
