# One-month controlled AWS deployment runbook

Status: **requires AWS execution verification**. Nothing in this runbook has
been executed against AWS by this code-finalization pass.

Use one approved disposable environment and January 2024 first. Configure the
AWS CLI default credential chain; never place keys in repository files.

## Required values

Set `AWS_REGION`, `TF_VAR_s3_bucket_name`, `GLUE_ROLE_ARN`, `S3_GOLD_PATH`,
`GLUE_GOLD_DATABASE`, and `ATHENA_WORKGROUP`. Copy
`terraform/terraform.tfvars.example` to an untracked `terraform.tfvars` and
replace placeholders. Keep `athena_bytes_scanned_cutoff` at 100 MiB unless a
reviewed plan lowers it.

## 1. Review and plan

```powershell
python scripts/package_glue_jobs.py --output build/nyc_glue_jobs.zip --check
terraform -chdir=terraform init -backend=false
terraform -chdir=terraform fmt -check -recursive
terraform -chdir=terraform validate
terraform -chdir=terraform plan -out one-month.tfplan
```

Expected: package check succeeds; Terraform reports no validation errors; the
plan contains only this project's private S3, Glue, IAM, Athena, and optional
runner resources. Stop if the plan replaces a bucket/database or enables
public access. The plan file is local-only and must not be committed.

## 2. Create the reviewed resources

After explicit operator approval only:

```powershell
terraform -chdir=terraform apply one-month.tfplan
```

Record outputs and resource names. Apply success proves provisioning only, not
the pipeline.

## 3. Stage and upload one official month

Downloading is optional when the official files already exist under `data/`:

```powershell
python -m scripts.fetch_source --year 2024 --month 1 --output-dir data
python -m scripts.upload_release_dataset --bucket <bucket> --year 2024 --month 1 --source-dir data
python -m scripts.upload_release_dataset --bucket <bucket> --year 2024 --month 1 --source-dir data --execute
```

Expected: the downloader refuses to start if the source plus its 256 MiB disk
reserve does not fit. It never writes directly to S3 or into the repository.

The first upload command is dry-run only. Expected output includes both S3
URIs, byte sizes, SHA-256 values, and Airflow variable JSON. The execute step
must report `uploaded` or `already-present`; stop on any identity conflict.

## 4. Initialize Iceberg

Get the provisioned names and run the initializer once:

```powershell
terraform -chdir=terraform output
aws glue start-job-run --job-name <initializer-job> --region $env:AWS_REGION
aws glue get-job-run --job-name <initializer-job> --run-id <run-id> --region $env:AWS_REGION
```

Poll `get-job-run` until `JobRunState` is `SUCCEEDED`. Expected: Bronze,
Silver, quarantine, and run-manifest tables exist as Iceberg format v2 tables
using Snappy and `source_year`/`source_month` partitions. Stop and capture the
CloudWatch error when the state is `FAILED`, `ERROR`, `TIMEOUT`, or `STOPPED`.

## 5. Configure the bounded Airflow runner

Use an approved Linux machine with Docker, at least 4 GiB RAM, enough disk for
the official image, outbound HTTPS, and either its Terraform instance profile
or another approved default AWS credential chain. For the optional private EC2
runner, SSM and AWS service access require NAT or the corresponding VPC
endpoints; Terraform intentionally does not create shared networking.

Copy `.env.cloud.example` to an untracked `build/airflow.env`. Fill values from
`terraform output` and the `airflow_variables` section emitted by the upload
script. In particular, set both direct dbt variables `GLUE_ROLE_ARN` and
`S3_GOLD_PATH`, plus the month-specific SHA-256 and byte-size variables.

On that remote machine from a fresh repository checkout:

```bash
docker build --file Dockerfile.airflow --tag nyc-hvfhs-airflow:3.3.0 .
docker volume create nyc-hvfhs-airflow-home
docker run --detach --name nyc-hvfhs-airflow --restart no \
  --env-file build/airflow.env \
  --volume nyc-hvfhs-airflow-home:/opt/airflow/runtime \
  --publish 127.0.0.1:8080:8080 \
  nyc-hvfhs-airflow:3.3.0 standalone
docker logs nyc-hvfhs-airflow
docker exec nyc-hvfhs-airflow airflow dags list
```

Expected: both `nyc_hvfhs_monthly` and
`nyc_hvfhs_four_month_backfill` appear with no import error. Keep the UI bound
to loopback and use SSM port forwarding if visual inspection is required.

## 6. Run the monthly path

Enable Athena smoke in `build/airflow.env` before starting the container, then
trigger `nyc_hvfhs_monthly`:

```bash
docker exec nyc-hvfhs-airflow airflow dags trigger nyc_hvfhs_monthly \
  --conf '{"year": 2024, "month": 1, "force": false}'
docker exec nyc-hvfhs-airflow airflow dags list-runs --dag-id nyc_hvfhs_monthly
```

Verify each task in order:

1. `prepare_month`: exact landed URI, size, checksum, and stable run ID.
2. `bronze_ingestion`: S3 metadata identity check and Bronze publication
   without record loss.
3. `great_expectations_checkpoint`: required columns and non-empty month pass.
4. `silver_transform`: valid rows plus reason-coded quarantine.
5. `dbt_build`: six Gold models, with explicit month variables.
6. `reconciliation`: Bronze = Silver + quarantine and fact = Silver.
7. `publication_manifest`: deterministic manifest URI and `published` status.
8. `athena_smoke`: expected Gold tables/columns, non-empty filtered result,
   and bytes scanned below the configured cutoff.

Stop at the first failure. Preserve the run ID, Glue logs, manifest state, and
Athena query ID before retrying. A rerun of the same identity should reuse the
stable run and safely replace only the requested month. A changed URI,
checksum, or size must be rejected; `force` does not authorize source change.

## 7. Capture evidence and control cost

Copy `docs/CLOUD_EVIDENCE_TEMPLATE.md` to a dated Markdown record under
`docs/evidence/final-e2e/`. Record counts, validation result, Gold tests,
publication object, query bytes, retry evidence, and independent business totals.
Use only one month, stop idle Glue/Airflow compute, keep Athena filters, and
review billing alerts before attempting the four-month backfill.

## 8. Stop the runner and teardown

Stop the standalone runner after evidence capture:

```bash
docker stop nyc-hvfhs-airflow
docker rm nyc-hvfhs-airflow
docker volume rm nyc-hvfhs-airflow-home
```

Follow [`TEARDOWN_RUNBOOK.md`](TEARDOWN_RUNBOOK.md). Canonical S3 data and Glue
databases are protected; never bypass that protection merely to make destroy
complete.
