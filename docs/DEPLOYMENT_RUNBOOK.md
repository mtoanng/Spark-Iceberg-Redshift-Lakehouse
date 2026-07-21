# Project 2 deployment and teardown sequence

This is a complete, unexecuted command sequence for an approved disposable
AWS environment. It uses the EC2 instance profile/default SDK credential chain.
It contains no access-key setup and no command in this document performs an
apply automatically.

## Build and validate locally

```powershell
python scripts/package_glue_jobs.py --output build/nyc_glue_jobs.zip --check
python scripts/run_e2e.py --year 2024 --month 1 --smoke
terraform -chdir=terraform fmt -check -recursive .
terraform -chdir=terraform init -backend=false
terraform -chdir=terraform validate
terraform -chdir=terraform plan -out phase-c.tfplan
```

Review the plan. If an approved temporary runner is required, provide only
`airflow_runner_ami_id` and `airflow_runner_subnet_id` in a private tfvars
file. Leave them empty for a plan-only run.

## Approved remote sequence (not executed here)

1. Apply the reviewed Terraform plan.
2. Upload the four selected official monthly Parquet files and Taxi Zone
   lookup to the landing/reference prefixes using the default AWS credential
   chain.
3. Run the initializer, then Bronze → Great Expectations → Silver for each
   month. The Airflow four-month DAG provides the ordered trigger path.
4. Run `dbt deps`, `dbt parse`, and `dbt build --target glue` on the runner.
5. Publish the validated manifest and reconcile Bronze = Silver + quarantine
   and Gold = valid Silver.
6. Run the Athena smoke verifier only when `ATHENA_SMOKE_ENABLED=true`; its
   command is intentionally deferred by default.
7. Retain logs and counts as evidence; do not call them verified until output
   is independently reviewed.

## Teardown and verification

```powershell
scripts/teardown.ps1
python scripts/verify_teardown.py --bucket <bucket> --workgroup <workgroup>
```

The teardown script creates a destroy plan only. A separately approved
operator may apply it after confirming canonical data retention and an empty
bucket. `verify_teardown.py` is read-only and reports bucket/workgroup state;
it never deletes data or infrastructure.
