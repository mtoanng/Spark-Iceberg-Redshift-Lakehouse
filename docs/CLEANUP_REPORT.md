# Closure Phase C cleanup report

Date: 2026-07-21

## Removed

- Tracked `terraform/tfplan`, the obsolete saved Instacart/ML plan artifact.

## Kept

- Ignored Terraform state, backup state, private tfvars, provider cache, and
  all credentials. These require operator/state review and remain protected by
  ignore rules.
- Valid NYC Terraform, S3 baseline, Glue jobs, Athena resources, Airflow image,
  source fixtures, and historical phase reports.
- Legacy directories and unrelated old scripts; no additional deletion was
  authorized in this phase.

## Added or replaced

- Four logical Glue Catalog namespaces and deterministic shared Glue package
  contract.
- Optional instance-profile Airflow runner with IMDSv2 enforcement.
- Airflow image/environment contract, publication and Athena hooks.
- Smoke, four-month release, reconciliation, package, and guarded teardown
  scripts.

## Reason

The active Terraform source now describes only NYC TLC, Glue/Iceberg, Airflow,
Athena, and protected S3 resources. Obsolete tracked plan material was removed
only after the replacement code and static checks were present.
