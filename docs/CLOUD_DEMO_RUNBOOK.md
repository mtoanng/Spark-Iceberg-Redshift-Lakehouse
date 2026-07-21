# Bounded cloud demo and teardown runbook

This runbook is intentionally manual. It is a checklist for an approved,
disposable AWS account; it is not evidence that the repository has been
deployed. Do not execute `terraform apply`, Glue jobs, or destructive cleanup
without explicit approval and a budget limit.

## Before provisioning

1. Use a disposable AWS account or a dedicated environment and set a budget
   alert. Do not put credentials, account IDs, or Terraform state in Git.
2. Measure SHA-256 and byte size for each selected monthly Parquet file and the
   Taxi Zone lookup. Record them in the evidence template.
3. Review `terraform plan` and the expected resource list: one private S3
   bucket, versioning/encryption/public-access block, four Glue Catalog
   namespaces, packaged Glue jobs, one Athena workgroup/policy, and the
   optional IMDSv2 Airflow runner/profile.

## Provision and run

```powershell
cd terraform
terraform init
terraform fmt -check -recursive .
terraform validate
terraform plan -out phase6.tfplan
# Stop for explicit approval before this command:
terraform apply phase6.tfplan
```

Upload only the selected source and reference files to the output prefixes.
Use the Terraform outputs for bucket, warehouse, database, role, and job names.
Then follow the Phase 5 order in the root README:

1. initialize Iceberg tables;
2. run Bronze for one month with its measured checksum and run ID;
3. run Silver validation/quarantine;
4. run remote `dbt build` and tests;
5. run the quality checkpoint;
6. run the mandatory Great Expectations checkpoint before Silver and the
   post-Gold reconciliation after dbt;
7. publish the validated manifest and run the bounded Athena query pack;
8. if the one-month evidence is clean, trigger the four sequential monthly
   runs with Airflow or use `scripts/run_release.ps1` to generate the reviewed
   command plan.

Capture command output and service logs immediately. Replace no `NOT VERIFIED`
marker with a claim unless the output is retained and independently reviewed.

## Evidence to capture

Complete `docs/CLOUD_EVIDENCE_TEMPLATE.md` with:

- UTC timestamps and the non-secret environment label;
- Terraform plan/apply/destroy summaries;
- source file names, sizes, and SHA-256 values;
- Glue job run IDs and CloudWatch log references;
- Bronze/Silver/quarantine/Gold counts and reconciliation results;
- dbt build/test output;
- Athena query IDs, outputs, scanned bytes, and selected Gold table locations;
- Airflow DAG run, retry/clear/rerun, and four-month sequence evidence;
- final S3/Glue cleanup state.

## Teardown

Do not delete canonical data implicitly. First export the evidence, confirm the
account is disposable, and decide whether source/Iceberg data must be retained.
With an empty bucket, destroy the infrastructure:

```powershell
cd terraform
terraform plan -destroy -out phase6-destroy.tfplan
# Stop for explicit approval before this command:
terraform apply phase6-destroy.tfplan
```

If the bucket contains data, `force_destroy = false` intentionally prevents an
accidental recursive delete. Emptying it is a separate, explicitly approved
operation and is not automated by this repository.

After teardown, rerun `terraform plan` only if a new environment is needed and
record the absence of the resources. Costs, recovery, and teardown remain
unverified until this bounded run occurs.
