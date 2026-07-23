# Protected teardown runbook

Status: **requires AWS execution verification**. Teardown needs a separate
operator approval and must preserve canonical storage.

Before planning teardown, export or retain the completed evidence, inspect the
publication manifest, confirm no Glue/Airflow run is active, and list the exact
resources belonging to this project. Never empty or delete the canonical S3
bucket just to satisfy Terraform.

```powershell
powershell -File scripts/teardown.ps1
```

The script creates a bounded destroy plan for non-canonical compute and serving
resources only; it does not apply it. Review the target list and plan. After a
second explicit approval, apply the generated plan using the exact command
printed by the script.

Then run the read-only verification:

```powershell
python scripts/verify_teardown.py --bucket <bucket> --workgroup <workgroup> `
  --region <region> --athena-results-prefix <results-prefix>
```

Expected: the canonical bucket and four Glue databases remain,
temporary/query-result prefixes are empty, and the Athena workgroup, Glue
jobs, optional runner, and removable IAM resources are absent. Any retained
job-script objects are low-cost deployment
artifacts; remove them only through a separately reviewed, explicit object
list. Record all residual resources and costs in the evidence file.
