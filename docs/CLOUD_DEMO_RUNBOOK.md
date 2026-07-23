# Controlled cloud demo

This page is an index, not an alternate deployment path. The cloud service
path is **requires AWS execution verification** and must remain bounded.

1. Complete the [one-month runbook](DEPLOYMENT_RUNBOOK.md).
2. Capture and independently review [cloud evidence](CLOUD_EVIDENCE_TEMPLATE.md).
3. Only then run the [four-month sequential backfill](FOUR_MONTH_BACKFILL_RUNBOOK.md).
4. Finish with the [protected teardown](TEARDOWN_RUNBOOK.md).

No step authorizes committed credentials, source data, Terraform state, an
unreviewed apply, or deletion of canonical S3/Iceberg data.
