# Learning guide

The key design decision is the separation of promotion and row handling:
Great Expectations blocks Silver when a requested Bronze month is empty or
structurally unsafe; Silver applies deterministic business validation and
preserves every invalid record in reason-coded quarantine.

The monthly sequence is:

```text
prepare_month -> bronze_ingestion -> great_expectations_checkpoint
-> silver_transform -> dbt_build -> reconciliation
-> publication_manifest -> athena_smoke
```

`ops.source_run_manifest` carries source URI, SHA-256, size, year/month, stable
run ID, lifecycle state, counts, validation result, publication state, and
failure details. The same identity is retryable; a changed identity for an
existing month is rejected.

Gold contains three dimensions, `fct_trips`, and two marts. Athena is a
read-only bounded consumer, not canonical storage. See the
[`component map`](COMPONENT_MAP.md), [`interview guide`](INTERVIEW_GUIDE.md),
and [`deployment runbook`](../DEPLOYMENT_RUNBOOK.md).

AWS execution, physical Iceberg retry behavior, scheduler recovery, costs, and
teardown remain **NOT VERIFIED**.
