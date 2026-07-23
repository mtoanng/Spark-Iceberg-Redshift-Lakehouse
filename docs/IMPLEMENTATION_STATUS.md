# Implementation status

## Implemented and locally verified

- official monthly filename/URI and landed-S3 identity contracts;
- SHA-256, byte size, year/month, stable run ID, and changed-source rejection;
- source-faithful Bronze metadata and retry-safe monthly partition replacement;
- structural Great Expectations suite construction and blocking state;
- deterministic Silver validation, duplicate handling, reason codes, and
  Bronze = Silver + quarantine reconciliation;
- manifest lifecycle through Bronze, GE, Silver, Gold reconciliation, and
  publication;
- exact six-model dbt graph and month-scoped incremental fact configuration;
- monthly/four-month Airflow topology and parameter contracts;
- Athena runner, catalog/table/column verification, non-empty fact smoke,
  bound parameters, and scan bound;
- deterministic Glue package and repository hygiene checks.

## Implemented and statically verified

- private/versioned/encrypted S3 bucket with public access blocked;
- Glue Catalog namespaces, Glue jobs, Iceberg v2/Snappy DDL, and shared package;
- least-scoped Airflow/dbt/Athena permissions for the bounded workflow;
- Athena workgroup output encryption and bytes-scanned cutoff;
- optional IMDSv2/SSM Airflow runner;
- guarded source upload, deployment, evidence, and retained-data teardown paths;
- credential-independent GitHub Actions workflow.

## Requires AWS execution verification

- Terraform plan/apply against the selected account;
- S3 upload identity and object metadata;
- Glue/Iceberg physical reads, writes, partition replacement, and retries;
- Great Expectations running inside Glue;
- dbt-glue Iceberg merge behavior and all dbt tests;
- Airflow instance-profile authentication and task failure propagation;
- publication snapshot metadata availability;
- Athena results, bytes scanned, and result locations;
- controlled retry/clear/rerun, four-month sequence, cost, and teardown.

## Not implemented

- 2025 schema evolution;
- automated snapshot expiration, orphan deletion, or compaction;
- full-year backfill, dashboards, ML, recommendations, or alternate engines.

The codebase is ready for one controlled deployment review. It is not
production-ready and has not been deployed as verified evidence.
