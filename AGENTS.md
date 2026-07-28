# AGENTS.md — NYC HVFHV core industrial lakehouse

## Source of truth

Read `docs/PROJECT2_BLUEPRINT_FINAL.md` before editing. Active architecture:

```text
immutable NYC TLC HVFHV month + immutable Taxi Zones in S3 landing
-> regular MWAA (Airflow 3 control plane)
-> EMR Serverless/PySpark Bronze
-> EMR Serverless/PySpark Silver + quarantine
-> S3 Iceberg + Glue Data Catalog
-> Redshift Serverless external schemas
-> one dbt-redshift build producing managed Gold
-> Athena/Redshift reconciliation
-> deterministic publication
-> bounded read-after-publish verification
```

This is a production-shaped, cost-bounded reference architecture. S3 landing is
an input contract: do not add source downloading or upload orchestration. Keep
one orchestrator and one implementation path. Do not add Cosmos, an EC2 Airflow
runner, MWAA Serverless, EKS, Glue ETL jobs, Lake Formation, ML/AI, dashboards,
another query engine, or a general Iceberg maintenance suite.

## Locked semantics

- Landing identity is URI + SHA-256 metadata + byte size + year + month.
- `row_id` is the exact-row SHA-256 and the only canonical dedup/merge key.
- `business_trip_key` is a probable-trip analytical key and never silently
  removes or quarantines rows.
- `identity_policy_version` and ordered fields come only from
  `etl/contracts/nyc_hvfhs_identity.py`.
- Bronze is source-faithful plus ingestion and identity metadata.
- Bronze owns source existence, checksum, size, schema, and non-empty input.
- Silver owns row-level validation, exact deduplication, reason priority, and
  quarantine.
- Every Bronze row is classified exactly once:
  `bronze_count = silver_count + quarantine_count`.
- Gold remains exactly three dimensions, one fact, and two marts.
- The operational manifest is the single monthly source/run state boundary.
- Publication writes one durable JSON artifact with reconciled open-layer
  counts/snapshots, Redshift Gold relation names, and dbt evidence.
- Verification is deliberately bounded: publication integrity plus one Silver
  and one Gold consumer count. Athena remains read-only.
- Rerunning the same immutable source is idempotent. Changed URI, checksum, or
  byte size for an existing month is rejected; there is no `force` bypass.

## Delivery and safety

Prove one immutable 2024 month before the four-month sequence. The only
post-baseline Iceberg semantic is adding nullable `cbd_congestion_fee` for 2025,
ingesting a new snapshot, and querying the old snapshot with Athena version
travel.

Do not commit source data, credentials, private URLs, account IDs, `.env`,
Terraform state, or saved plans. Do not run `terraform apply` or incur AWS cost
without separate approval. Preserve unrelated user changes.

Run all credential-independent tests, Python formatting/lint/compile, dbt at
the highest supported offline level, DAG topology checks, Terraform
fmt/init/validate, packaging, secret scan, and documentation links. Mark live
MWAA, S3, EMR Serverless, Redshift Serverless, Iceberg snapshot, Athena,
retry/clear/rerun, schema-evolution, and teardown evidence `NOT VERIFIED` until
a retained AWS run exists.

End work with baseline failures, decisions, files, cleanup, exact commands,
PASS/FAIL/NOT VERIFIED criteria, identity/rerun/publication evidence, remaining
risks, verification boundary, the learning task, and three teach-back
questions.
