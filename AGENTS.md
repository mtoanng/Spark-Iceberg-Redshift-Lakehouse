# AGENTS.md — NYC HVFHV snapshot-governed lakehouse

## Source of truth

Read `docs/PROJECT2_BLUEPRINT_FINAL.md` before editing. Active architecture:

```text
immutable NYC TLC HVFHV month + Taxi Zones
-> S3 landing -> Airflow 3 -> Glue 4.0/PySpark Bronze
-> structural Great Expectations gate
-> Glue 4.0/PySpark Silver + quarantine
-> dbt-glue Gold -> reconciliation -> snapshot-aware publication
-> bounded read-only Athena
```

The current temporary EC2/instance-profile Airflow runner remains the deployment
model for the first baseline. Do not add Cosmos, MWAA, EKS, Glue 5.1, Lake
Formation, ML/AI, dashboards, another query engine, or an Iceberg maintenance
suite.

## Locked semantics

- Landing identity is URI + SHA-256 + byte size + year + month.
- `row_id` is the exact-row SHA-256 and the only canonical dedup/merge key.
- `business_trip_key` is a probable-trip analytical key and never silently
  removes or quarantines rows.
- `identity_policy_version` and ordered fields come only from
  `etl/contracts/nyc_hvfhs_identity.py`.
- Bronze is source-faithful plus ingestion and identity metadata.
- Great Expectations blocks only on required year-specific columns, non-empty
  month, and identity-policy inputs.
- Silver owns row-level validation, exact deduplication, reason priority, and
  quarantine.
- Every Bronze row is classified exactly once:
  `bronze_count = silver_count + quarantine_count`.
- Gold remains exactly three dimensions, one fact, and two marts.
- Publication writes a durable JSON artifact with counts, locations, and
  snapshots before marking the run published.
- Athena remains bounded and read-only.

## Delivery and safety

Prove one immutable 2024 month before the existing four-month sequence. The
only post-baseline Iceberg semantic is adding nullable
`cbd_congestion_fee` for 2025, ingesting a new snapshot, and querying the old
snapshot with Athena version travel.

Do not commit source data, credentials, private URLs, account IDs, `.env`,
Terraform state, or saved plans. Do not run `terraform apply` or incur AWS cost
without separate approval. Preserve unrelated user changes.

Run all credential-independent tests, Python formatting/lint/compile, dbt at
the highest supported level, DAG topology checks, Terraform fmt/init/validate,
packaging, secret scan, and documentation links. Mark live Glue, Airflow, S3,
Iceberg snapshot, Athena, retry/clear/rerun, schema-evolution, and teardown
evidence `NOT VERIFIED` until a retained AWS run exists.

End work with baseline failures, decisions, files, cleanup, exact commands,
PASS/FAIL/NOT VERIFIED criteria, identity/rerun/publication evidence, remaining
risks, verification boundary, the learning task, and three teach-back
questions.
