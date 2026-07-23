# Interview guide

## Short project explanation

This is a bounded monthly NYC TLC HVFHV lakehouse. Official Parquet lands
immutably in S3. Glue writes source-faithful Bronze Iceberg; a small Great
Expectations suite blocks structurally unsafe promotion; Glue separates valid
Silver rows from reason-coded quarantine; dbt builds six Gold tables; a
reconciliation job and publication manifest make serving state explicit; and
Athena provides read-only, scan-bounded analysis. Airflow orchestrates and
Terraform defines the AWS boundary.

## Decisions to explain

1. Source identity includes URI, SHA-256, byte size, year, and month so a retry
   cannot silently replace history.
2. Bronze preserves source grain; Silver owns row-level business validity.
3. Great Expectations is deliberately structural, avoiding duplicate business
   rules and keeping invalid rows visible in quarantine.
4. `trip_id` is deterministic and is the logical key for the month-aware
   `fct_trips` merge.
5. Reconciliation precedes publication; Athena only reads published Gold.
6. Small dimensions and marts rebuild because four months is bounded and
   clarity matters more than a generic incremental framework.

## Honest limitation

Credential-independent contracts are local; Glue, Airflow, dbt-glue, Iceberg,
Athena, retry/recovery, cost, and teardown behavior require the first controlled
AWS execution and must not be described as verified yet.
