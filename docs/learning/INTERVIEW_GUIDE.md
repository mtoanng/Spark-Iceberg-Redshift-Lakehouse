# Interview and teach-back guide

Explain this system as two storage domains joined by explicit checks: Bronze,
Silver, and quarantine are source-governed Iceberg tables; Gold is a
Redshift-managed analytical graph. Cosmos runs dbt-redshift, Athena counts only
open Iceberg layers, and Redshift Data API counts/verifies Gold.

`row_id` is the versioned exact-row identity and the only merge/dedup key.
`business_trip_key` is analytical evidence only. Bronze validates input,
Silver owns row decisions, dbt owns Gold tests, and reconciliation owns
cross-layer counts.

Questions to answer:

1. Why does Athena not query Gold after Gold moved to Redshift?
2. Which failure prevents publication when a dbt model or test fails?
3. How does a repeated publication avoid silently replacing different evidence?
