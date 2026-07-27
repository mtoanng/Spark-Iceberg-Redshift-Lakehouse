# Component layer map

| Component | Owner | Input | Output | Failure boundary |
| --- | --- | --- | --- | --- |
| Landing/Bronze | EMR Serverless PySpark | immutable S3 objects | source-faithful Bronze Iceberg + run metadata | source/hash/size/schema/empty input |
| Silver/quarantine | EMR Serverless PySpark | Bronze and Taxi Zones | canonical Silver or reason-coded quarantine Iceberg | row validation, exact deduplication, partition write |
| Gold | Cosmos Watcher + dbt-redshift | Redshift external Bronze/Silver schemas | six managed Redshift Gold relations + archived `run_results.json` | model or dbt-test failure |
| Reconciliation | Airflow Python | Athena open-layer counts + Redshift Data API fact count | structured invariant result | Bronze/Silver/quarantine or Silver/Gold mismatch |
| Publication | Airflow Python | audit, reconciliation, archived dbt artifact, Athena snapshots | deterministic S3 JSON | conflicting content or missing dbt evidence |
| Verification | Airflow Python | Athena open layers + Redshift Data API Gold | read-only execution IDs/count proof | missing relation, unreadable partition, count mismatch |

Glue Data Catalog is shared metadata for Bronze, Silver, quarantine, and
operational Iceberg state. Redshift owns Gold. The only two monthly EMR
processing submissions are Bronze and Silver; reconciliation and publication
never submit Spark jobs.
