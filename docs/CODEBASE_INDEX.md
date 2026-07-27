# Codebase index

| Area | Active implementation |
| --- | --- |
| Bronze/Silver | `etl/spark_jobs/nyc_bronze_ingestion.py`, `etl/spark_jobs/nyc_silver_transform.py` |
| Quality | Bronze source contract, Silver reason priority/quarantine, dbt tests, reconciliation |
| Gold | `etl/dbt_project/` via Cosmos/dbt-redshift |
| Reconciliation | `etl/orchestration/nyc_hvfhs_reconciliation.py` |
| Publication | `etl/orchestration/nyc_hvfhs_publication.py`, `etl/publication/nyc_hvfhs.py` |
| Verification | `etl/orchestration/nyc_hvfhs_verification.py` |
| Catalog | Glue Data Catalog for open Iceberg layers; Redshift-managed Gold |
