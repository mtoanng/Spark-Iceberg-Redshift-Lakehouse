# Code-module layer reference

- `etl/spark_jobs/nyc_bronze_ingestion.py`: validates immutable S3 source
  evidence, schema, and non-empty input; writes Bronze and source metadata.
- `etl/spark_jobs/nyc_silver_transform.py`: applies ordered validation reasons,
  exact `row_id` deduplication, and writes Silver plus quarantine.
- `etl/dags/nyc_hvfhs_monthly_dag.py`: defines the exact eight-step monthly
  DAG, Cosmos Watcher configuration, and default all-success blocking.
- `etl/orchestration/nyc_hvfhs_reconciliation.py`: queries three open layers
  through Athena and `gold.fct_trips` through Redshift Data API.
- `etl/orchestration/nyc_hvfhs_publication.py`: validates archived dbt results,
  reads open-layer snapshot IDs, and writes/reuses a deterministic publication
  object.
- `etl/orchestration/nyc_hvfhs_verification.py`: verifies open Iceberg layers
  with Athena and all six Gold relations/marts with Redshift Data API.
- `etl/orchestration/nyc_hvfhs_cosmos.py`: archives and requires durable
  `run_results.json` before reconciliation.
- `etl/dbt_project/`: exactly three dimensions, one fact, and two marts;
  `fct_trips` merges by `row_id`.
- `terraform/emr_serverless.tf`: uploads only initializer, Bronze, and Silver
  scripts to one persistent application.
- `terraform/redshift_serverless.tf`: creates the Redshift Serverless
  namespace/workgroup, external open-layer schemas, and local `gold` schema.
