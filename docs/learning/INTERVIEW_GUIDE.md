# Interview guide

These questions are grounded in files, functions, tables, and resources that exist in this repository.

## Repository-grounded questions and answers

1. What business problem does the repository solve?

   It builds a monthly batch lakehouse for official NYC TLC HVFHV trips so analysts can query validated ride-hailing facts and marts through bounded Athena SQL. The active architecture is documented in [docs/PROJECT2_BLUEPRINT_FINAL.md](../PROJECT2_BLUEPRINT_FINAL.md).

2. What is the approved source dataset?

   Official NYC TLC monthly HVFHV Parquet files named `fhvhv_tripdata_YYYY-MM.parquet` plus `taxi_zone_lookup.csv`. The source contract is in [etl/sources/nyc_hvfhs.py](../../etl/sources/nyc_hvfhs.py).

3. Which function builds the official monthly filename?

   `monthly_trip_filename(year, month)` in [etl/sources/nyc_hvfhs.py](../../etl/sources/nyc_hvfhs.py).

4. Which function builds the official monthly source URI?

   `monthly_trip_uri(year, month)` in [etl/sources/nyc_hvfhs.py](../../etl/sources/nyc_hvfhs.py), using `NYC_TLC_TRIP_DATA_BASE_URI`.

5. What is the source grain?

   One source-level HVFHV trip record from one monthly Parquet file. Bronze preserves that record and adds only ingestion metadata.

6. How is `trip_id` created?

   `canonical_trip_id(record)` hashes source-level `TRIP_IDENTITY_COLUMNS` in [etl/sources/nyc_hvfhs.py](../../etl/sources/nyc_hvfhs.py). The Glue equivalent is `_fingerprint()` in [etl/glue_jobs/nyc_silver_transform.py](../../etl/glue_jobs/nyc_silver_transform.py).

7. Why should `trip_id` not include `_ingested_at`?

   `_ingested_at` changes between runs. Including it would make the same source trip produce different IDs and break retry-safe deduplication.

8. What belongs in Bronze?

   Source-shaped trip records and Taxi Zone records plus `_source_file`, `_source_year`, `_source_month`, `_source_checksum`, `_ingestion_run_id`, and `_ingested_at`. The pure function is `bronze_records` in [etl/transforms/nyc_hvfhs.py](../../etl/transforms/nyc_hvfhs.py).

9. What belongs in Silver?

   Validated, deduplicated trip fields needed by the project, plus derived `trip_duration_minutes`, `pickup_date`, and `pickup_hour`; invalid rows go to quarantine with `reason_code`.

10. What are the active upstream Iceberg tables?

   `bronze.bronze_hvfhs_trips`, `bronze.bronze_taxi_zones`, `silver.silver_trips`, `silver.quarantine_trips`, and `ops.source_run_manifest`, defined in [etl/iceberg/catalog.py](../../etl/iceberg/catalog.py).

11. What is the purpose of `ops.source_run_manifest`?

   It records immutable source identity, run ID, run status, row counts, validation fields, and failure details. The pure state machine is `SourceRunManifest` in [etl/manifests/nyc_hvfhs.py](../../etl/manifests/nyc_hvfhs.py).

12. Which statuses can a source run manifest use?

   `discovered`, `bronze_published`, `ge_passed`, `ge_blocked`, `silver_published`, and `failed`, defined by `RunStatus`.

13. What blocks a changed source checksum?

   `manifest_decision` in [etl/sources/nyc_hvfhs.py](../../etl/sources/nyc_hvfhs.py), `retry_is_safe` in [etl/manifests/nyc_hvfhs.py](../../etl/manifests/nyc_hvfhs.py), and `_may_process` in [etl/glue_jobs/nyc_bronze_ingestion.py](../../etl/glue_jobs/nyc_bronze_ingestion.py).

14. Where is the mandatory Great Expectations gate?

   [etl/glue_jobs/nyc_great_expectations_checkpoint.py](../../etl/glue_jobs/nyc_great_expectations_checkpoint.py) for Glue execution and [etl/quality/nyc_hvfhs_ge.py](../../etl/quality/nyc_hvfhs_ge.py) for the fixture-tested local contract.

15. What does `ge_blocked` mean?

   The Great Expectations checkpoint failed, the manifest recorded a blocking status, and Silver publication must not run.

16. Why does Great Expectations not replace quarantine?

   Great Expectations blocks unsafe promotion at the boundary. Quarantine preserves row-level invalid records with deterministic `reason_code` during Silver transformation.

17. What quarantine reason codes exist in the pure transform?

   Examples include `MISSING_OR_INVALID_TIMESTAMP`, `DROPOFF_BEFORE_PICKUP`, `INVALID_ZONE_ID`, `UNKNOWN_PICKUP_ZONE`, `UNKNOWN_DROPOFF_ZONE`, `NEGATIVE_TRIP_MILES`, `NEGATIVE_TRIP_TIME`, `NEGATIVE_PASSENGER_FARE`, `NEGATIVE_DRIVER_PAY`, `MISSING_TRIP_IDENTITY_FIELD`, and `DUPLICATE_TRIP_ID`.

18. What is the fact grain?

   `fct_trips` has one row per validated, deduplicated Silver `trip_id`. The dbt model is [etl/dbt_project/models_nyc/facts/fct_trips.sql](../../etl/dbt_project/models_nyc/facts/fct_trips.sql).

19. What are the six Gold models?

   `dim_operator`, `dim_zone`, `dim_date`, `fct_trips`, `mart_hourly_zone_demand`, and `mart_operator_metrics`.

20. What does `mart_hourly_zone_demand` represent?

   One row per pickup date, pickup hour, and pickup zone. Its dbt model is [etl/dbt_project/models_nyc/marts/mart_hourly_zone_demand.sql](../../etl/dbt_project/models_nyc/marts/mart_hourly_zone_demand.sql).

21. What does `mart_operator_metrics` represent?

   One row per source year, source month, and operator. Its dbt model is [etl/dbt_project/models_nyc/marts/mart_operator_metrics.sql](../../etl/dbt_project/models_nyc/marts/mart_operator_metrics.sql).

22. What is the Airflow monthly DAG ID?

   `nyc_hvfhs_monthly`, defined as `MONTHLY_DAG_ID` in [etl/dags/nyc_hvfhs_monthly_dag.py](../../etl/dags/nyc_hvfhs_monthly_dag.py).

23. What is the four-month backfill DAG ID?

   `nyc_hvfhs_four_month_backfill`, defined as `BACKFILL_DAG_ID` in [etl/dags/nyc_hvfhs_monthly_dag.py](../../etl/dags/nyc_hvfhs_monthly_dag.py).

24. What is the exact Airflow monthly task order?

   `prepare_month -> bronze_ingestion -> great_expectations_checkpoint -> silver_transform -> dbt_build -> quality_checkpoint -> publication_manifest -> athena_smoke`.

25. What does `force` mean?

   `force` allows an intentional retry of the same immutable source identity. It does not allow a changed checksum or URI to replace an existing monthly source.

26. Which component owns post-Gold reconciliation?

   [etl/glue_jobs/nyc_quality_checkpoint.py](../../etl/glue_jobs/nyc_quality_checkpoint.py) in Glue and [etl/quality/nyc_hvfhs_checkpoint.py](../../etl/quality/nyc_hvfhs_checkpoint.py) for local fixture contracts.

27. What is the publication manifest for?

   It records validated serving publication state after dbt and reconciliation. The writer is [scripts/publish_publication_manifest.py](../../scripts/publish_publication_manifest.py).

28. Why is Athena read-only?

   Iceberg on S3 and Glue Catalog are canonical. Athena is only the bounded analytical serving layer, implemented by [athena/query_runner.py](../../athena/query_runner.py), [athena/verify_gold.py](../../athena/verify_gold.py), and four SQL files under [athena/sql](../../athena/sql).

29. What are the four Athena SQL artifacts?

   `gold_smoke.sql`, `mart_hourly_zone_demand.sql`, `iceberg_history.sql`, and `time_travel.sql.tmpl`.

30. Which Terraform resource defines the Athena workgroup?

   `aws_athena_workgroup.gold_query` in [terraform/athena.tf](../../terraform/athena.tf).

31. Which Terraform resource defines the canonical S3 bucket?

   `aws_s3_bucket.lakehouse` in [terraform/s3.tf](../../terraform/s3.tf).

32. Which Terraform resource defines Glue Catalog databases?

   `aws_glue_catalog_database.namespace` in [terraform/glue_catalog.tf](../../terraform/glue_catalog.tf).

33. Which Terraform resources define Glue jobs?

   `aws_glue_job.initialize`, `aws_glue_job.bronze`, `aws_glue_job.silver`, `aws_glue_job.quality`, `aws_glue_job.great_expectations`, and `aws_glue_job.schema_evolution` in [terraform/glue_jobs.tf](../../terraform/glue_jobs.tf).

34. Which IAM role runs Glue?

   `aws_iam_role.glue_service` in [terraform/iam.tf](../../terraform/iam.tf).

35. How does the optional Airflow runner authenticate?

   Through `aws_iam_instance_profile.airflow_runner` and the AWS SDK default credential chain. It must not use committed static AWS keys.

36. What local command verifies the Python unit/contract suite?

   `.\.venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\unit tests\contract -q`.

37. What local command validates the Glue package contract?

   `.\.venv\Scripts\python.exe scripts\package_glue_jobs.py --output build\nyc_glue_jobs.zip --check`.

38. What local dbt command is expected to run without cloud credentials?

   From `etl\dbt_project`, run `..\..\.venv\Scripts\dbt.exe parse --profiles-dir . --target local_parse --no-partial-parse` after setting parse-only environment variables.

39. What must remain NOT VERIFIED until real AWS evidence exists?

   AWS credentials, S3/Glue/Iceberg writes, Glue jobs, Great Expectations on Glue, Airflow service scheduling, dbt-glue execution, Athena query results, Terraform apply/destroy, teardown, costs, performance, and recovery behavior.

40. What legacy area is not active?

   [legacy](../../legacy) is archived migration history and must not be imported or deployed as the NYC architecture.

## Strong design answers to practice

### Bronze versus Silver

Bronze is for source preservation and ingestion metadata. Silver is for project-level validity, deduplication, field selection, derived columns, and quarantine. This separation keeps raw evidence available while making analytical tables clean and explainable.

### Great Expectations versus quarantine

Great Expectations is a blocking boundary check. It answers: should this Bronze month be allowed to promote? Quarantine is a row-level evidence table. It answers: which records failed deterministic Silver rules and why?

### Manifest versus publication manifest

`ops.source_run_manifest` is operational run state for ingestion and promotion. The publication manifest is serving state after Gold is built and reconciled. They are related, but they are not the same object.

### Athena versus Iceberg/Glue Catalog

Iceberg on S3 owns table data and metadata files. Glue Catalog owns table discovery. Athena reads cataloged Gold tables through a bounded query pack. Athena should not become the write path or a generic serving API.

### Retry versus rerun versus backfill

A retry repeats a failed task for the same source identity. A rerun repeats a monthly run and must respect manifest idempotency. A backfill sequences multiple month-scoped runs, implemented by `nyc_hvfhs_four_month_backfill`.
