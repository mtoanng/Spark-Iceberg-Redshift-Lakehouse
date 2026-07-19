# NYC HVFHV Gold — dbt

This dbt project reads the validated Iceberg Silver trip table and the
source-faithful Bronze Taxi Zone lookup from AWS Glue Catalog. It materializes
exactly six Iceberg Gold tables:

- `dim_operator`: one row per operator code;
- `dim_zone`: one row per Taxi Zone ID;
- `dim_date`: one row per pickup calendar date;
- `fct_trips`: one row per valid, deduplicated `trip_id`;
- `mart_hourly_zone_demand`: one row per pickup date/hour/zone;
- `mart_operator_metrics`: one row per source year/month/operator.

The production target is `glue`. The `local_parse` target is only for
credential-independent `dbt parse`; it must not compile, run, or publish
canonical data because dbt-spark's session adapter starts local Spark.

```powershell
dbt parse --profiles-dir . --target local_parse --no-partial-parse
```

Cloud execution requires `GLUE_ROLE_ARN` and `S3_GOLD_PATH`, the dbt-glue
adapter, provisioned Iceberg source tables, and approved AWS access. It remains
unverified until a bounded remote run is captured.
