# NYC HVFHV Gold dbt project

This project materializes exactly six Iceberg Gold tables from validated
Silver trips and Bronze Taxi Zones: `dim_date`, `dim_operator`, `dim_zone`,
`fct_trips`, `mart_hourly_zone_demand`, and `mart_operator_metrics`.

`fct_trips` uses a month-filtered Iceberg incremental merge keyed by
`trip_id`. The small dimensions and marts use bounded table rebuilds, which is
deliberately simpler for the four-month portfolio scope.

CI uses the credential-independent `ci` target and a deliberately closed
Thrift endpoint. Parsing and compilation must not contact AWS or start local
Spark:

```powershell
dbt deps --profiles-dir . --target ci
dbt parse --profiles-dir . --target ci --no-partial-parse --no-introspect
dbt compile --profiles-dir . --target ci --no-partial-parse --no-introspect
```

An approved cloud build uses target `glue`, `GLUE_ROLE_ARN`, `S3_GOLD_PATH`,
and explicit month variables:

```powershell
dbt build --profiles-dir . --target glue --vars '{source_year: 2024, source_month: 1}'
```

AWS/dbt-glue execution is **requires AWS execution verification**.
