# NYC HVFHV Gold dbt project

This project materializes exactly six Redshift-managed Gold relations from
validated Silver trips and Bronze Taxi Zones exposed through Redshift external
schemas: `dim_date`, `dim_operator`, `dim_zone`, `fct_trips`,
`mart_hourly_zone_demand`, and `mart_operator_metrics`.

`fct_trips` uses a month-filtered Redshift incremental merge keyed by `row_id`.
`business_trip_key` remains analytical traceability and never drives exact
deduplication. The small dimensions and marts use bounded table rebuilds,
which is deliberately simpler for the four-month portfolio scope.

CI uses the credential-independent `ci` target and disables introspection and
relation-cache population. Parsing and compilation must not contact AWS:

```powershell
dbt deps --profiles-dir . --target ci
dbt parse --profiles-dir . --target ci --no-partial-parse
dbt compile --profiles-dir . --target ci --no-partial-parse --no-introspect --no-populate-cache
```

An approved cloud build uses target `redshift`, the default AWS credential
chain for Redshift Serverless IAM authentication, and explicit month
variables. `REDSHIFT_HOST`, `REDSHIFT_WORKGROUP_NAME`,
`REDSHIFT_DATABASE`, `AWS_REGION`, and `AWS_ACCOUNT_ID` are supplied by the
runtime environment:

```powershell
dbt build --profiles-dir . --target redshift --vars '{source_year: 2024, source_month: 1}'
```

Live Redshift Serverless/Spectrum execution is **NOT VERIFIED**.
