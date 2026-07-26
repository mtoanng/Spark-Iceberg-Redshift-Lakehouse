# dbt Cloud + Databricks exercises

Set the dbt Cloud project subdirectory to `learning_playground/dbt`. The project reads the two persistent Bronze tables created in Databricks from the uploaded Volume fixtures.

Create a Databricks connection in dbt Cloud with your own values:

| dbt Cloud field | Value |
| --- | --- |
| Host | Your Databricks workspace host, without a token in Git |
| HTTP Path | Your SQL Warehouse HTTP path |
| Catalog | Your learner Unity Catalog, for example `workspace` |
| Schema | `lakehouse_learning` (or a schema you can create) |
| Authentication | dbt Cloud's configured Databricks credential/auth flow |

Do not commit a token, host-private URL, or `profiles.yml`. dbt Cloud stores connection credentials outside this repository.

Run exactly after the Bronze tables exist:

```text
dbt build
dbt test
```

The graph is `bronze_nyc_trips source -> stg_nyc_trips -> int_valid_trips -> int_deduplicated_trips -> fct_trips -> mart_hourly_zone_demand`; `bronze_taxi_zones` flows through `stg_taxi_zones` into validation and the fact. The six active models are small, runnable reference answers with TODO comments. Matching answer copies are under `solutions/` for comparison.

Fundamentals covered: `source()`, `ref()`, staging, modular SQL, generic `not_null`/`unique` tests, the singular reconciliation test, descriptions in `schema.yml`, and `dbt build`. No packages, macros, incremental models, snapshots, semantic layer, or multiple environments are included.
