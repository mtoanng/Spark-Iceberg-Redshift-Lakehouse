# Athena scope

Athena is a bounded, read-only verifier for the open Iceberg layers only:
`bronze.bronze_hvfhs_trips`, `silver.silver_trips`, and
`silver.quarantine_trips`. Every runtime query is scoped to the requested
source year and month.

Gold is Redshift-managed. The Redshift Data API, not Athena, verifies the six
Gold relations, `fct_trips` for the requested month, both marts, and the Gold
count from reconciliation. Glue Data Catalog remains metadata for the open
Iceberg layers and the operational manifest; it is not a Gold serving catalog.
