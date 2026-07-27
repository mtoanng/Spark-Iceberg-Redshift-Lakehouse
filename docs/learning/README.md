# NYC HVFHV lakehouse learning guide

This guide is organized in three layers.

1. General architecture: immutable source identity flows through S3 landing,
   two EMR Serverless PySpark jobs, Redshift external schemas, Cosmos/dbt-redshift,
   reconciliation, publication, and verification.
2. Component layer: [COMPONENT_MAP.md](COMPONENT_MAP.md) describes ownership and
   data/control handoffs.
3. Code-module layer: [CODE_MODULE_REFERENCE.md](CODE_MODULE_REFERENCE.md)
   maps the active entrypoints and adapters.

Quality ownership is intentionally narrow: Bronze validates source existence,
checksum, size, schema, and non-empty input; Silver validates rows, assigns
deterministic quarantine reasons, and exact-deduplicates by `row_id`; dbt tests
Gold; reconciliation validates `Bronze = Silver + quarantine` and
`Silver = Gold fct_trips`.

The final monthly chain is:

```text
prepare_month
-> bronze_ingestion_emr
-> silver_transform_emr
-> Cosmos dbt_build -> dbt_result_artifact
-> reconciliation -> publication_manifest -> verification
```

Athena reads only open Iceberg layers. Redshift Data API reads only Redshift
Gold. The publication JSON contains immutable source identity, open-layer
identifiers and available snapshots, Redshift relation names, counts,
reconciliation evidence, and archived dbt artifact evidence. Gold relation
metadata is database/schema/name only.

Read [DEPLOYMENT_WALKTHROUGH.md](DEPLOYMENT_WALKTHROUGH.md) after these layers
for the execution path, then use [INTERVIEW_GUIDE.md](INTERVIEW_GUIDE.md) to
teach it back. Live AWS behavior remains **NOT VERIFIED** until a retained,
bounded execution exists.
