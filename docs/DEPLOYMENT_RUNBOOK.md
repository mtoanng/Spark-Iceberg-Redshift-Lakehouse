# One-month controlled AWS deployment runbook

Status: **requires AWS execution verification**. Configure the default AWS
credential chain and keep all credentials out of repository files.

1. Build the shared Spark package and run Terraform format/validation. Apply is
   separately approved.
2. Configure Airflow with immutable source facts, one EMR Serverless application
   and role, Spark script/package locations, Athena workgroup, publication
   prefix, Redshift database, and Redshift workgroup name.
3. Initialize the open Iceberg tables once, then stage one immutable source
   month.
4. Trigger `nyc_hvfhs_monthly`. The required order is `prepare_month`, Bronze
   EMR, Silver EMR, Cosmos dbt build, durable dbt artifact, reconciliation,
   publication, and verification.
5. Preserve the immutable run ID, all Athena query IDs, Redshift Data API
   statement IDs, dbt artifact URI/SHA-256, open-layer snapshot IDs, and the
   publication object before retrying.

Do not claim a successful deployment until the two invariants, publication
object, and final verification are retained for a bounded live month.
