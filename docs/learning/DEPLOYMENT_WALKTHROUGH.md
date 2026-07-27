# Deployment walkthrough

1. Terraform provisions the protected S3 bucket, Glue Catalog open-layer
   namespaces, one EMR Serverless application, Redshift Serverless, external
   schemas, IAM roles, and bounded Athena workgroup.
2. Upload one immutable month and configure Airflow source variables plus
   Redshift database/workgroup variables.
3. Initialize open Iceberg tables once, then trigger the manual monthly DAG.
4. Bronze and Silver are the two EMR submissions. Cosmos runs dbt-redshift in
   Watcher mode and archives successful `run_results.json`.
5. Airflow reconciliation uses Athena for Bronze/Silver/quarantine and the
   Redshift Data API for Gold. Any mismatch fails before publication.
6. Publication writes one immutable-run JSON object. Verification then proves
   open-layer partition access through Athena and Gold relations through
   Redshift Data API.

No credentials, `terraform apply`, or live queries are part of local
verification. A real AWS run must retain S3, EMR, dbt, Athena, Redshift,
reconciliation, publication, retry, and teardown evidence.
