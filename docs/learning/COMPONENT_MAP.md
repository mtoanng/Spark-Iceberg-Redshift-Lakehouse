# Component map

| Component | Code | Contract |
| --- | --- | --- |
| Source staging | `scripts/fetch_source.py`, `scripts/upload_release_dataset.py` | Official file, local hash/size, immutable S3 landing |
| Source/run identity | `etl/sources/`, `etl/manifests/` | URI + SHA-256 + size + month; stable run ID and status |
| Bronze | `etl/glue_jobs/nyc_bronze_ingestion.py` | One month, source-faithful rows, ingestion metadata |
| GE gate | `etl/glue_jobs/nyc_great_expectations_checkpoint.py` | Required columns and non-empty month; blocks Silver |
| Silver/quarantine | `etl/glue_jobs/nyc_silver_transform.py` | Deterministic trip ID, valid rows, stable reason codes |
| Gold | `etl/dbt_project/` | Six Iceberg models; month-aware fact merge |
| Reconciliation | `etl/glue_jobs/nyc_quality_checkpoint.py` | Bronze = Silver + quarantine; fact = Silver |
| Publication | `etl/glue_jobs/nyc_publish_manifest.py` | Publish only a reconciled, validated month |
| Airflow | `etl/dags/nyc_hvfhs_monthly_dag.py` | Exact monthly order; four sequential child runs |
| Athena | `athena/` | Gold-only read queries, schema/result/scan checks |
| Terraform | `terraform/` | Protected S3, Glue, IAM, Athena, optional runner |

All AWS runtime behavior is **NOT VERIFIED** until retained cloud evidence
exists.
