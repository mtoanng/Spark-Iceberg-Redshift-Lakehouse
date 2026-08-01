locals {
  spark_package_file = abspath("${path.module}/../${var.spark_package_path}")
}

resource "aws_s3_object" "spark_package" {
  bucket = aws_s3_bucket.lakehouse.id
  key    = var.spark_package_s3_key
  source = local.spark_package_file
  etag   = filemd5(local.spark_package_file)
}

resource "aws_s3_object" "spark_script" {
  for_each = toset([
    "apply_nyc_2025_schema_evolution.py",
    "nyc_bronze_ingestion.py",
    "nyc_silver_transform.py",
    "verify_nyc_snapshot.py",
  ])

  bucket = aws_s3_bucket.lakehouse.id
  key    = "spark_jobs/${each.value}"
  source = "${path.module}/../etl/spark_jobs/${each.value}"
  etag   = filemd5("${path.module}/../etl/spark_jobs/${each.value}")
}

resource "aws_emrserverless_application" "spark" {
  name          = "${var.project_name}-${var.environment}-spark"
  release_label = "emr-6.15.0"
  type          = "SPARK"

  auto_start_configuration { enabled = true }
  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = var.emr_serverless_idle_timeout_minutes
  }

  maximum_capacity {
    cpu    = "4 vCPU"
    memory = "16 GB"
    disk   = "80 GB"
  }
}
