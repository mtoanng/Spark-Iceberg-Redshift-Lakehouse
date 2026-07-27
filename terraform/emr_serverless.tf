locals {
  spark_package_file = abspath("${path.module}/../${var.spark_package_path}")
  spark_submit_parameters = join(" ", [
    "--py-files s3://${aws_s3_bucket.lakehouse.id}/${var.spark_package_s3_key}",
    "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "--conf spark.sql.defaultCatalog=glue_catalog",
    "--conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog",
    "--conf spark.sql.catalog.glue_catalog.warehouse=s3://${aws_s3_bucket.lakehouse.id}/${var.warehouse_prefix}",
    "--conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
    "--conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
  ])
}

resource "aws_s3_object" "spark_package" {
  bucket = aws_s3_bucket.lakehouse.id
  key    = var.spark_package_s3_key
  source = local.spark_package_file
  etag   = filemd5(local.spark_package_file)
}

resource "aws_s3_object" "spark_script" {
  for_each = toset([
    "initialize_nyc_iceberg_tables.py",
    "nyc_bronze_ingestion.py",
    "nyc_great_expectations_checkpoint.py",
    "nyc_silver_transform.py",
    "nyc_quality_checkpoint.py",
    "nyc_publish_manifest.py",
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
    disk   = "20 GB"
  }
}
