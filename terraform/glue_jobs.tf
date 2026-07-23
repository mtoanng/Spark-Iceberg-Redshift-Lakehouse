locals {
  glue_package_file = abspath("${path.module}/../${var.glue_package_path}")

  glue_common_arguments = {
    "--datalake-formats"                 = "iceberg"
    "--enable-glue-datacatalog"          = "true"
    "--enable-job-insights"              = "true"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-language"                     = "python"
    "--TempDir"                          = "s3://${aws_s3_bucket.lakehouse.id}/tmp/"
    "--conf"                             = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.defaultCatalog=glue_catalog --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://${aws_s3_bucket.lakehouse.id}/${var.warehouse_prefix} --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
    "--extra-py-files"                   = "s3://${aws_s3_bucket.lakehouse.id}/${var.glue_package_s3_key}"
    "--CATALOG_NAME"                     = "glue_catalog"
    "--BRONZE_DATABASE"                  = "bronze"
    "--SILVER_DATABASE"                  = "silver"
    "--OPS_DATABASE"                     = "ops"
    "--GOLD_DATABASE"                    = "gold"
    "--WAREHOUSE_URI"                    = "s3://${aws_s3_bucket.lakehouse.id}/${var.warehouse_prefix}"
  }

}

resource "aws_s3_object" "glue_package" {
  bucket = aws_s3_bucket.lakehouse.id
  key    = var.glue_package_s3_key
  source = local.glue_package_file
  etag   = filemd5(local.glue_package_file)
}

resource "aws_s3_object" "initialize_script" {
  bucket = aws_s3_bucket.lakehouse.id
  key    = "glue_jobs/initialize_nyc_iceberg_tables.py"
  source = "${path.module}/../etl/glue_jobs/initialize_nyc_iceberg_tables.py"
  etag   = filemd5("${path.module}/../etl/glue_jobs/initialize_nyc_iceberg_tables.py")
}

resource "aws_s3_object" "bronze_script" {
  bucket = aws_s3_bucket.lakehouse.id
  key    = "glue_jobs/nyc_bronze_ingestion.py"
  source = "${path.module}/../etl/glue_jobs/nyc_bronze_ingestion.py"
  etag   = filemd5("${path.module}/../etl/glue_jobs/nyc_bronze_ingestion.py")
}

resource "aws_s3_object" "silver_script" {
  bucket = aws_s3_bucket.lakehouse.id
  key    = "glue_jobs/nyc_silver_transform.py"
  source = "${path.module}/../etl/glue_jobs/nyc_silver_transform.py"
  etag   = filemd5("${path.module}/../etl/glue_jobs/nyc_silver_transform.py")
}

resource "aws_s3_object" "quality_script" {
  bucket = aws_s3_bucket.lakehouse.id
  key    = "glue_jobs/nyc_quality_checkpoint.py"
  source = "${path.module}/../etl/glue_jobs/nyc_quality_checkpoint.py"
  etag   = filemd5("${path.module}/../etl/glue_jobs/nyc_quality_checkpoint.py")
}

resource "aws_s3_object" "great_expectations_script" {
  bucket = aws_s3_bucket.lakehouse.id
  key    = "glue_jobs/nyc_great_expectations_checkpoint.py"
  source = "${path.module}/../etl/glue_jobs/nyc_great_expectations_checkpoint.py"
  etag   = filemd5("${path.module}/../etl/glue_jobs/nyc_great_expectations_checkpoint.py")
}

resource "aws_s3_object" "publication_script" {
  bucket = aws_s3_bucket.lakehouse.id
  key    = "glue_jobs/nyc_publish_manifest.py"
  source = "${path.module}/../etl/glue_jobs/nyc_publish_manifest.py"
  etag   = filemd5("${path.module}/../etl/glue_jobs/nyc_publish_manifest.py")
}

resource "aws_glue_job" "initialize" {
  name              = "${var.project_name}-${var.environment}-initialize"
  role_arn          = aws_iam_role.glue_service.arn
  glue_version      = "4.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_worker_count
  max_retries       = 0
  timeout           = 30

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.lakehouse.id}/${aws_s3_object.initialize_script.key}"
  }
  default_arguments = local.glue_common_arguments
}

resource "aws_glue_job" "bronze" {
  name              = "${var.project_name}-${var.environment}-bronze"
  role_arn          = aws_iam_role.glue_service.arn
  glue_version      = "4.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_worker_count
  max_retries       = 1
  timeout           = 120

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.lakehouse.id}/${aws_s3_object.bronze_script.key}"
  }
  default_arguments = local.glue_common_arguments
}

resource "aws_glue_job" "silver" {
  name              = "${var.project_name}-${var.environment}-silver"
  role_arn          = aws_iam_role.glue_service.arn
  glue_version      = "4.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_worker_count
  max_retries       = 1
  timeout           = 120

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.lakehouse.id}/${aws_s3_object.silver_script.key}"
  }
  default_arguments = local.glue_common_arguments
}

resource "aws_glue_job" "quality" {
  name              = "${var.project_name}-${var.environment}-reconciliation"
  role_arn          = aws_iam_role.glue_service.arn
  glue_version      = "4.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_worker_count
  max_retries       = 0
  timeout           = 60

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.lakehouse.id}/${aws_s3_object.quality_script.key}"
  }
  default_arguments = local.glue_common_arguments
}

resource "aws_glue_job" "great_expectations" {
  name              = "${var.project_name}-${var.environment}-great-expectations"
  role_arn          = aws_iam_role.glue_service.arn
  glue_version      = "4.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_worker_count
  max_retries       = 0
  timeout           = 60

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.lakehouse.id}/${aws_s3_object.great_expectations_script.key}"
  }
  default_arguments = merge(local.glue_common_arguments, {
    "--additional-python-modules" = "great-expectations==1.19.0"
  })
}

resource "aws_glue_job" "publication" {
  name              = "${var.project_name}-${var.environment}-publication"
  role_arn          = aws_iam_role.glue_service.arn
  glue_version      = "4.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_worker_count
  max_retries       = 0
  timeout           = 30

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.lakehouse.id}/${aws_s3_object.publication_script.key}"
  }
  default_arguments = local.glue_common_arguments
}
