locals {
  glue_common_arguments = {
    "--datalake-formats"                 = "iceberg"
    "--enable-glue-datacatalog"          = "true"
    "--enable-job-insights"              = "true"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-language"                     = "python"
    "--TempDir"                          = "s3://${aws_s3_bucket.lakehouse.id}/tmp/"
    "--conf"                             = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
  }
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

resource "aws_s3_object" "schema_evolution_script" {
  bucket = aws_s3_bucket.lakehouse.id
  key    = "glue_jobs/nyc_schema_evolution_2025.py"
  source = "${path.module}/../etl/glue_jobs/nyc_schema_evolution_2025.py"
  etag   = filemd5("${path.module}/../etl/glue_jobs/nyc_schema_evolution_2025.py")
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
  name              = "${var.project_name}-${var.environment}-quality"
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

resource "aws_glue_job" "schema_evolution" {
  name              = "${var.project_name}-${var.environment}-schema-evolution-2025"
  role_arn          = aws_iam_role.glue_service.arn
  glue_version      = "4.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_worker_count
  max_retries       = 0
  timeout           = 30

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.lakehouse.id}/${aws_s3_object.schema_evolution_script.key}"
  }
  default_arguments = local.glue_common_arguments
}
