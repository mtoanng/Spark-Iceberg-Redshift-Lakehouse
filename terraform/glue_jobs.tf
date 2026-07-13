# AWS Glue Jobs for Bronze and Silver layers

# Upload Glue job scripts to S3 first
resource "aws_s3_object" "bronze_script" {
  bucket = aws_s3_bucket.lakehouse.id
  key    = "glue_jobs/bronze_ingestion.py"
  source = "../etl/glue_jobs/bronze_ingestion.py"
  etag   = filemd5("../etl/glue_jobs/bronze_ingestion.py")
}

resource "aws_s3_object" "silver_script" {
  bucket = aws_s3_bucket.lakehouse.id
  key    = "glue_jobs/silver_transformation.py"
  source = "../etl/glue_jobs/silver_transformation.py"
  etag   = filemd5("../etl/glue_jobs/silver_transformation.py")
}

# Bronze Ingestion Glue Job
resource "aws_glue_job" "bronze_ingestion" {
  name     = "${var.project_name}-bronze-ingestion"
  role_arn = aws_iam_role.glue_service_role.arn
  
  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.lakehouse.id}/${aws_s3_object.bronze_script.key}"
    python_version  = "3"
  }
  
  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--enable-metrics"                   = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.lakehouse.id}/spark-logs/"
    "--enable-job-insights"              = "true"
    "--enable-glue-datacatalog"          = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--TempDir"                          = "s3://${aws_s3_bucket.lakehouse.id}/temp/"
    
    # Job-specific parameters
    "--S3_BUCKET"     = aws_s3_bucket.lakehouse.id
    "--S3_RAW_PREFIX" = "raw/instacart"
    
    # Iceberg configuration
    "--datalake-formats" = "iceberg"
    "--conf"             = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
  }
  
  glue_version      = "4.0"
  max_retries       = 1
  timeout           = 120  # 2 hours
  worker_type       = "G.1X"
  number_of_workers = 2
  
  execution_property {
    max_concurrent_runs = 1
  }
  
  tags = {
    Name  = "bronze-ingestion"
    Layer = "bronze"
  }
  
  depends_on = [
    aws_s3_object.bronze_script,
    aws_glue_catalog_database.instacart
  ]
}

# Silver Transformation Glue Job
resource "aws_glue_job" "silver_transformation" {
  name     = "${var.project_name}-silver-transformation"
  role_arn = aws_iam_role.glue_service_role.arn
  
  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.lakehouse.id}/${aws_s3_object.silver_script.key}"
    python_version  = "3"
  }
  
  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--enable-metrics"                   = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.lakehouse.id}/spark-logs/"
    "--enable-job-insights"              = "true"
    "--enable-glue-datacatalog"          = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--TempDir"                          = "s3://${aws_s3_bucket.lakehouse.id}/temp/"
    
    # Iceberg configuration
    "--datalake-formats" = "iceberg"
    "--conf"             = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
  }
  
  glue_version      = "4.0"
  max_retries       = 1
  timeout           = 180  # 3 hours
  worker_type       = "G.1X"
  number_of_workers = 3  # More workers for transformation
  
  execution_property {
    max_concurrent_runs = 1
  }
  
  tags = {
    Name  = "silver-transformation"
    Layer = "silver"
  }
  
  depends_on = [
    aws_s3_object.silver_script,
    aws_glue_job.bronze_ingestion  # Run after bronze
  ]
}

# CloudWatch Log Groups for Glue Jobs
resource "aws_cloudwatch_log_group" "bronze_job_logs" {
  name              = "/aws-glue/jobs/${aws_glue_job.bronze_ingestion.name}"
  retention_in_days = 7
  
  tags = {
    Job = "bronze-ingestion"
  }
}

resource "aws_cloudwatch_log_group" "silver_job_logs" {
  name              = "/aws-glue/jobs/${aws_glue_job.silver_transformation.name}"
  retention_in_days = 7
  
  tags = {
    Job = "silver-transformation"
  }
}
