# AWS Glue Spark job for ML training, scoring, and MongoDB Atlas writes.

resource "aws_s3_object" "ml_recommendations_script" {
  bucket = aws_s3_bucket.lakehouse.id
  key    = "glue_jobs/ml/spark_recommendations.py"
  source = "../etl/ml/spark_recommendations.py"
  etag   = filemd5("../etl/ml/spark_recommendations.py")
}

resource "aws_glue_job" "ml_recommendations" {
  name     = "${var.project_name}-ml-recommendations"
  role_arn = aws_iam_role.glue_service_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.lakehouse.id}/${aws_s3_object.ml_recommendations_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-glue-datacatalog"          = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--TempDir"                          = "s3://${aws_s3_bucket.lakehouse.id}/temp/"
    "--additional-python-modules"        = "pymongo"
    "--datalake-formats"                 = "iceberg"
    "--conf"                             = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"

    # MONGODB_URI is intentionally not stored in Terraform state.
    # Pass it at run time with --arguments, or wire it through Secrets Manager later.
    "--MONGODB_DATABASE"                   = "instacart_warehouse"
    "--MONGODB_RECOMMENDATIONS_COLLECTION" = "recommendations"
    "--WAREHOUSE_TABLE_PREFIX"             = "glue_catalog.gold"
    "--TOP_N"                              = "10"
    "--PREDICT_ONLY_UNLABELED"             = "true"
    "--MODEL_VERSION"                      = "spark_logistic_regression_v1"
  }

  glue_version      = "4.0"
  max_retries       = 0
  timeout           = 120
  worker_type       = "G.1X"
  number_of_workers = 2

  execution_property {
    max_concurrent_runs = 1
  }

  tags = {
    Name  = "ml-recommendations"
    Layer = "ml"
  }

  depends_on = [
    aws_s3_object.ml_recommendations_script,
    aws_glue_job.silver_transformation
  ]
}
