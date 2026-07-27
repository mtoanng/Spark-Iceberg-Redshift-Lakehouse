terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

data "aws_caller_identity" "current" {}

output "aws_account_id" {
  value       = data.aws_caller_identity.current.account_id
  description = "AWS account used by the approved Terraform run."
}

output "aws_region" {
  value       = var.aws_region
  description = "AWS region used by the approved Terraform run."
}

output "s3_bucket_name" {
  value       = aws_s3_bucket.lakehouse.id
  description = "Canonical lakehouse S3 bucket."
}

output "s3_landing_uri" {
  value       = "s3://${aws_s3_bucket.lakehouse.id}/${var.landing_prefix}"
  description = "Landing prefix for monthly source objects."
}

output "s3_warehouse_uri" {
  value       = "s3://${aws_s3_bucket.lakehouse.id}/${var.warehouse_prefix}"
  description = "Iceberg warehouse root."
}

output "glue_database_name" {
  value       = { for name, database in aws_glue_catalog_database.namespace : name => database.name }
  description = "Canonical Glue Catalog namespaces for the NYC lakehouse."
}

output "emr_serverless_application_id" {
  value       = aws_emrserverless_application.spark.id
  description = "Persistent EMR Serverless Spark application ID."
}

output "emr_serverless_execution_role_arn" {
  value       = aws_iam_role.emr_serverless_execution.arn
  description = "EMR Serverless execution role ARN; also retained for dbt-glue sessions."
}

output "spark_script_prefix_uri" {
  value       = "s3://${aws_s3_bucket.lakehouse.id}/spark_jobs"
  description = "S3 prefix for EMR Serverless PySpark entrypoints."
}

output "spark_package_uri" {
  value       = "s3://${aws_s3_bucket.lakehouse.id}/${var.spark_package_s3_key}"
  description = "S3 URI for the shared EMR Serverless Python package."
}

output "airflow_runner_instance_profile" {
  value       = var.airflow_runner_ami_id == "" ? null : aws_iam_instance_profile.airflow_runner[0].name
  description = "Instance profile used by the optional temporary Airflow runner."
}

output "publication_prefix_uri" {
  value       = "s3://${aws_s3_bucket.lakehouse.id}/manifests"
  description = "Month-partitioned publication-manifest prefix."
}
