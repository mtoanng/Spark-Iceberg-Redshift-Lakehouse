# Terraform Configuration for Instacart Lakehouse
# AWS Glue + S3 + Iceberg Infrastructure

terraform {
  required_version = ">= 1.0"

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

# Outputs
output "aws_account_id" {
  description = "AWS account ID for runtime AWS_ACCOUNT_ID"
  value       = data.aws_caller_identity.current.account_id
}

output "aws_region" {
  description = "AWS region for runtime AWS_REGION"
  value       = var.aws_region
}

output "s3_bucket_name" {
  description = "Lakehouse S3 bucket name"
  value       = aws_s3_bucket.lakehouse.id
}

output "s3_raw_prefix" {
  description = "Raw data prefix in the lakehouse S3 bucket"
  value       = var.s3_raw_prefix
}

output "s3_gold_path" {
  description = "Gold layer S3 path for runtime S3_GOLD_PATH"
  value       = "s3://${aws_s3_bucket.lakehouse.id}/gold"
}

output "s3_warehouse_path" {
  description = "Iceberg warehouse S3 path"
  value       = "s3://${aws_s3_bucket.lakehouse.id}/warehouse"
}

output "glue_database_name" {
  description = "Glue Catalog database name"
  value       = aws_glue_catalog_database.instacart.name
}

output "glue_role_arn" {
  description = "Glue service role ARN"
  value       = aws_iam_role.glue_service_role.arn
}

output "bronze_job_name" {
  description = "Bronze ingestion Glue job name"
  value       = aws_glue_job.bronze_ingestion.name
}

output "silver_job_name" {
  description = "Silver transformation Glue job name"
  value       = aws_glue_job.silver_transformation.name
}

output "ml_recommendations_job_name" {
  description = "Spark ML recommendation Glue job name"
  value       = aws_glue_job.ml_recommendations.name
}
