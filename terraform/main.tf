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

output "glue_role_arn" {
  value       = aws_iam_role.glue_service.arn
  description = "Glue execution role ARN."
}

output "glue_job_names" {
  value = {
    initialize            = aws_glue_job.initialize.name
    bronze                = aws_glue_job.bronze.name
    silver                = aws_glue_job.silver.name
    quality               = aws_glue_job.quality.name
    great_expectations    = aws_glue_job.great_expectations.name
    schema_evolution_2025 = aws_glue_job.schema_evolution.name
  }
  description = "Glue jobs consumed by the Phase 5 Airflow DAG."
}

output "airflow_runner_instance_profile" {
  value       = var.airflow_runner_ami_id == "" ? null : aws_iam_instance_profile.airflow_runner[0].name
  description = "Instance profile used by the optional temporary Airflow runner."
}
