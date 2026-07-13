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
      Project     = "instacart-lakehouse"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

# Variables
variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment (dev/prod)"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project name prefix"
  type        = string
  default     = "instacart-lakehouse"
}

variable "s3_bucket_name" {
  description = "S3 bucket name for lakehouse (must be globally unique)"
  type        = string
}

# Outputs
output "s3_bucket_name" {
  description = "Lakehouse S3 bucket name"
  value       = aws_s3_bucket.lakehouse.id
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
