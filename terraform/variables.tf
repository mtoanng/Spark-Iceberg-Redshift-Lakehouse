# Instacart Lakehouse - Terraform Variables
# AWS S3 (Iceberg storage)

# ============================================================================
# General
# ============================================================================
variable "project_name" {
  description = "Project name for resource naming"
  type        = string
  default     = "instacart-lakehouse"
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

# ============================================================================
# AWS Variables
# ============================================================================
variable "aws_region" {
  description = "AWS region for S3 bucket"
  type        = string
  default     = "us-east-1"
}
