variable "aws_region" {
  description = "AWS region for all lakehouse resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment suffix for resource names (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project name prefix for AWS resources"
  type        = string
  default     = "instacart-lakehouse"
}

variable "s3_bucket_name" {
  description = "S3 bucket name for lakehouse storage (must be globally unique)"
  type        = string
}

variable "s3_raw_prefix" {
  description = "S3 prefix containing raw Instacart CSV files"
  type        = string
  default     = "raw/instacart"
}
