variable "aws_region" {
  type        = string
  description = "AWS region for the bounded demo."
  default     = "us-east-1"
}

variable "environment" {
  type        = string
  description = "Short environment name used in resource names."
  default     = "dev"

  validation {
    condition     = can(regex("^[a-z0-9-]+$", var.environment))
    error_message = "environment must contain lowercase letters, digits, and hyphens only."
  }
}

variable "project_name" {
  type        = string
  description = "Resource prefix for the NYC HVFHV lakehouse."
  default     = "nyc-hvfhs-lakehouse"
}

variable "s3_bucket_name" {
  type        = string
  description = "Globally unique S3 bucket name; set in a private tfvars file."
}

variable "landing_prefix" {
  type        = string
  description = "S3 prefix for immutable source objects."
  default     = "landing"
}

variable "reference_prefix" {
  type        = string
  description = "S3 prefix for the Taxi Zone lookup."
  default     = "reference"
}

variable "warehouse_prefix" {
  type        = string
  description = "S3 prefix for Iceberg metadata and data files."
  default     = "warehouse"
}

variable "glue_worker_type" {
  type        = string
  description = "Smallest approved Glue worker type for the bounded demo."
  default     = "G.1X"
}

variable "glue_worker_count" {
  type        = number
  description = "Worker count for each Glue job."
  default     = 2

  validation {
    condition     = var.glue_worker_count >= 2 && var.glue_worker_count <= 10
    error_message = "glue_worker_count must be between 2 and 10."
  }
}
