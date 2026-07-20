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

variable "athena_results_prefix" {
  type        = string
  description = "Prefix in the existing project bucket for Athena query results."
  default     = "athena-results"

  validation {
    condition     = can(regex("^[a-zA-Z0-9!_.*'()=-]+(/[a-zA-Z0-9!_.*'()=-]+)*$", var.athena_results_prefix))
    error_message = "athena_results_prefix must be a safe, relative S3 prefix."
  }
}

variable "athena_bytes_scanned_cutoff" {
  type        = number
  description = "Maximum bytes Athena may scan per query; Athena requires at least 10 MiB."
  default     = 104857600

  validation {
    condition     = var.athena_bytes_scanned_cutoff >= 10485760
    error_message = "athena_bytes_scanned_cutoff must be at least 10485760 bytes (10 MiB)."
  }
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

variable "glue_package_path" {
  type        = string
  description = "Deterministic zip produced by scripts/package_glue_jobs.py before deployment."
  default     = "build/nyc_glue_jobs.zip"
}

variable "glue_package_s3_key" {
  type        = string
  description = "S3 key for the shared Glue Python package."
  default     = "glue_jobs/nyc_glue_jobs.zip"
}

variable "airflow_runner_ami_id" {
  type        = string
  description = "Optional approved Linux AMI ID; empty keeps the temporary runner disabled."
  default     = ""
}

variable "airflow_runner_subnet_id" {
  type        = string
  description = "Subnet for the optional temporary Airflow runner."
  default     = ""
}

variable "airflow_runner_instance_type" {
  type        = string
  description = "Instance type for the optional temporary Airflow runner."
  default     = "t3.small"
}

variable "airflow_runner_key_name" {
  type        = string
  description = "Optional pre-existing EC2 key name; leave empty for SSM-only access."
  default     = ""
}
