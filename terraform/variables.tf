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

variable "spark_package_path" {
  type        = string
  description = "Repository-root-relative deterministic PySpark zip produced before deployment."
  default     = "build/nyc_spark_jobs.zip"
}

variable "redshift_database_name" {
  type        = string
  description = "Database created inside the bounded Redshift Serverless namespace."
  default     = "lakehouse"
}

variable "spark_package_s3_key" {
  type        = string
  description = "S3 key for the shared EMR Serverless Python package."
  default     = "spark_jobs/nyc_spark_jobs.zip"
}

variable "emr_serverless_idle_timeout_minutes" {
  type        = number
  description = "Idle minutes before the persistent EMR Serverless application stops."
  default     = 15

  validation {
    condition     = var.emr_serverless_idle_timeout_minutes >= 1 && var.emr_serverless_idle_timeout_minutes <= 60
    error_message = "emr_serverless_idle_timeout_minutes must be between 1 and 60."
  }
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
  default     = "t3.medium"
}

variable "airflow_runner_key_name" {
  type        = string
  description = "Optional pre-existing EC2 key name; leave empty for SSM-only access."
  default     = ""
}
