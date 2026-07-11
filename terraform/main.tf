# Instacart Lakehouse Infrastructure - AWS Only
# AWS S3 (Iceberg storage) + Spark OSS (compute)

terraform {
  required_version = ">= 1.5.0"
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }
}

# Random suffix for unique names
resource "random_id" "suffix" {
  byte_length = 4
}

# ============================================================================
# AWS PROVIDER - S3 for Lakehouse Storage
# ============================================================================
provider "aws" {
  region = var.aws_region
}

# S3 Bucket for Iceberg Tables (Bronze + Silver)
resource "aws_s3_bucket" "lakehouse" {
  bucket = "${var.project_name}-${random_id.suffix.hex}"
  
  tags = {
    Name        = "Instacart Lakehouse"
    Environment = var.environment
    ManagedBy   = "Terraform"
    Layer       = "Bronze-Silver"
  }
}

# Enable versioning for data protection
resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  
  versioning_configuration {
    status = "Enabled"
  }
}

# Lifecycle policy for cost optimization
resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    id     = "archive-old-versions"
    status = "Enabled"

    noncurrent_version_transition {
      noncurrent_days = 30
      storage_class   = "STANDARD_IA"
    }

    noncurrent_version_expiration {
      noncurrent_days = 90
    }
  }
  
  rule {
    id     = "transition-to-ia"
    status = "Enabled"
    
    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }
  }
}

# Block public access
resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# IAM User for Spark
resource "aws_iam_user" "spark" {
  name = "${var.project_name}-spark"
  
  tags = {
    Name      = "Spark Service User"
    ManagedBy = "Terraform"
  }
}

# Access keys for Spark
resource "aws_iam_access_key" "spark" {
  user = aws_iam_user.spark.name
}

# IAM Policy for S3 access
resource "aws_iam_user_policy" "spark_s3" {
  name = "S3LakehouseAccess"
  user = aws_iam_user.spark.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ListBucket"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = aws_s3_bucket.lakehouse.arn
      },
      {
        Sid    = "ObjectAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = "${aws_s3_bucket.lakehouse.arn}/*"
      }
    ]
  })
}

# ============================================================================
# OUTPUTS
# ============================================================================

# AWS Outputs
output "s3_bucket_name" {
  description = "Name of S3 bucket for lakehouse"
  value       = aws_s3_bucket.lakehouse.id
}

output "s3_bucket_arn" {
  description = "ARN of S3 bucket"
  value       = aws_s3_bucket.lakehouse.arn
}

output "aws_access_key_id" {
  description = "AWS access key for Spark"
  value       = aws_iam_access_key.spark.id
  sensitive   = true
}

output "aws_secret_access_key" {
  description = "AWS secret key for Spark"
  value       = aws_iam_access_key.spark.secret
  sensitive   = true
}

output "architecture_summary" {
  description = "Architecture summary"
  value = {
    storage  = "AWS S3 (${aws_s3_bucket.lakehouse.id})"
    compute  = "Spark OSS (local dev / EC2 deploy)"
    format   = "Apache Iceberg (Bronze/Silver/Gold)"
    metadata = "MongoDB (catalog)"
    query    = "DuckDB (embedded)"
  }
}
