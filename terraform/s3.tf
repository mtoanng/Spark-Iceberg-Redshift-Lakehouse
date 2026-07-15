# S3 Bucket for Lakehouse Storage

resource "aws_s3_bucket" "lakehouse" {
  bucket = var.s3_bucket_name

  tags = {
    Name = "${var.project_name}-${var.environment}"
  }
}

# Enable versioning for data recovery
resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Enable server-side encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
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

# Lifecycle rules for cost optimization
resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    id     = "transition-old-versions"
    status = "Enabled"

    filter {}

    noncurrent_version_transition {
      noncurrent_days = 30
      storage_class   = "STANDARD_IA"
    }

    noncurrent_version_transition {
      noncurrent_days = 90
      storage_class   = "GLACIER"
    }

    noncurrent_version_expiration {
      noncurrent_days = 365
    }
  }

  rule {
    id     = "delete-incomplete-uploads"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# S3 folder structure (virtual via prefixes)
# /raw/instacart/*.csv          - Raw CSV files from Kaggle
# /warehouse/bronze/            - Iceberg Bronze tables
# /warehouse/silver/            - Iceberg Silver tables
# /warehouse/gold/              - Iceberg Gold tables (dbt)
# /glue_jobs/                   - Glue job scripts
# /dbt-glue-staging/            - dbt-glue temporary files
