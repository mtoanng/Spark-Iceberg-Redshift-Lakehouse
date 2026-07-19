# IAM Roles and Policies for AWS Glue

# Glue Service Role
resource "aws_iam_role" "glue_service_role" {
  name = "AWSGlueServiceRole-${var.project_name}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Name = "glue-service-role"
  }
}

# Attach AWS managed Glue service policy
resource "aws_iam_role_policy_attachment" "glue_service_policy" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Custom policy for S3 access
resource "aws_iam_role_policy" "glue_s3_policy" {
  name = "GlueS3AccessPolicy"
  role = aws_iam_role.glue_service_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          "${aws_s3_bucket.lakehouse.arn}",
          "${aws_s3_bucket.lakehouse.arn}/*"
        ]
      }
    ]
  })
}

# Custom policy for Glue Catalog access
resource "aws_iam_role_policy" "glue_catalog_policy" {
  name = "GlueCatalogAccessPolicy"
  role = aws_iam_role.glue_service_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:GetPartition",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:BatchCreatePartition",
          "glue:BatchDeletePartition",
          "glue:BatchGetPartition"
        ]
        Resource = [
          "arn:aws:glue:${var.aws_region}:*:catalog",
          "arn:aws:glue:${var.aws_region}:*:database/${aws_glue_catalog_database.instacart.name}",
          "arn:aws:glue:${var.aws_region}:*:table/${aws_glue_catalog_database.instacart.name}/*"
        ]
      }
    ]
  })
}

# CloudWatch Logs policy (for Glue job logs)
resource "aws_iam_role_policy" "glue_cloudwatch_policy" {
  name = "GlueCloudWatchLogsPolicy"
  role = aws_iam_role.glue_service_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:${var.aws_region}:*:log-group:/aws-glue/*"
      }
    ]
  })
}

# Note: DuckDB runs inside Docker with ~/.aws mounted as read-only volume.
# It authenticates using the IAM user credentials directly via credential_chain.
# No separate DuckDB role is needed - the IAM user permissions above are sufficient.

output "duckdb_role_arn" {
  description = "DuckDB uses IAM user credentials directly via mounted ~/.aws - no separate role needed"
  value       = "n/a - DuckDB uses IAM user credentials via ~/.aws mount"
}
