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

# DuckDB Role (for warehouse query engine)
# Allows DuckDB to read Iceberg tables via Glue Catalog
resource "aws_iam_role" "duckdb_role" {
  name = "DuckDBGlueCatalogRole-${var.project_name}"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"  # Adjust based on where DuckDB runs
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
  
  tags = {
    Name = "duckdb-catalog-role"
  }
}

# DuckDB S3 read-only access
resource "aws_iam_role_policy" "duckdb_s3_policy" {
  name = "DuckDBS3ReadPolicy"
  role = aws_iam_role.duckdb_role.id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
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

# DuckDB Glue Catalog read access
resource "aws_iam_role_policy" "duckdb_catalog_policy" {
  name = "DuckDBCatalogReadPolicy"
  role = aws_iam_role.duckdb_role.id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:GetPartition",
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

# Output DuckDB role ARN for warehouse configuration
output "duckdb_role_arn" {
  description = "DuckDB role ARN for Glue Catalog access"
  value       = aws_iam_role.duckdb_role.arn
}
