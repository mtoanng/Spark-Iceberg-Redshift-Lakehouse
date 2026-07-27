resource "aws_iam_role" "redshift_spectrum" {
  name = "${var.project_name}-${var.environment}-redshift-spectrum"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "redshift.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "redshift_spectrum" {
  name = "${var.project_name}-${var.environment}-bronze-silver-read"
  role = aws_iam_role.redshift_spectrum.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListIcebergWarehouse"
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation", "s3:ListBucket"]
        Resource = aws_s3_bucket.lakehouse.arn
        Condition = {
          StringLike = {
            "s3:prefix" = [
              "${var.warehouse_prefix}/bronze/*",
              "${var.warehouse_prefix}/silver/*"
            ]
          }
        }
      },
      {
        Sid    = "ReadBronzeSilverIceberg"
        Effect = "Allow"
        Action = ["s3:GetObject"]
        Resource = [
          "${aws_s3_bucket.lakehouse.arn}/${var.warehouse_prefix}/bronze/*",
          "${aws_s3_bucket.lakehouse.arn}/${var.warehouse_prefix}/silver/*"
        ]
      },
      {
        Sid    = "ReadGlueCatalog"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetTableVersion",
          "glue:GetTableVersions",
          "glue:BatchGetPartition"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_redshiftserverless_namespace" "gold" {
  namespace_name        = "${var.project_name}-${var.environment}"
  db_name               = var.redshift_database_name
  manage_admin_password = true
  iam_roles             = [aws_iam_role.redshift_spectrum.arn]
  default_iam_role_arn  = aws_iam_role.redshift_spectrum.arn

  depends_on = [aws_iam_role_policy.redshift_spectrum]
}

resource "aws_redshiftserverless_workgroup" "gold" {
  namespace_name      = aws_redshiftserverless_namespace.gold.namespace_name
  workgroup_name      = "${var.project_name}-${var.environment}"
  base_capacity       = 8
  publicly_accessible = false
}

resource "aws_redshiftdata_statement" "bronze_external_schema" {
  workgroup_name = aws_redshiftserverless_workgroup.gold.workgroup_name
  database       = aws_redshiftserverless_namespace.gold.db_name
  secret_arn     = aws_redshiftserverless_namespace.gold.admin_password_secret_arn
  statement_name = "create-bronze-external-schema"
  sql            = "CREATE EXTERNAL SCHEMA IF NOT EXISTS bronze_external FROM DATA CATALOG DATABASE 'bronze' IAM_ROLE default REGION '${var.aws_region}'"
}

resource "aws_redshiftdata_statement" "silver_external_schema" {
  workgroup_name = aws_redshiftserverless_workgroup.gold.workgroup_name
  database       = aws_redshiftserverless_namespace.gold.db_name
  secret_arn     = aws_redshiftserverless_namespace.gold.admin_password_secret_arn
  statement_name = "create-silver-external-schema"
  sql            = "CREATE EXTERNAL SCHEMA IF NOT EXISTS silver_external FROM DATA CATALOG DATABASE 'silver' IAM_ROLE default REGION '${var.aws_region}'"
}

resource "aws_redshiftdata_statement" "gold_schema" {
  workgroup_name = aws_redshiftserverless_workgroup.gold.workgroup_name
  database       = aws_redshiftserverless_namespace.gold.db_name
  secret_arn     = aws_redshiftserverless_namespace.gold.admin_password_secret_arn
  statement_name = "create-gold-schema"
  sql            = "CREATE SCHEMA IF NOT EXISTS gold"
}
