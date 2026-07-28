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
  subnet_ids          = var.private_subnet_ids
  security_group_ids  = [aws_security_group.redshift.id]
}

resource "aws_security_group" "redshift" {
  name        = "${var.project_name}-${var.environment}-redshift"
  description = "Redshift Serverless access from the MWAA execution environment."
  vpc_id      = var.vpc_id

  ingress {
    description     = "dbt and verification from MWAA"
    from_port       = 5439
    to_port         = 5439
    protocol        = "tcp"
    security_groups = [aws_security_group.mwaa.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
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

locals {
  mwaa_redshift_user = "IAMR:${aws_iam_role.mwaa_execution.name}"
}

resource "aws_redshiftdata_statement" "mwaa_database_user" {
  workgroup_name = aws_redshiftserverless_workgroup.gold.workgroup_name
  database       = aws_redshiftserverless_namespace.gold.db_name
  secret_arn     = aws_redshiftserverless_namespace.gold.admin_password_secret_arn
  statement_name = "create-mwaa-iam-user"
  sql            = "CREATE USER \"${local.mwaa_redshift_user}\" PASSWORD DISABLE"

  depends_on = [aws_redshiftdata_statement.gold_schema]
}

resource "aws_redshiftdata_statement" "mwaa_bronze_usage" {
  workgroup_name = aws_redshiftserverless_workgroup.gold.workgroup_name
  database       = aws_redshiftserverless_namespace.gold.db_name
  secret_arn     = aws_redshiftserverless_namespace.gold.admin_password_secret_arn
  statement_name = "grant-mwaa-bronze-usage"
  sql            = "GRANT USAGE ON SCHEMA bronze_external TO \"${local.mwaa_redshift_user}\""

  depends_on = [
    aws_redshiftdata_statement.bronze_external_schema,
    aws_redshiftdata_statement.mwaa_database_user,
  ]
}

resource "aws_redshiftdata_statement" "mwaa_silver_usage" {
  workgroup_name = aws_redshiftserverless_workgroup.gold.workgroup_name
  database       = aws_redshiftserverless_namespace.gold.db_name
  secret_arn     = aws_redshiftserverless_namespace.gold.admin_password_secret_arn
  statement_name = "grant-mwaa-silver-usage"
  sql            = "GRANT USAGE ON SCHEMA silver_external TO \"${local.mwaa_redshift_user}\""

  depends_on = [
    aws_redshiftdata_statement.silver_external_schema,
    aws_redshiftdata_statement.mwaa_database_user,
  ]
}

resource "aws_redshiftdata_statement" "mwaa_gold_owner" {
  workgroup_name = aws_redshiftserverless_workgroup.gold.workgroup_name
  database       = aws_redshiftserverless_namespace.gold.db_name
  secret_arn     = aws_redshiftserverless_namespace.gold.admin_password_secret_arn
  statement_name = "grant-mwaa-gold-owner"
  sql            = "GRANT CREATE, USAGE ON SCHEMA gold TO \"${local.mwaa_redshift_user}\""

  depends_on = [
    aws_redshiftdata_statement.gold_schema,
    aws_redshiftdata_statement.mwaa_database_user,
  ]
}

resource "aws_redshiftdata_statement" "mwaa_temp_tables" {
  workgroup_name = aws_redshiftserverless_workgroup.gold.workgroup_name
  database       = aws_redshiftserverless_namespace.gold.db_name
  secret_arn     = aws_redshiftserverless_namespace.gold.admin_password_secret_arn
  statement_name = "grant-mwaa-temp-tables"
  sql            = "GRANT TEMP ON DATABASE ${aws_redshiftserverless_namespace.gold.db_name} TO \"${local.mwaa_redshift_user}\""

  depends_on = [aws_redshiftdata_statement.mwaa_database_user]
}
