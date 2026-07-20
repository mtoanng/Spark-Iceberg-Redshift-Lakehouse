resource "aws_iam_role" "glue_service" {
  name = "${var.project_name}-${var.environment}-glue"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "glue.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_service.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_lakehouse" {
  name = "${var.project_name}-${var.environment}-lakehouse-access"
  role = aws_iam_role.glue_service.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "BucketLocation"
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation", "s3:ListBucket"]
        Resource = aws_s3_bucket.lakehouse.arn
      },
      {
        Sid    = "LakehouseObjects"
        Effect = "Allow"
        Action = [
          "s3:AbortMultipartUpload",
          "s3:DeleteObject",
          "s3:GetObject",
          "s3:ListMultipartUploadParts",
          "s3:PutObject"
        ]
        Resource = "${aws_s3_bucket.lakehouse.arn}/*"
      },
      {
        Sid    = "GlueCatalog"
        Effect = "Allow"
        Action = [
          "glue:BatchCreatePartition",
          "glue:BatchDeletePartition",
          "glue:BatchGetPartition",
          "glue:CreateDatabase",
          "glue:CreateTable",
          "glue:DeleteTable",
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:GetTables",
          "glue:UpdateTable"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy" "athena_gold_query" {
  name = "${var.project_name}-${var.environment}-athena-gold-query"
  role = aws_iam_role.airflow_runner.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AthenaOneWorkgroup"
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:StopQueryExecution",
          "athena:GetWorkGroup"
        ]
        Resource = aws_athena_workgroup.gold_query.arn
      },
      {
        Sid    = "ReadGoldGlueMetadata"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartitions"
        ]
        Resource = [
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:database/gold",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/gold/*"
        ]
      },
      {
        Sid      = "ListGoldAndResultsPrefixes"
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = aws_s3_bucket.lakehouse.arn
        Condition = {
          StringLike = {
            "s3:prefix" = ["${var.warehouse_prefix}/gold/*", "${var.athena_results_prefix}/*"]
          }
        }
      },
      {
        Sid      = "ReadGoldIcebergObjects"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = "${aws_s3_bucket.lakehouse.arn}/${var.warehouse_prefix}/gold/*"
      },
      {
        Sid      = "ReadWriteAthenaResultsOnly"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"]
        Resource = "${aws_s3_bucket.lakehouse.arn}/${var.athena_results_prefix}/*"
      }
    ]
  })
}

resource "aws_iam_role" "airflow_runner" {
  name = "${var.project_name}-${var.environment}-airflow-runner"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_instance_profile" "airflow_runner" {
  count = var.airflow_runner_ami_id == "" ? 0 : 1
  name  = "${var.project_name}-${var.environment}-airflow-profile"
  role  = aws_iam_role.airflow_runner.name
}

resource "aws_iam_role_policy_attachment" "airflow_ssm" {
  role       = aws_iam_role.airflow_runner.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy" "airflow_runner_access" {
  name = "${var.project_name}-${var.environment}-airflow-runner-access"
  role = aws_iam_role.airflow_runner.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "InvokeNamedGlueJobs"
        Effect   = "Allow"
        Action   = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:GetJob"]
        Resource = "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:job/${var.project_name}-${var.environment}-*"
      },
      {
        Sid    = "ReadLandingAndReference"
        Effect = "Allow"
        Action = ["s3:GetObject"]
        Resource = [
          "${aws_s3_bucket.lakehouse.arn}/${var.landing_prefix}/*",
          "${aws_s3_bucket.lakehouse.arn}/${var.reference_prefix}/*"
        ]
      },
      {
        Sid       = "ListLandingAndReference"
        Effect    = "Allow"
        Action    = ["s3:GetBucketLocation", "s3:ListBucket"]
        Resource  = aws_s3_bucket.lakehouse.arn
        Condition = { StringLike = { "s3:prefix" = ["${var.landing_prefix}/*", "${var.reference_prefix}/*"] } }
      },
      {
        Sid      = "PublishRunManifests"
        Effect   = "Allow"
        Action   = ["s3:PutObject", "s3:GetObject"]
        Resource = "${aws_s3_bucket.lakehouse.arn}/manifests/*"
      }
    ]
  })
}
