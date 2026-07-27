resource "aws_iam_role" "emr_serverless_execution" {
  name = "${var.project_name}-${var.environment}-emr-serverless"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "emr-serverless.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "emr_serverless_lakehouse" {
  name = "${var.project_name}-${var.environment}-lakehouse-access"
  role = aws_iam_role.emr_serverless_execution.id

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
        Sid    = "ReadSourceReferenceAndArtifacts"
        Effect = "Allow"
        Action = ["s3:GetObject"]
        Resource = [
          "${aws_s3_bucket.lakehouse.arn}/${var.landing_prefix}/*",
          "${aws_s3_bucket.lakehouse.arn}/${var.reference_prefix}/*",
          "${aws_s3_bucket.lakehouse.arn}/spark_jobs/*"
        ]
      },
      {
        Sid    = "ManageCanonicalTablesAndRunArtifacts"
        Effect = "Allow"
        Action = [
          "s3:AbortMultipartUpload",
          "s3:DeleteObject",
          "s3:GetObject",
          "s3:ListMultipartUploadParts",
          "s3:PutObject"
        ]
        Resource = [
          "${aws_s3_bucket.lakehouse.arn}/${var.warehouse_prefix}/*",
          "${aws_s3_bucket.lakehouse.arn}/tmp/*",
          "${aws_s3_bucket.lakehouse.arn}/manifests/*",
          "${aws_s3_bucket.lakehouse.arn}/emr-serverless-logs/*"
        ]
      },
      {
        Sid    = "GlueCatalogIcebergMetadata"
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

resource "aws_iam_role_policy" "athena_iceberg_verify" {
  name = "${var.project_name}-${var.environment}-athena-iceberg-verify"
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
        Resource = aws_athena_workgroup.iceberg_verify.arn
      },
      {
        Sid    = "ReadIcebergGlueMetadata"
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
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:database/bronze",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:database/silver",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/bronze/*",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/silver/*"
        ]
      },
      {
        Sid      = "ListIcebergAndResultsPrefixes"
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = aws_s3_bucket.lakehouse.arn
        Condition = {
          StringLike = {
            "s3:prefix" = ["${var.warehouse_prefix}/bronze/*", "${var.warehouse_prefix}/silver/*", "${var.athena_results_prefix}/*"]
          }
        }
      },
      {
        Sid    = "ReadOpenIcebergObjects"
        Effect = "Allow"
        Action = ["s3:GetObject"]
        Resource = [
          "${aws_s3_bucket.lakehouse.arn}/${var.warehouse_prefix}/bronze/*",
          "${aws_s3_bucket.lakehouse.arn}/${var.warehouse_prefix}/silver/*"
        ]
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
        Sid      = "StartAndObserveEmrServerlessJobs"
        Effect   = "Allow"
        Action   = ["emr-serverless:StartJobRun", "emr-serverless:GetJobRun", "emr-serverless:CancelJobRun", "emr-serverless:GetApplication"]
        Resource = aws_emrserverless_application.spark.arn
      },
      {
        Sid    = "ConnectToRedshiftServerlessForDbt"
        Effect = "Allow"
        Action = ["redshift-serverless:GetCredentials", "redshift-serverless:GetWorkgroup", "redshift-serverless:GetNamespace"]
        Resource = [
          aws_redshiftserverless_workgroup.gold.arn,
          aws_redshiftserverless_namespace.gold.arn
        ]
      },
      {
        Sid      = "UseRedshiftDataApiForGoldVerification"
        Effect   = "Allow"
        Action   = ["redshift-data:DescribeStatement", "redshift-data:ExecuteStatement", "redshift-data:GetStatementResult"]
        Resource = "*"
      },
      {
        Sid      = "PassEmrServerlessRole"
        Effect   = "Allow"
        Action   = ["iam:PassRole"]
        Resource = aws_iam_role.emr_serverless_execution.arn
        Condition = {
          StringEquals = { "iam:PassedToService" = "emr-serverless.amazonaws.com" }
        }
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
