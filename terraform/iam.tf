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
        Sid    = "ManageCanonicalTablesAndLogs"
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

resource "aws_iam_role" "mwaa_execution" {
  name = "${var.project_name}-${var.environment}-mwaa"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = ["airflow.amazonaws.com", "airflow-env.amazonaws.com"]
      }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "mwaa_platform" {
  name = "${var.project_name}-${var.environment}-mwaa-platform"
  role = aws_iam_role.mwaa_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "PublishAirflowMetrics"
        Effect   = "Allow"
        Action   = ["airflow:PublishMetrics"]
        Resource = "arn:aws:airflow:${var.aws_region}:${data.aws_caller_identity.current.account_id}:environment/${var.project_name}-${var.environment}"
      },
      {
        Sid      = "ReadMwaaSourceBucket"
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation", "s3:GetBucketPublicAccessBlock", "s3:GetBucketVersioning", "s3:GetEncryptionConfiguration", "s3:ListBucket"]
        Resource = aws_s3_bucket.lakehouse.arn
      },
      {
        Sid      = "ReadMwaaDagAndRequirements"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:GetObjectVersion"]
        Resource = "${aws_s3_bucket.lakehouse.arn}/${var.mwaa_dag_s3_prefix}/*"
      },
      {
        Sid      = "ReadAccountPublicAccessBlock"
        Effect   = "Allow"
        Action   = ["s3:GetAccountPublicAccessBlock"]
        Resource = "*"
      },
      {
        Sid    = "MwaaCloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:DescribeLogGroups",
          "logs:GetLogGroupFields",
          "logs:GetLogEvents",
          "logs:GetLogRecord",
          "logs:GetQueryResults",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:airflow-${var.project_name}-${var.environment}-*"
      },
      {
        Sid      = "MwaaCloudWatchMetrics"
        Effect   = "Allow"
        Action   = ["cloudwatch:PutMetricData"]
        Resource = "*"
      },
      {
        Sid      = "MwaaCeleryQueue"
        Effect   = "Allow"
        Action   = ["sqs:ChangeMessageVisibility", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:GetQueueUrl", "sqs:ReceiveMessage", "sqs:SendMessage"]
        Resource = "arn:aws:sqs:${var.aws_region}:*:airflow-celery-*"
      },
      {
        Sid      = "MwaaManagedKeyForCelery"
        Effect   = "Allow"
        Action   = ["kms:Decrypt", "kms:DescribeKey", "kms:GenerateDataKey*"]
        Resource = "*"
        Condition = {
          StringLike = {
            "kms:ViaService" = "sqs.${var.aws_region}.amazonaws.com"
          }
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "mwaa_pipeline" {
  name = "${var.project_name}-${var.environment}-mwaa-pipeline"
  role = aws_iam_role.mwaa_execution.id

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
        Sid      = "UseRedshiftDataApiQueryPlane"
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
        Sid      = "ListPipelinePrefixes"
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation", "s3:ListBucket"]
        Resource = aws_s3_bucket.lakehouse.arn
        Condition = {
          StringLike = {
            "s3:prefix" = [
              "${var.landing_prefix}/*",
              "${var.reference_prefix}/*",
              "manifests/*"
            ]
          }
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
        Sid      = "PublishAndVerifyRunEvidence"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject"]
        Resource = "${aws_s3_bucket.lakehouse.arn}/manifests/*"
      }
    ]
  })
}
