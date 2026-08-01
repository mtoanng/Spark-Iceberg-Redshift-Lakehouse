locals {
  mwaa_environment_name = "${var.project_name}-${var.environment}"
  mwaa_python_files = toset([
    for path in setunion(
      fileset("${path.module}/../etl", "__init__.py"),
      fileset("${path.module}/../etl", "dags/*.py"),
      fileset("${path.module}/../etl", "contracts/*.py"),
      fileset("${path.module}/../etl", "orchestration/*.py"),
      fileset("${path.module}/../etl", "publication/*.py"),
      fileset("${path.module}/../etl", "sources/*.py")
    ) : "etl/${path}"
  ])
  mwaa_dbt_files = toset([
    for path in setunion(
      fileset("${path.module}/../etl/dbt_project", "**/*.sql"),
      fileset("${path.module}/../etl/dbt_project", "**/*.yml"),
      fileset("${path.module}/../etl/dbt_project", "**/*.yaml")
    ) : "etl/dbt_project/${path}"
    if !startswith(path, "target/") && path != ".user.yml"
  ])
  mwaa_source_files = setunion(local.mwaa_python_files, local.mwaa_dbt_files)
}

resource "aws_s3_object" "mwaa_source" {
  for_each = local.mwaa_source_files

  bucket = aws_s3_bucket.lakehouse.id
  key    = "${var.mwaa_dag_s3_prefix}/${each.value}"
  source = "${path.module}/../${each.value}"
  etag   = filemd5("${path.module}/../${each.value}")
}

resource "aws_s3_object" "mwaa_airflowignore" {
  bucket = aws_s3_bucket.lakehouse.id
  key    = "${var.mwaa_dag_s3_prefix}/.airflowignore"
  content = join("\n", [
    "etl/contracts/*",
    "etl/orchestration/*",
    "etl/publication/*",
    "etl/sources/*",
    ""
  ])
  content_type = "text/plain"
}

resource "aws_s3_object" "mwaa_requirements" {
  bucket = aws_s3_bucket.lakehouse.id
  key    = "${var.mwaa_dag_s3_prefix}/requirements.txt"
  source = "${path.module}/../requirements-airflow.txt"
  etag   = filemd5("${path.module}/../requirements-airflow.txt")
}

resource "aws_security_group" "mwaa" {
  name        = "${local.mwaa_environment_name}-mwaa"
  description = "Self-referencing MWAA traffic and controlled outbound access."
  vpc_id      = var.vpc_id

  ingress {
    description = "MWAA components communicate with each other."
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  egress {
    description = "AWS APIs, package installation, and Redshift access through private subnet routing."
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_mwaa_environment" "orchestration" {
  name               = local.mwaa_environment_name
  airflow_version    = "3.2.1"
  environment_class  = var.mwaa_environment_class
  execution_role_arn = aws_iam_role.mwaa_execution.arn
  source_bucket_arn  = aws_s3_bucket.lakehouse.arn
  dag_s3_path        = var.mwaa_dag_s3_prefix

  requirements_s3_path           = aws_s3_object.mwaa_requirements.key
  requirements_s3_object_version = aws_s3_object.mwaa_requirements.version_id
  min_workers                    = 1
  max_workers                    = var.mwaa_max_workers
  schedulers                     = 2
  webserver_access_mode          = "PUBLIC_AND_PRIVATE"
  endpoint_management            = "SERVICE"

  airflow_configuration_options = {
    "core.load_examples" = "False"
  }

  network_configuration {
    security_group_ids = [aws_security_group.mwaa.id]
    subnet_ids         = var.private_subnet_ids
  }

  logging_configuration {
    dag_processing_logs {
      enabled   = true
      log_level = "INFO"
    }
    scheduler_logs {
      enabled   = true
      log_level = "INFO"
    }
    task_logs {
      enabled   = true
      log_level = "INFO"
    }
    webserver_logs {
      enabled   = true
      log_level = "INFO"
    }
    worker_logs {
      enabled   = true
      log_level = "INFO"
    }
  }

  depends_on = [
    aws_iam_role_policy.mwaa_platform,
    aws_iam_role_policy.mwaa_pipeline,
    aws_s3_bucket_versioning.lakehouse,
    aws_s3_object.mwaa_source,
  ]
}
