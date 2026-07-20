resource "aws_athena_workgroup" "gold_query" {
  name          = "${var.project_name}-${var.environment}-gold"
  force_destroy = false

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.athena_bytes_scanned_cutoff

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      output_location = "s3://${aws_s3_bucket.lakehouse.id}/${var.athena_results_prefix}/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

output "athena_workgroup_name" {
  value       = aws_athena_workgroup.gold_query.name
  description = "Bounded read-only Gold Athena workgroup."
}

output "athena_results_prefix" {
  value       = var.athena_results_prefix
  description = "Result prefix in the canonical project bucket."
}
