resource "aws_instance" "airflow_runner" {
  count = var.airflow_runner_ami_id == "" ? 0 : 1

  ami                         = var.airflow_runner_ami_id
  instance_type               = var.airflow_runner_instance_type
  subnet_id                   = var.airflow_runner_subnet_id
  key_name                    = var.airflow_runner_key_name == "" ? null : var.airflow_runner_key_name
  iam_instance_profile        = aws_iam_instance_profile.airflow_runner[0].name
  associate_public_ip_address = false
  monitoring                  = false

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }

  user_data = file("${path.module}/../scripts/bootstrap_airflow_runner.sh")

  tags = { Name = "${var.project_name}-${var.environment}-airflow-runner" }
}
