param(
    [string]$TerraformDirectory = 'terraform',
    [string]$PlanPath = 'bounded-destroy.tfplan'
)
$ErrorActionPreference = 'Stop'
$targets = @(
    'aws_instance.airflow_runner',
    'aws_iam_instance_profile.airflow_runner',
    'aws_iam_role_policy_attachment.airflow_ssm',
    'aws_iam_role_policy.airflow_runner_access',
    'aws_iam_role_policy.athena_gold_query',
    'aws_iam_role.airflow_runner',
    'aws_athena_workgroup.gold_query',
    'aws_emrserverless_application.spark',
    'aws_iam_role_policy.emr_serverless_lakehouse',
    'aws_iam_role.emr_serverless_execution'
)
$arguments = @('plan', '-destroy', "-out=$PlanPath")
$arguments += $targets | ForEach-Object { "-target=$_" }
Push-Location $TerraformDirectory
terraform @arguments
Pop-Location
Write-Output 'Bounded destroy plan generated for review; no apply command was run.'
Write-Output 'The S3 bucket, Glue databases, and canonical Iceberg data are intentionally excluded.'
Write-Output 'After a separately approved apply, run scripts/verify_teardown.py.'
