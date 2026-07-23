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
    'aws_glue_job.initialize',
    'aws_glue_job.bronze',
    'aws_glue_job.great_expectations',
    'aws_glue_job.silver',
    'aws_glue_job.quality',
    'aws_glue_job.publication',
    'aws_iam_role_policy.glue_lakehouse',
    'aws_iam_role_policy_attachment.glue_service',
    'aws_iam_role.glue_service'
)
$arguments = @('plan', '-destroy', "-out=$PlanPath")
$arguments += $targets | ForEach-Object { "-target=$_" }
Push-Location $TerraformDirectory
terraform @arguments
Pop-Location
Write-Output 'Bounded destroy plan generated for review; no apply command was run.'
Write-Output 'The S3 bucket, Glue databases, and canonical Iceberg data are intentionally excluded.'
Write-Output 'After a separately approved apply, run scripts/verify_teardown.py.'
