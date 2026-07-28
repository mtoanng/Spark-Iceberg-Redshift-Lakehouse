param(
    [string]$TerraformDirectory = 'terraform',
    [string]$PlanPath = 'bounded-destroy.tfplan'
)
$ErrorActionPreference = 'Stop'

# Cost-bearing control/compute/serving resources only. The S3 bucket, Glue
# namespaces, and canonical Iceberg objects are deliberately retained.
$targets = @(
    'aws_mwaa_environment.orchestration',
    'aws_security_group.mwaa',
    'aws_iam_role_policy.mwaa_platform',
    'aws_iam_role_policy.mwaa_pipeline',
    'aws_iam_role_policy.athena_iceberg_verify',
    'aws_iam_role.mwaa_execution',
    'aws_athena_workgroup.iceberg_verify',
    'aws_emrserverless_application.spark',
    'aws_iam_role_policy.emr_serverless_lakehouse',
    'aws_iam_role.emr_serverless_execution',
    'aws_redshiftdata_statement.mwaa_bronze_usage',
    'aws_redshiftdata_statement.mwaa_silver_usage',
    'aws_redshiftdata_statement.mwaa_gold_owner',
    'aws_redshiftdata_statement.mwaa_temp_tables',
    'aws_redshiftdata_statement.mwaa_database_user',
    'aws_redshiftdata_statement.bronze_external_schema',
    'aws_redshiftdata_statement.silver_external_schema',
    'aws_redshiftdata_statement.gold_schema',
    'aws_redshiftserverless_workgroup.gold',
    'aws_redshiftserverless_namespace.gold',
    'aws_security_group.redshift',
    'aws_iam_role_policy.redshift_spectrum',
    'aws_iam_role.redshift_spectrum'
)
$arguments = @('plan', '-destroy', "-out=$PlanPath")
$arguments += $targets | ForEach-Object { "-target=$_" }
Push-Location $TerraformDirectory
terraform @arguments
Pop-Location
Write-Output 'Bounded destroy plan generated for review; no apply command was run.'
Write-Output 'S3, Glue, and canonical Iceberg data are intentionally excluded.'
Write-Output 'Apply requires separate approval; then run scripts/verify_teardown.py.'
