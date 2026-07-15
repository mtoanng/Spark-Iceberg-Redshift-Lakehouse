param(
    [string]$TerraformDir = "terraform",
    [string]$OutputFile = ".env.terraform"
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $TerraformDir)) {
    throw "Terraform directory not found: $TerraformDir"
}

Push-Location $TerraformDir
try {
    $json = terraform output -json | ConvertFrom-Json
}
finally {
    Pop-Location
}

if (-not $json) {
    throw "No Terraform outputs found. Run 'terraform apply' first."
}

$values = [ordered]@{
    AWS_ACCOUNT_ID      = $json.aws_account_id.value
    AWS_REGION          = $json.aws_region.value
    S3_BUCKET           = $json.s3_bucket_name.value
    S3_RAW_PREFIX       = $json.s3_raw_prefix.value
    S3_WAREHOUSE_PREFIX = "warehouse"
    S3_GOLD_PATH        = $json.s3_gold_path.value
    GLUE_DATABASE       = $json.glue_database_name.value
    GLUE_ROLE_ARN       = $json.glue_role_arn.value
    DUCKDB_ROLE_ARN     = $json.duckdb_role_arn.value
}

$lines = @(
    "# Generated from Terraform outputs."
    "# Do not put AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY in this file."
)

foreach ($key in $values.Keys) {
    if ($null -ne $values[$key] -and "" -ne $values[$key]) {
        $lines += "$key=$($values[$key])"
    }
}

Set-Content -Path $OutputFile -Value $lines -Encoding UTF8
Write-Host "Wrote $OutputFile"
