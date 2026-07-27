param(
    [int]$Year = 2024,
    [int]$FirstMonth = 1,
    [switch]$Force
)
$ErrorActionPreference = 'Stop'
python scripts/package_spark_jobs.py --output build/nyc_spark_jobs.zip --check
$arguments = @('-m', 'scripts.run_e2e', '--year', $Year, '--month', $FirstMonth)
if ($Force) { $arguments += '--force' }
python @arguments
Write-Output 'Four-month release command plan generated; remote execution remains deferred.'
