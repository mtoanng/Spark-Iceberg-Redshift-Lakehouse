param(
    [int]$Year = 2024,
    [int]$Month = 1,
    [switch]$Force
)
$ErrorActionPreference = 'Stop'
python scripts/package_glue_jobs.py --output build/nyc_glue_jobs.zip --check
$arguments = @('-m', 'scripts.run_e2e', '--year', $Year, '--month', $Month, '--smoke')
if ($Force) { $arguments += '--force' }
python @arguments
Write-Output 'Smoke command plan generated; remote execution remains deferred.'
