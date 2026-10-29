$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

& "$projectRoot\env\Scripts\python.exe" -u `
    "$projectRoot\Scripts\upload_to_s3.py" --upload

exit $LASTEXITCODE