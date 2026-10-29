$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

$env:JAVA_HOME = "C:\Program Files\Microsoft\jdk-21.0.12.101-hotspot"
$env:Path = "$env:JAVA_HOME\bin;$env:Path"

$env:PYSPARK_PYTHON = "$projectRoot\env\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON = $env:PYSPARK_PYTHON

$credentialPath = "$env:LOCALAPPDATA\Marketflow\postgres.xml"

if (-not (Test-Path $credentialPath)) {
    throw "Saved PostgreSQL credential not found."
}

$databaseCredential = Import-Clixml $credentialPath
$env:CRYPTO_DB_PASSWORD = $databaseCredential.GetNetworkCredential().Password
Remove-Variable databaseCredential

& "$projectRoot\env\Scripts\spark-submit.cmd" `
    --packages org.postgresql:postgresql:42.7.4 `
    "$projectRoot\spark_streaming\analytics.py"

exit $LASTEXITCODE