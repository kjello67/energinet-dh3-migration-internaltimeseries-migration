$version = "1.0.1"
$now = Get-Date -UFormat "%d-%m-%Y_%T"
$sha1 = (git rev-parse HEAD).Trim()

go build -ldflags "-X main.sha1ver=$sha1 -X main.version=$version -X main.buildTime=$now"

Write-Host ".\timeseries-migration.exe -version : print version to stdout and exit"
