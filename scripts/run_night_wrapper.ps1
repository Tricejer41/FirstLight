$ErrorActionPreference = "Stop"

$repo = Split-Path -Parent $PSScriptRoot
Set-Location $repo

$logsDir = Join-Path $repo "alertDB\logs\scheduler"
New-Item -ItemType Directory -Force $logsDir | Out-Null

$ts = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"
$log = Join-Path $logsDir ("night_" + $ts + ".log")

"START $ts" | Out-File -FilePath $log -Encoding utf8
try {
  powershell -NoProfile -ExecutionPolicy Bypass -File scripts\run_night.ps1 -MaxHours 13 *>> $log
} catch {
  "ERROR: $($_.Exception.Message)" | Out-File -FilePath $log -Append -Encoding utf8
  throw
} finally {
  "END $(Get-Date -Format "yyyy-MM-dd_HH-mm-ss")" | Out-File -FilePath $log -Append -Encoding utf8
}
