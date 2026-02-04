param(
  [double]$MaxHours = 13
)

$ErrorActionPreference = "Stop"

# Repo root
$repo = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $repo

# Logs en ProgramData
$base = "C:\ProgramData\FirstLight"
$logsDir = Join-Path $base "logs\scheduler"
New-Item -ItemType Directory -Force -Path $logsDir | Out-Null

$ts = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"
$log = Join-Path $logsDir ("night_" + $ts + ".log")
$outLog = Join-Path $logsDir ("night_" + $ts + ".out.log")
$errLog = Join-Path $logsDir ("night_" + $ts + ".err.log")

$ps = Join-Path $env:WINDIR "System32\WindowsPowerShell\v1.0\powershell.exe"
$night = Join-Path $repo "scripts\run_night.ps1"

# Ajusta a tu venv real
$pythonExe = "C:\Users\Tricejer\Desktop\FirstlightTest\.venv\Scripts\python.exe"
$finkExe   = "C:\Users\Tricejer\Desktop\FirstlightTest\.venv\Scripts\fink_consumer.exe"

"START $ts" | Out-File -FilePath $log -Encoding utf8
"repo=$repo" | Out-File -FilePath $log -Append -Encoding utf8
"ps=$ps" | Out-File -FilePath $log -Append -Encoding utf8
"night=$night" | Out-File -FilePath $log -Append -Encoding utf8
"MaxHours=$MaxHours" | Out-File -FilePath $log -Append -Encoding utf8
"PythonExe=$pythonExe" | Out-File -FilePath $log -Append -Encoding utf8
"FinkConsumerExe=$finkExe" | Out-File -FilePath $log -Append -Encoding utf8

try {
  if (-not (Test-Path $ps)) { throw "powershell.exe not found at: $ps" }
  if (-not (Test-Path $night)) { throw "run_night.ps1 not found at: $night" }
  if (-not (Test-Path $pythonExe)) { throw "python.exe not found at: $pythonExe" }
  if (-not (Test-Path $finkExe)) { throw "fink_consumer.exe not found at: $finkExe" }

  $maxHoursStr = [string]::Format([System.Globalization.CultureInfo]::InvariantCulture, "{0}", $MaxHours)

  $args = @(
    "-NoProfile",
    "-ExecutionPolicy", "Bypass",
    "-File", $night,
    "-EnvFile", (Join-Path $repo ".env"),
    "-MaxHours", $maxHoursStr,
    "-PythonExe", $pythonExe,
    "-FinkConsumerExe", $finkExe
  )

  "Launching: $ps $($args -join ' ')" | Out-File -FilePath $log -Append -Encoding utf8

  $p = Start-Process -FilePath $ps -ArgumentList $args -WorkingDirectory $repo -NoNewWindow -PassThru -Wait `
    -RedirectStandardOutput $outLog -RedirectStandardError $errLog

  $exitCode = $p.ExitCode
  "ExitCode=$exitCode" | Out-File -FilePath $log -Append -Encoding utf8

  if ($exitCode -ne 0) {
    throw "run_night.ps1 failed with ExitCode=$exitCode. See: $outLog and $errLog"
  }

} catch {
  "ERROR: $($_.Exception.Message)" | Out-File -FilePath $log -Append -Encoding utf8
  throw
} finally {
  "END $(Get-Date -Format 'yyyy-MM-dd_HH-mm-ss')" | Out-File -FilePath $log -Append -Encoding utf8
}
