param(
  [Parameter(Mandatory=$false)]
  [double]$MaxHours = 13,

  [Parameter(Mandatory=$false)]
  [string]$EnvFile = ".env",

  [Parameter(Mandatory=$false)]
  [string]$RepoRoot = "",

  [Parameter(Mandatory=$false)]
  [string]$PythonExe = "",

  [Parameter(Mandatory=$false)]
  [string]$FinkConsumerExe = "",

  [Parameter(Mandatory=$false)]
  [string]$CfgPath = ".\config\n1.example.yaml",

  [Parameter(Mandatory=$false)]
  [string]$DbPath = ".\firstlight.sqlite",

  # Cada cuántos segundos relanzar dispatch (en lugar de --every-s)
  [Parameter(Mandatory=$false)]
  [int]$DispatchEveryS = 60,

  # Cada cuánto comprobar si siguen vivos consumer/replay
  [Parameter(Mandatory=$false)]
  [int]$WatchdogS = 2
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Resolve-PathFlex([string]$base, [string]$p) {
  if ([string]::IsNullOrWhiteSpace($p)) { throw "Empty path" }
  if ([System.IO.Path]::IsPathRooted($p)) {
    return (Resolve-Path $p).Path
  } else {
    return (Resolve-Path (Join-Path $base $p)).Path
  }
}

function Load-DotEnv([string]$path) {
  if (-not (Test-Path $path)) { throw ".env not found: $path" }
  $lines = Get-Content -LiteralPath $path -ErrorAction Stop
  foreach ($ln in $lines) {
    $s = $ln.Trim()
    if ($s.Length -eq 0) { continue }
    if ($s.StartsWith("#")) { continue }
    $idx = $s.IndexOf("=")
    if ($idx -lt 1) { continue }
    $k = $s.Substring(0,$idx).Trim()
    $v = $s.Substring($idx+1).Trim()

    if ($v.StartsWith('"') -and $v.EndsWith('"') -and $v.Length -ge 2) { $v = $v.Substring(1,$v.Length-2) }
    if ($v.StartsWith("'") -and $v.EndsWith("'") -and $v.Length -ge 2) { $v = $v.Substring(1,$v.Length-2) }

    [System.Environment]::SetEnvironmentVariable($k, $v, "Process")
  }
}

function Proc-Status([System.Diagnostics.Process]$p, [string]$label) {
  if (-not $p) { return ("{0} - <null>" -f $label) }
  $p.Refresh()
  if ($p.HasExited) { return ("{0} - EXITED code={1}" -f $label, $p.ExitCode) }
  return ("{0} - RUNNING pid={1}" -f $label, $p.Id)
}

# Repo root default: parent folder of this script
if ([string]::IsNullOrWhiteSpace($RepoRoot)) {
  $RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
} else {
  $RepoRoot = (Resolve-Path $RepoRoot).Path
}

$envPath = Resolve-PathFlex $RepoRoot $EnvFile
$cfgAbs  = Resolve-PathFlex $RepoRoot $CfgPath
$dbAbs   = Resolve-PathFlex $RepoRoot $DbPath

if ([string]::IsNullOrWhiteSpace($PythonExe)) {
  $PythonExe = (Get-Command python -ErrorAction Stop).Source
} else {
  $PythonExe = Resolve-PathFlex $RepoRoot $PythonExe
}
if ([string]::IsNullOrWhiteSpace($FinkConsumerExe)) {
  $FinkConsumerExe = (Get-Command fink_consumer -ErrorAction Stop).Source
} else {
  $FinkConsumerExe = Resolve-PathFlex $RepoRoot $FinkConsumerExe
}

Load-DotEnv $envPath

$run = (Get-Date -Format "yyyy-MM-dd_HH-mm-ss")
$rawDir = Join-Path $RepoRoot ("alertDB\raw\" + $run)
$logDir = Join-Path $RepoRoot ("alertDB\logs\" + $run)
New-Item -ItemType Directory -Force -Path $rawDir | Out-Null
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

$consumerOut = Join-Path $logDir "consumer.out.log"
$consumerErr = Join-Path $logDir "consumer.err.log"
$replayOut   = Join-Path $logDir "replay.out.log"
$replayErr   = Join-Path $logDir "replay.err.log"
$dispatchOut = Join-Path $logDir "dispatch.out.log"
$dispatchErr = Join-Path $logDir "dispatch.err.log"
$replayJsonl = Join-Path $logDir "replay.jsonl"
$meta        = Join-Path $logDir "_run.meta.txt"
$stopReason  = Join-Path $logDir "_stop_reason.txt"

"RUN=$run"            | Out-File -FilePath $meta -Encoding utf8
"RAW=$rawDir"         | Add-Content -Path $meta -Encoding utf8
"LOG=$logDir"         | Add-Content -Path $meta -Encoding utf8
"DB=$dbAbs"           | Add-Content -Path $meta -Encoding utf8
"CFG=$cfgAbs"         | Add-Content -Path $meta -Encoding utf8
"ENV=$envPath"        | Add-Content -Path $meta -Encoding utf8
"MaxHours=$MaxHours"  | Add-Content -Path $meta -Encoding utf8
"DispatchEveryS=$DispatchEveryS" | Add-Content -Path $meta -Encoding utf8
"PythonExe=$PythonExe" | Add-Content -Path $meta -Encoding utf8
"FinkConsumerExe=$FinkConsumerExe" | Add-Content -Path $meta -Encoding utf8

# Start consumer
$consumer = Start-Process -FilePath $FinkConsumerExe `
  -ArgumentList @("--save","-outdir",$rawDir,"-limit","0") `
  -NoNewWindow -PassThru `
  -RedirectStandardOutput $consumerOut -RedirectStandardError $consumerErr

# Start replay follower
$replay = Start-Process -FilePath $PythonExe `
  -ArgumentList @((Join-Path $RepoRoot "scripts\replay_avro_dir.py"), $rawDir, "--cfg", $cfgAbs, "--db", $dbAbs, "--follow", "--poll-s", "2", "--print-every", "200", "--jsonl", $replayJsonl) `
  -NoNewWindow -PassThru `
  -RedirectStandardOutput $replayOut -RedirectStandardError $replayErr

$until = (Get-Date).AddHours($MaxHours)
Write-Host ("Running until {0}" -f $until)

# Dispatch loop (no --every-s)
$nextDispatch = Get-Date
$firstExitText = $null

while ((Get-Date) -lt $until) {

  # watchdog: si consumer o replay mueren, paramos
  $consumer.Refresh()
  $replay.Refresh()
  if ($consumer.HasExited -or $replay.HasExited) {
    $firstExitText = @(
      "EARLY STOP: a process exited",
      (Proc-Status $consumer "consumer"),
      (Proc-Status $replay   "replay")
    ) -join "`n"
    break
  }

  # lanzar un dispatch cada X segundos
  if ((Get-Date) -ge $nextDispatch) {
    $dispatch = Start-Process -FilePath $PythonExe `
      -ArgumentList @("-m","firstlight","--env",$envPath,"tns","dispatch-sandbox","--db",$dbAbs,"--since-hours","13","--max-submit","3","--wait-s","600") `
      -NoNewWindow -PassThru `
      -RedirectStandardOutput $dispatchOut -RedirectStandardError $dispatchErr

    # Espera a que termine el dispatch (es normal que dure poco)
    try {
      $dispatch.WaitForExit()
    } catch {}

    $nextDispatch = (Get-Date).AddSeconds([Math]::Max(5,$DispatchEveryS))
  }

  Start-Sleep -Seconds ([Math]::Max(1,$WatchdogS))
}

if ($firstExitText) {
  $firstExitText | Out-File $stopReason -Encoding utf8
  Write-Host $firstExitText
} else {
  ("TIME LIMIT reached ({0})" -f $until) | Out-File $stopReason -Encoding utf8
}

Write-Host "Stopping processes..."
foreach ($p in @($replay,$consumer)) {
  try {
    if ($p) { $p.Refresh() }
    if ($p -and -not $p.HasExited) { Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue }
  } catch {}
}
Write-Host "Done."
