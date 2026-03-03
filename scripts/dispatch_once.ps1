# scripts/dispatch_once.ps1
# Run ONE TNS dispatch pass with robust exit code + logs.
# - Writes:
#   - dispatch.last.out.log / dispatch.last.err.log (overwrite each run)
#   - dispatch.out.log / dispatch.err.log (append)
# - Guarantees exit code is captured (no "unknown").

[CmdletBinding()]
param(
  # Keep your interpreter. Default set to what your morning_check prints.
  [string]$PythonExe = "C:\Users\Tricejer\Desktop\FirstlightTest\.venv\Scripts\python.exe",

  # Repo-relative defaults (safe if you run from repo root OR from Task Scheduler)
  [string]$RepoRoot = (Split-Path -Parent $PSScriptRoot),
  [string]$EnvPath  = "",
  [string]$DbPath   = "",
  [string]$LogDir   = "",

  [double]$SinceHours = 24.0,
  [int]$MaxSubmit = 3,
  [string]$Topic = "",

  [switch]$SkipReply = $true,
  [int]$WaitS = 60,
  [int]$PollS = 5
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function NowStamp() {
  return (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
}

function Ensure-Dir([string]$p) {
  if (-not (Test-Path -LiteralPath $p)) { New-Item -ItemType Directory -Path $p | Out-Null }
}

try {
  if ([string]::IsNullOrWhiteSpace($EnvPath)) { $EnvPath = Join-Path $RepoRoot ".env" }
  if ([string]::IsNullOrWhiteSpace($DbPath))  { $DbPath  = Join-Path $RepoRoot "firstlight.sqlite" }
  if ([string]::IsNullOrWhiteSpace($LogDir))  { $LogDir  = Join-Path $RepoRoot "alertDB\logs\_last" }

  Ensure-Dir $LogDir

  $lastOut = Join-Path $LogDir "dispatch.last.out.log"
  $lastErr = Join-Path $LogDir "dispatch.last.err.log"
  $outLog  = Join-Path $LogDir "dispatch.out.log"
  $errLog  = Join-Path $LogDir "dispatch.err.log"

  # Header line in err log (like your historic "run started" lines)
  if (-not (Test-Path -LiteralPath $errLog)) {
    Add-Content -LiteralPath $errLog -Value ("[{0}] run started" -f (NowStamp))
  }

  $topicDisp = $Topic
  if ([string]::IsNullOrWhiteSpace($topicDisp)) { $topicDisp = "" }

  $skipDisp = $SkipReply.IsPresent
  Add-Content -LiteralPath $outLog -Value ("[{0}] dispatch start (since_hours={1} max_submit={2} skip_reply={3} topic={4})" -f (NowStamp), $SinceHours, $MaxSubmit, $skipDisp, $topicDisp)

  # Build args for python -m firstlight ... tns dispatch ...
  $args = New-Object System.Collections.Generic.List[string]
  $args.Add("-m"); $args.Add("firstlight")
  $args.Add("--env"); $args.Add($EnvPath)
  $args.Add("tns"); $args.Add("dispatch")
  $args.Add("--db"); $args.Add($DbPath)
  $args.Add("--since-hours"); $args.Add([string]$SinceHours)
  $args.Add("--max-submit");  $args.Add([string]$MaxSubmit)
  $args.Add("--wait-s");      $args.Add([string]$WaitS)
  $args.Add("--poll-s");      $args.Add([string]$PollS)

  if ($SkipReply.IsPresent) { $args.Add("--skip-reply") }

  if (-not [string]::IsNullOrWhiteSpace($Topic)) {
    $args.Add("--topic"); $args.Add($Topic)
  }

  # Run and capture logs; IMPORTANT: capture real exit code from external program
  $t0 = Get-Date
  $global:LASTEXITCODE = 0

  try {
    & $PythonExe @args 1> $lastOut 2> $lastErr
  } catch {
    # If the process couldn't even start, treat as hard failure
    $msg = $_.Exception.Message
    Set-Content -LiteralPath $lastErr -Value ("dispatch_once.ps1: failed to start python: {0}" -f $msg)
    $global:LASTEXITCODE = 900
  }

  $exitCode = $global:LASTEXITCODE
  if ($null -eq $exitCode) { $exitCode = 901 }

  $dt = (Get-Date) - $t0
  $ms = [int]$dt.TotalMilliseconds

  # Pull the "done:" line if present to enrich the summary line
  $doneLine = ""
  if (Test-Path -LiteralPath $lastOut) {
    $done = Select-String -LiteralPath $lastOut -Pattern "^done:" -ErrorAction SilentlyContinue | Select-Object -Last 1
    if ($done -and $done.Line) { $doneLine = $done.Line.Trim() }
  }

  Add-Content -LiteralPath $outLog -Value ("[{0}] dispatch end exit={1} elapsed_ms={2} {3}" -f (NowStamp), $exitCode, $ms, $doneLine)

  # If stderr has content OR exit != 0, append a short tail to dispatch.err.log
  $errBytes = 0
  if (Test-Path -LiteralPath $lastErr) { $errBytes = (Get-Item -LiteralPath $lastErr).Length }

  if (($exitCode -ne 0) -or ($errBytes -gt 0)) {
    Add-Content -LiteralPath $errLog -Value ("[{0}] dispatch issue exit={1} stderr_bytes={2}" -f (NowStamp), $exitCode, $errBytes)

    if ($errBytes -gt 0) {
      $tail = Get-Content -LiteralPath $lastErr -ErrorAction SilentlyContinue | Select-Object -Last 80
      foreach ($line in $tail) {
        Add-Content -LiteralPath $errLog -Value ("    {0}" -f $line)
      }
    }
  }

  exit $exitCode
}
catch {
  # Fail-safe: log and exit non-zero
  $msg = $_.Exception.Message
  try {
    if (-not [string]::IsNullOrWhiteSpace($LogDir)) {
      $errLog2 = Join-Path $LogDir "dispatch.err.log"
      Add-Content -LiteralPath $errLog2 -Value ("[{0}] dispatch_once.ps1 fatal: {1}" -f (NowStamp), $msg)
    }
  } catch {}
  exit 999
}