# scripts/sweep_replies_once.ps1
# Run ONE TNS sweep-replies pass with logs.

[CmdletBinding()]
param(
  # Keep your interpreter
  [string]$PythonExe = "C:\Users\Tricejer\Desktop\FirstlightTest\.venv\Scripts\python.exe",

  [string]$RepoRoot = (Split-Path -Parent $PSScriptRoot),
  [string]$EnvPath  = "",
  [string]$DbPath   = "",
  [string]$LogDir   = "",

  [double]$SinceHours = 24.0,
  [int]$Max = 50
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

  $lastOut = Join-Path $LogDir "replies.last.out.log"
  $lastErr = Join-Path $LogDir "replies.last.err.log"
  $outLog  = Join-Path $LogDir "replies.out.log"
  $errLog  = Join-Path $LogDir "replies.err.log"

  Add-Content -LiteralPath $outLog -Value ("[{0}] sweep-replies start (since_hours={1} max={2})" -f (NowStamp), $SinceHours, $Max)

  $args = New-Object System.Collections.Generic.List[string]
  $args.Add("-m"); $args.Add("firstlight")
  $args.Add("--env"); $args.Add($EnvPath)
  $args.Add("tns"); $args.Add("sweep-replies")
  $args.Add("--db"); $args.Add($DbPath)
  $args.Add("--since-hours"); $args.Add([string]$SinceHours)
  $args.Add("--max"); $args.Add([string]$Max)

  $t0 = Get-Date
  $global:LASTEXITCODE = 0

  try {
    & $PythonExe @args 1> $lastOut 2> $lastErr
  } catch {
    $msg = $_.Exception.Message
    Set-Content -LiteralPath $lastErr -Value ("sweep_replies_once.ps1: failed to start python: {0}" -f $msg)
    $global:LASTEXITCODE = 900
  }

  $exitCode = $global:LASTEXITCODE
  if ($null -eq $exitCode) { $exitCode = 901 }

  $dt = (Get-Date) - $t0
  $ms = [int]$dt.TotalMilliseconds

  Add-Content -LiteralPath $outLog -Value ("[{0}] sweep-replies end exit={1} elapsed_ms={2}" -f (NowStamp), $exitCode, $ms)

  $errBytes = 0
  if (Test-Path -LiteralPath $lastErr) { $errBytes = (Get-Item -LiteralPath $lastErr).Length }

  if (($exitCode -ne 0) -or ($errBytes -gt 0)) {
    Add-Content -LiteralPath $errLog -Value ("[{0}] sweep-replies issue exit={1} stderr_bytes={2}" -f (NowStamp), $exitCode, $errBytes)
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
  try {
    $msg = $_.Exception.Message
    if (-not [string]::IsNullOrWhiteSpace($LogDir)) {
      $errLog2 = Join-Path $LogDir "replies.err.log"
      Add-Content -LiteralPath $errLog2 -Value ("[{0}] sweep_replies_once.ps1 fatal: {1}" -f (NowStamp), $msg)
    }
  } catch {}
  exit 999
}