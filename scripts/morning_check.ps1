# scripts/morning_check.ps1
# Quick morning health + stats for FirstLight sandbox runs
# Usage (examples):
#   powershell -ExecutionPolicy Bypass -File .\scripts\morning_check.ps1
#   powershell -ExecutionPolicy Bypass -File .\scripts\morning_check.ps1 -Db .\firstlight.sqlite -LogsRoot .\alertDB\logs -RawRoot .\alertDB\raw

param(
  [string]$Db = ".\firstlight.sqlite",
  [string]$LogsRoot = ".\alertDB\logs",
  [string]$RawRoot  = ".\alertDB\raw",
  [int]$Tail = 25
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Resolve-Abs([string]$p) {
  return (Resolve-Path $p).Path
}

function Get-LatestRunDir([string]$root) {
  if (-not (Test-Path $root)) { return $null }
  $d = Get-ChildItem $root -Directory -ErrorAction SilentlyContinue |
       Sort-Object LastWriteTime -Descending |
       Select-Object -First 1
  return $d
}

function Print-Section([string]$title) {
  Write-Host ""
  Write-Host ("=" * 78)
  Write-Host $title
  Write-Host ("=" * 78)
}

# --- Resolve paths ---
$dbAbs = $Db
if (Test-Path $Db) { $dbAbs = Resolve-Abs $Db }

$logsAbs = $LogsRoot
if (Test-Path $LogsRoot) { $logsAbs = Resolve-Abs $LogsRoot }

$rawAbs = $RawRoot
if (Test-Path $RawRoot) { $rawAbs = Resolve-Abs $RawRoot }

Print-Section "Paths"
Write-Host ("DB      : {0}" -f $dbAbs)
Write-Host ("LogsRoot: {0}" -f $logsAbs)
Write-Host ("RawRoot : {0}" -f $rawAbs)

# --- Latest run dir ---
$lastLogDir = Get-LatestRunDir $logsAbs
Print-Section "Latest log run"
if (-not $lastLogDir) {
  Write-Host "No log directories found."
  exit 1
}
Write-Host ("Last log dir: {0} (LastWriteTime={1})" -f $lastLogDir.FullName, $lastLogDir.LastWriteTime)

# Show stop reason if present
$stopReason = Join-Path $lastLogDir.FullName "_stop_reason.txt"
if (Test-Path $stopReason) {
  Write-Host ("Stop reason: {0}" -f (Get-Content $stopReason -Raw).Trim())
} else {
  Write-Host "Stop reason: <missing>"
}

# --- Raw run dir from _run.meta.txt if present ---
$meta = Join-Path $lastLogDir.FullName "_run.meta.txt"
$rawDir = $null
if (Test-Path $meta) {
  $metaText = Get-Content $meta -ErrorAction SilentlyContinue
  $rawLine = $metaText | Where-Object { $_ -match '^RAW=' } | Select-Object -First 1
  if ($rawLine) {
    $rawDir = $rawLine.Split("=",2)[1].Trim()
  }
}
if (-not $rawDir) {
  # fallback: guess latest raw dir
  $lastRawDir = Get-LatestRunDir $rawAbs
  if ($lastRawDir) { $rawDir = $lastRawDir.FullName }
}

Print-Section "Raw ingestion"
if ($rawDir -and (Test-Path $rawDir)) {
  Write-Host ("Raw dir: {0}" -f $rawDir)
  $avroCount = (Get-ChildItem $rawDir -Filter "*.avro" -ErrorAction SilentlyContinue | Measure-Object).Count
  Write-Host ("Avros in raw dir: {0}" -f $avroCount)
  $seenPath = Join-Path $rawDir ".replay_seen.txt"
  if (Test-Path $seenPath) {
    $seenCount = (Get-Content $seenPath -ErrorAction SilentlyContinue | Measure-Object).Count
    Write-Host ("Seen markers (.replay_seen.txt lines): {0}" -f $seenCount)
  } else {
    Write-Host "Seen markers: <missing .replay_seen.txt>"
  }
} else {
  Write-Host "Raw dir not found."
}

# --- Logs sizes ---
Print-Section "Log files (sizes)"
Get-ChildItem $lastLogDir.FullName -File |
  Sort-Object Name |
  Select-Object Name, Length, LastWriteTime |
  Format-Table -AutoSize

# --- Dispatch tail (if any) ---
$dispatchOut = Join-Path $lastLogDir.FullName "dispatch.out.log"
$dispatchErr = Join-Path $lastLogDir.FullName "dispatch.err.log"
Print-Section "Dispatch tail"
if (Test-Path $dispatchErr) {
  $errBytes = (Get-Item $dispatchErr).Length
  Write-Host ("dispatch.err.log bytes: {0}" -f $errBytes)
  if ($errBytes -gt 0) {
    Write-Host "--- dispatch.err.log (tail) ---"
    Get-Content $dispatchErr -Tail $Tail
  }
}
if (Test-Path $dispatchOut) {
  $outBytes = (Get-Item $dispatchOut).Length
  Write-Host ("dispatch.out.log bytes: {0}" -f $outBytes)
  if ($outBytes -gt 0) {
    Write-Host "--- dispatch.out.log (tail) ---"
    Get-Content $dispatchOut -Tail $Tail
  }
}

# --- Replay reasons from replay.jsonl if present ---
$replayJsonl = Join-Path $lastLogDir.FullName "replay.jsonl"
Print-Section "Replay reasons (top)"
if (Test-Path $replayJsonl) {
  python -c "import json,collections,pathlib; p=r'$replayJsonl'; c=collections.Counter(); 
for ln in pathlib.Path(p).read_text(encoding='utf-8').splitlines():
  o=json.loads(ln)
  r=o.get('reason')
  if r: c[r]+=1
print('reasons_top=', c.most_common(10))"
} else {
  Write-Host "No replay.jsonl in latest run."
}

# --- DB counts + last 24h deltas ---
Print-Section "DB counts"
if (-not (Test-Path $dbAbs)) {
  Write-Host "DB not found."
  exit 1
}

python -c "import sqlite3,datetime; 
db=r'$dbAbs'; con=sqlite3.connect(db); 
def cnt(q,params=()): 
  try: return con.execute(q,params).fetchone()[0]
  except Exception: return None
print('DB=',db)
print('alerts=', cnt('select count(*) from alerts'))
print('decisions=', cnt('select count(*) from decisions'))
print('tns_actions=', cnt('select count(*) from tns_actions'))
# 24h window based on created_utc string ISO; best-effort
cut=(datetime.datetime.utcnow()-datetime.timedelta(hours=24)).replace(microsecond=0).isoformat()
print('cut_utc_24h=',cut)
print('alerts_24h=', cnt('select count(*) from alerts where created_utc>=?', (cut,)))
print('decisions_24h=', cnt('select count(*) from decisions where created_utc>=?', (cut,)))
print('passed_24h=', cnt('select count(*) from decisions where created_utc>=? and passed=1', (cut,)))
print('tns_actions_24h=', cnt('select count(*) from tns_actions where created_utc>=?', (cut,)))"
