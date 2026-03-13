# scripts/morning_check.ps1
# Quick morning health + stats for FirstLight sandbox runs
# Usage:
#   powershell -ExecutionPolicy Bypass -File .\scripts\morning_check.ps1
#   powershell -ExecutionPolicy Bypass -File .\scripts\morning_check.ps1 -Db .\firstlight.sqlite -LogsRoot .\alertDB\logs -RawRoot .\alertDB\raw
#   powershell -ExecutionPolicy Bypass -File .\scripts\morning_check.ps1 -PythonExe .\.venv\Scripts\python.exe

param(
  [string]$Db = ".\firstlight_prod.sqlite",
  [string]$LogsRoot = ".\alertDB\logs",
  [string]$RawRoot  = ".\alertDB\raw",
  [int]$Tail = 25,

  # How many recent tns_actions to print
  [int]$TnsTail = 20,

  # Duplicate threshold alert
  [int]$DupTopN = 10,

  # Hours window for "recent" stats (default 24h, but configurable)
  [int]$WindowHours = 24,

  # Python interpreter to use for inline scripts (avoid PATH drift)
  [string]$PythonExe = ""
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

# ---------------------------------------------------------------------------
# Python runner: temp .py file (stable quoting) + explicit interpreter
# ---------------------------------------------------------------------------
function Invoke-PythonTempScript {
  param(
    [Parameter(Mandatory=$true)][string] $Code,
    [Parameter(Mandatory=$false)][string[]] $Args = @(),
    [Parameter(Mandatory=$true)][string] $PyExe
  )

  $tmp = Join-Path $env:TEMP ("firstlight_inline_{0}.py" -f ([guid]::NewGuid().ToString("N")))
  try {
    # UTF-8 no BOM: avoids weird characters issues
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($tmp, $Code, $utf8NoBom)

    & $PyExe $tmp @Args
    if ($LASTEXITCODE -ne 0) {
      throw "Python exited with code $LASTEXITCODE"
    }
  }
  finally {
    Remove-Item -LiteralPath $tmp -ErrorAction SilentlyContinue
  }
}

# --- Resolve repo-root-ish python if not provided ---
if ([string]::IsNullOrWhiteSpace($PythonExe)) {
  # assume scripts/ is under repo root
  $repoRootGuess = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
  $venvPy = Join-Path $repoRootGuess ".venv\Scripts\python.exe"
  if (Test-Path $venvPy) {
    $PythonExe = (Resolve-Path $venvPy).Path
  } else {
    $PythonExe = (Get-Command python -ErrorAction Stop).Source
  }
} else {
  if (Test-Path $PythonExe) { $PythonExe = (Resolve-Abs $PythonExe) }
}

# --- Resolve paths ---
$dbAbs = $Db
if (Test-Path $Db) { $dbAbs = Resolve-Abs $Db }

$logsAbs = $LogsRoot
if (Test-Path $LogsRoot) { $logsAbs = Resolve-Abs $LogsRoot }

$rawAbs = $RawRoot
if (Test-Path $RawRoot) { $rawAbs = Resolve-Abs $RawRoot }

Print-Section "Paths"
Write-Host ("DB       : {0}" -f $dbAbs)
Write-Host ("LogsRoot : {0}" -f $logsAbs)
Write-Host ("RawRoot  : {0}" -f $rawAbs)
Write-Host ("PythonExe: {0}" -f $PythonExe)

# --- Latest run dir ---
$lastLogDir = Get-LatestRunDir $logsAbs
Print-Section "Latest log run"
if (-not $lastLogDir) {
  Write-Host "No log directories found."
  exit 1
}
Write-Host ("Last log dir: {0} (DirLastWriteTime={1})" -f $lastLogDir.FullName, $lastLogDir.LastWriteTime)

# Last activity inside dir
$lastActivity = Get-ChildItem $lastLogDir.FullName -File -ErrorAction SilentlyContinue |
  Sort-Object LastWriteTime -Descending |
  Select-Object -First 1
if ($lastActivity) {
  Write-Host ("Last activity inside dir: {0}" -f $lastActivity.LastWriteTime)
}

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
  $py = @"
import json, collections, pathlib, sys
p = sys.argv[1]
c = collections.Counter()
for ln in pathlib.Path(p).read_text(encoding='utf-8').splitlines():
    o = json.loads(ln)
    r = o.get('reason')
    if r:
        c[r] += 1
print('reasons_top=', c.most_common(10))
"@
  Invoke-PythonTempScript -Code $py -Args @($replayJsonl) -PyExe $PythonExe
} else {
  Write-Host "No replay.jsonl in latest run."
}

# --- DB counts + window deltas ---
Print-Section "DB counts"
if (-not (Test-Path $dbAbs)) {
  Write-Host "DB not found."
  exit 1
}

$py = @"
import sqlite3, sys
from datetime import datetime, timedelta, timezone

db = sys.argv[1]
hours = int(sys.argv[2])

con = sqlite3.connect(db)

def has_col(table, col):
    try:
        rows = con.execute(f"PRAGMA table_info({table})").fetchall()
        return any(r[1] == col for r in rows)
    except Exception:
        return False

def is_legacy_epoch(s):
    if not s:
        return True
    return str(s).startswith("1970-01-01T00:00:00")

def choose_decision_time_col():
    has_created = has_col("decisions","created_utc")
    has_decided = has_col("decisions","decided_utc")
    if has_created and not has_decided:
        return "created_utc"
    if has_decided and not has_created:
        return "decided_utc"
    if has_created and has_decided:
        row = con.execute("select min(created_utc), max(created_utc) from decisions").fetchone()
        mn, mx = row[0], row[1]
        if is_legacy_epoch(mn) and is_legacy_epoch(mx):
            return "decided_utc"
        return "created_utc"
    return "created_utc"

def cnt(q, params=()):
    try:
        return con.execute(q, params).fetchone()[0]
    except Exception:
        return None

tcol = choose_decision_time_col()

print("DB=", db)
print("alerts=", cnt("select count(*) from alerts"))
print("decisions=", cnt("select count(*) from decisions"))
print("tns_actions=", cnt("select count(*) from tns_actions"))
print("decisions_time_col=", tcol)

cut = (datetime.now(timezone.utc) - timedelta(hours=hours)).replace(microsecond=0).isoformat()
print("cut_utc_window=", cut, "(hours=", hours, ")")

print("alerts_window=", cnt("select count(*) from alerts where created_utc>=?", (cut,)))
print("decisions_window=", cnt(f"select count(*) from decisions where {tcol}>=?", (cut,)))
print("passed_window=", cnt(f"select count(*) from decisions where {tcol}>=? and passed=1", (cut,)))
print("tns_actions_window=", cnt("select count(*) from tns_actions where created_utc>=?", (cut,)))

# dispatchable: passed decisions not yet submitted/skipped_permanent
dispatchable = cnt(f"""
select count(*) from decisions d
where d.passed=1 and d.{tcol}>=?
and not exists (
  select 1 from tns_actions a
  where a.object_id=d.object_id and a.candid=d.candid
    and a.action in ('submitted','skipped_permanent')
)
""", (cut,))
print("dispatchable_window=", dispatchable)
"@
Invoke-PythonTempScript -Code $py -Args @($dbAbs, "$WindowHours") -PyExe $PythonExe

# --- TNS actions health ---
Print-Section "TNS actions (health)"

$py = @"
import sqlite3, sys
from datetime import datetime, timedelta, timezone
from collections import Counter

db = sys.argv[1]
hours = int(sys.argv[2])
dup_top_n = int(sys.argv[3])
tail = int(sys.argv[4])

con = sqlite3.connect(db)
con.row_factory = sqlite3.Row

cut = (datetime.now(timezone.utc) - timedelta(hours=hours)).replace(microsecond=0).isoformat()

def qall(sql, params=()):
    return con.execute(sql, params).fetchall()

def qone(sql, params=()):
    r = con.execute(sql, params).fetchone()
    return r[0] if r else None

def classify_detail(s):
    s = (s or '')
    s_low = s.lower()

    if ('fatal=auth' in s_low) or ('unauthorized' in s_low) or ('http=401' in s_low) or (' 401' in s_low):
        return 'auth_401'
    if ('http=403' in s_low) or (' 403' in s_low) or ('forbidden' in s_low):
        return 'auth_403'
    if ('timeout' in s_low) or ('timed out' in s_low):
        return 'timeout'
    if 'submit_failed' in s_low:
        return 'submit_failed'
    if ('ok' in s_low) and ('objname=' in s_low):
        return 'ok_submitted'
    if ('http=200' in s_low) and ('id_code=200' in s_low):
        return 'ok_http200'
    return 'other'

# 1) counts by action (window + all-time)
rows_all = qall('select action, count(*) as n from tns_actions group by action order by n desc')
rows_win = qall('select action, count(*) as n from tns_actions where created_utc>=? group by action order by n desc', (cut,))
print('by_action_all=', [(r['action'], r['n']) for r in rows_all])
print('by_action_window=', [(r['action'], r['n']) for r in rows_win])

# 2) detail classification (window)
rows = qall('select detail from tns_actions where created_utc>=?', (cut,))
c = Counter()
for r in rows:
    c[classify_detail(r['detail'])] += 1
print('detail_class_window=', c.most_common())

# 3) duplicates by (object_id,candid,action) in window
dup = qall('''
select object_id, candid, action, count(*) as n
from tns_actions
where created_utc>=?
group by object_id, candid, action
having count(*)>1
order by n desc
limit ?
''', (cut, dup_top_n))
print('dups_window_top=', [(r['object_id'], r['candid'], r['action'], r['n']) for r in dup])

# 4) ratio actions per passed (window) + actions split
passed = qone('select count(*) from decisions where created_utc>=? and passed=1', (cut,)) or 0
acts   = qone('select count(*) from tns_actions where created_utc>=?', (cut,)) or 0
ratio = (acts / passed) if passed else None
print('passed_window(created_utc)=', passed, 'tns_actions_window=', acts, 'actions_per_passed=', ratio)

np = qall('''
select
  sum(case when d.passed=1 then 1 else 0 end) as actions_on_passed,
  sum(case when d.passed=0 then 1 else 0 end) as actions_on_not_passed,
  sum(case when d.passed is null then 1 else 0 end) as actions_no_decision
from tns_actions a
left join decisions d
  on a.object_id=d.object_id and a.candid=d.candid
where a.created_utc>=?
''', (cut,))
if np:
    r = np[0]
    print('actions_split_window=', {
        'on_passed': r['actions_on_passed'],
        'on_not_passed': r['actions_on_not_passed'],
        'no_decision_match': r['actions_no_decision']
    })

# 5) last N actions (most recent)
last = qall(
    'select created_utc, action, object_id, candid, report_id, substr(detail,1,180) as detail_snip '
    'from tns_actions order by created_utc desc limit ?',
    (tail,)
)
print('last_actions=')
for r in last:
    line = "- {0} action={1} obj={2} candid={3} report_id={4} detail={5}".format(
        r['created_utc'], r['action'], r['object_id'], r['candid'], r['report_id'], r['detail_snip']
    )
    print(line)
"@
Invoke-PythonTempScript -Code $py -Args @($dbAbs, "$WindowHours", "$DupTopN", "$TnsTail") -PyExe $PythonExe

# End