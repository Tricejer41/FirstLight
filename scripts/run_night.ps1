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

  # Cada cuántos segundos relanzar dispatch
  [Parameter(Mandatory=$false)]
  [int]$DispatchEveryS = 60,

  # Cada cuánto comprobar/reanimar procesos
  [Parameter(Mandatory=$false)]
  [int]$WatchdogS = 2,

  # Preflight Kafka (segundos totales aprox.). Si pones 0, se salta.
  [Parameter(Mandatory=$false)]
  [int]$PreflightMaxWaitS = 120,

  # Ventana de decisiones para dispatch (horas)
  [Parameter(Mandatory=$false)]
  [double]$DispatchSinceHours = 13,

  # Máximo a enviar por iteración de dispatch
  [Parameter(Mandatory=$false)]
  [int]$DispatchMaxSubmit = 3,

  # Máximo tiempo total que dispatch esperará replies (segundos) — evita bloqueos largos
  [Parameter(Mandatory=$false)]
  [int]$DispatchWaitS = 60,

  # Timeout duro del proceso dispatch (segundos): si se pasa, se mata y se continúa la noche
  [Parameter(Mandatory=$false)]
  [int]$DispatchTimeoutS = 180
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Evita cualquier barra de progreso (a veces bloquea/hace lento)
$global:ProgressPreference = "SilentlyContinue"

# -------------------------
# Helpers
# -------------------------

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

function Proc-Alive([System.Diagnostics.Process]$p) {
  if (-not $p) { return $false }
  try { $p.Refresh() } catch {}
  return (-not $p.HasExited)
}

function Stop-Proc([System.Diagnostics.Process]$p) {
  if (-not $p) { return }
  try { $p.Refresh() } catch {}
  if (-not $p.HasExited) {
    try { Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue } catch {}
  }
}

function Append-Log([string]$path, [string]$text) {
  $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
  ("[{0}] {1}" -f $ts, $text) | Add-Content -Path $path -Encoding utf8
}

# Test TCP determinista con timeout (NO Test-NetConnection)
function Test-TcpPort([string]$HostName, [int]$Port, [int]$TimeoutMs = 5000) {
  $client = New-Object System.Net.Sockets.TcpClient
  try {
    $iar = $client.BeginConnect($HostName, $Port, $null, $null)
    $ok = $iar.AsyncWaitHandle.WaitOne($TimeoutMs, $false)
    if (-not $ok) { return $false }
    $client.EndConnect($iar)
    return $true
  } catch {
    return $false
  } finally {
    try { $client.Close() } catch {}
  }
}

function Preflight-Kafka([string]$HostName, [int]$Port, [int]$MaxWaitS, [string]$LogPath) {
  if ($MaxWaitS -le 0) {
    Append-Log $LogPath ("preflight SKIPPED (PreflightMaxWaitS={0})" -f $MaxWaitS)
    return $true
  }

  $deadline = (Get-Date).AddSeconds([Math]::Max(5,$MaxWaitS))
  $sleepS = 5

  while ((Get-Date) -lt $deadline) {
    $tcpOk = Test-TcpPort -HostName $HostName -Port $Port -TimeoutMs 5000

    if ($tcpOk) {
      Append-Log $LogPath ("preflight OK {0}:{1}" -f $HostName, $Port)
      return $true
    }

    Append-Log $LogPath ("preflight FAIL tcp={0} -> retry in {1}s" -f $tcpOk, $sleepS)
    Start-Sleep -Seconds $sleepS
    $sleepS = [Math]::Min(30, [Math]::Max(5, $sleepS + 5))
  }

  Append-Log $LogPath ("preflight GAVE UP after ~{0}s (will still try to run)" -f $MaxWaitS)
  return $false
}

# -------------------------
# Repo root default
# -------------------------

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

# -------------------------
# Lock anti-doble instancia
# -------------------------

$lockDir = "C:\ProgramData\FirstLight\locks"
New-Item -ItemType Directory -Force -Path $lockDir | Out-Null
$lockFile = Join-Path $lockDir "run_night.lock"

try {
  $lockStream = [System.IO.File]::Open(
    $lockFile,
    [System.IO.FileMode]::OpenOrCreate,
    [System.IO.FileAccess]::ReadWrite,
    [System.IO.FileShare]::None
  )
} catch {
  throw "Another run_night is already running (lock busy): $lockFile"
}

# -------------------------
# Create run dirs
# -------------------------

$run = (Get-Date -Format "yyyy-MM-dd_HH-mm-ss")
$rawDir = Join-Path $RepoRoot ("alertDB\raw\" + $run)
$logDir = Join-Path $RepoRoot ("alertDB\logs\" + $run)
New-Item -ItemType Directory -Force -Path $rawDir | Out-Null
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

$consumerOut = Join-Path $logDir "consumer.out.log"
$consumerErr = Join-Path $logDir "consumer.err.log"
$replayOut   = Join-Path $logDir "replay.out.log"
$replayErr   = Join-Path $logDir "replay.err.log"
$dispatchOut = Join-Path $logDir "dispatch.out.log"   # summary (append)
$dispatchErr = Join-Path $logDir "dispatch.err.log"   # summary (append)
$replayJsonl = Join-Path $logDir "replay.jsonl"
$meta        = Join-Path $logDir "_run.meta.txt"
$stopReason  = Join-Path $logDir "_stop_reason.txt"

"RUN=$run"                 | Out-File -FilePath $meta -Encoding utf8
"RAW=$rawDir"              | Add-Content -Path $meta -Encoding utf8
"LOG=$logDir"              | Add-Content -Path $meta -Encoding utf8
"DB=$dbAbs"                | Add-Content -Path $meta -Encoding utf8
"CFG=$cfgAbs"              | Add-Content -Path $meta -Encoding utf8
"ENV=$envPath"             | Add-Content -Path $meta -Encoding utf8
"MaxHours=$MaxHours"       | Add-Content -Path $meta -Encoding utf8
"DispatchEveryS=$DispatchEveryS" | Add-Content -Path $meta -Encoding utf8
"WatchdogS=$WatchdogS"     | Add-Content -Path $meta -Encoding utf8
"PreflightMaxWaitS=$PreflightMaxWaitS" | Add-Content -Path $meta -Encoding utf8
"DispatchSinceHours=$DispatchSinceHours" | Add-Content -Path $meta -Encoding utf8
"DispatchMaxSubmit=$DispatchMaxSubmit" | Add-Content -Path $meta -Encoding utf8
"DispatchWaitS=$DispatchWaitS" | Add-Content -Path $meta -Encoding utf8
"DispatchTimeoutS=$DispatchTimeoutS" | Add-Content -Path $meta -Encoding utf8
"PythonExe=$PythonExe"     | Add-Content -Path $meta -Encoding utf8
"FinkConsumerExe=$FinkConsumerExe" | Add-Content -Path $meta -Encoding utf8

Append-Log $dispatchOut "run started"
Append-Log $dispatchErr "run started"

# -------------------------
# Preflight Kafka (best effort)
# -------------------------

$kafkaHostName = "kafka-ztf.fink-broker.org"
$kafkaPort = 24499
$null = Preflight-Kafka -HostName $kafkaHostName -Port $kafkaPort -MaxWaitS $PreflightMaxWaitS -LogPath $consumerErr

# -------------------------
# Process start functions
# -------------------------

function Start-Consumer([string]$exe, [string]$outLog, [string]$errLog, [string]$rawDir) {
  Append-Log $errLog ("starting consumer -> {0}" -f $exe)
  return Start-Process -FilePath $exe `
    -ArgumentList @("--save","-outdir",$rawDir,"-limit","0") `
    -NoNewWindow -PassThru `
    -RedirectStandardOutput $outLog -RedirectStandardError $errLog
}

function Start-Replay([string]$py, [string]$outLog, [string]$errLog, [string]$repoRoot, [string]$rawDir, [string]$cfgAbs, [string]$dbAbs, [string]$jsonl) {
  $script = Join-Path $repoRoot "scripts\replay_avro_dir.py"
  Append-Log $errLog ("starting replay -> {0} {1}" -f $py, $script)
  return Start-Process -FilePath $py `
    -ArgumentList @($script, $rawDir, "--cfg", $cfgAbs, "--db", $dbAbs, "--follow", "--poll-s", "2", "--print-every", "200", "--jsonl", $jsonl) `
    -NoNewWindow -PassThru `
    -RedirectStandardOutput $outLog -RedirectStandardError $errLog
}

# FIX: dispatch logs per-iteration (do NOT overwrite summary)
# FIX: add timeout guard to avoid infinite wait on reply
function Run-DispatchOnce(
  [string]$py,
  [string]$envPath,
  [string]$dbAbs,
  [string]$logDir,
  [string]$outSummary,
  [string]$errSummary,
  [double]$sinceHours,
  [int]$maxSubmit,
  [int]$waitS,
  [int]$timeoutS
) {
  $ts = (Get-Date).ToString("yyyyMMdd_HHmmss")
  $outOne = Join-Path $logDir ("dispatch_" + $ts + ".out.log")
  $errOne = Join-Path $logDir ("dispatch_" + $ts + ".err.log")

  $args = @(
    "-m","firstlight",
    "--env",$envPath,
    "tns","dispatch-sandbox",
    "--db",$dbAbs,
    "--since-hours",("{0}" -f $sinceHours),
    "--max-submit",("{0}" -f $maxSubmit),
    "--wait-s",("{0}" -f $waitS)
  )

  Append-Log $outSummary ("dispatch start -> {0} (since_hours={1} max_submit={2} wait_s={3} timeout_s={4})" -f (Split-Path $outOne -Leaf), $sinceHours, $maxSubmit, $waitS, $timeoutS)

  $p = Start-Process -FilePath $py -ArgumentList $args -NoNewWindow -PassThru `
        -RedirectStandardOutput $outOne -RedirectStandardError $errOne

  $exited = $false
  try {
    $exited = $p.WaitForExit([Math]::Max(5, $timeoutS) * 1000)
  } catch {
    $exited = $false
  }

  if (-not $exited) {
    Append-Log $errSummary ("dispatch TIMEOUT -> killing pid={0} after {1}s (out={2} err={3})" -f $p.Id, $timeoutS, (Split-Path $outOne -Leaf), (Split-Path $errOne -Leaf))
    Stop-Proc $p
    try { $p.Refresh() } catch {}
  } else {
    try { $p.Refresh() } catch {}
  }

  $exitCode = $null
  try { $exitCode = $p.ExitCode } catch { $exitCode = "unknown" }

  Append-Log $outSummary ("dispatch end: exit={0} out={1} err={2}" -f $exitCode, (Split-Path $outOne -Leaf), (Split-Path $errOne -Leaf))

  try {
    if ((Test-Path $errOne) -and ((Get-Item $errOne).Length -gt 0)) {
      Append-Log $errSummary ("dispatch stderr non-empty -> {0}" -f (Split-Path $errOne -Leaf))
    }
  } catch {}
}

# -------------------------
# Start processes
# -------------------------

$consumer = $null
$replay = $null

$consumerRestarts = 0
$replayRestarts = 0
$maxRestarts = 50

$consumer = Start-Consumer -exe $FinkConsumerExe -outLog $consumerOut -errLog $consumerErr -rawDir $rawDir
$replay   = Start-Replay   -py  $PythonExe      -outLog $replayOut   -errLog $replayErr   -repoRoot $RepoRoot -rawDir $rawDir -cfgAbs $cfgAbs -dbAbs $dbAbs -jsonl $replayJsonl

$until = (Get-Date).AddHours($MaxHours)
Write-Host ("Running until {0}" -f $until)

$nextDispatch = Get-Date

$stopMsg = $null
$backoffConsumer = 5
$backoffReplay = 5

try {
  while ((Get-Date) -lt $until) {

    # --- consumer watchdog + auto-restart ---
    if (-not (Proc-Alive $consumer)) {
      $consumerRestarts += 1
      Append-Log $consumerErr ("consumer died -> restart #{0}" -f $consumerRestarts)
      if ($consumerRestarts -gt $maxRestarts) {
        $stopMsg = "STOP: consumer exceeded max restarts ($maxRestarts)"
        break
      }
      Start-Sleep -Seconds $backoffConsumer
      $backoffConsumer = [Math]::Min(60, $backoffConsumer * 2)
      $consumer = Start-Consumer -exe $FinkConsumerExe -outLog $consumerOut -errLog $consumerErr -rawDir $rawDir
    } else {
      $backoffConsumer = 5
    }

    # --- replay watchdog + auto-restart ---
    if (-not (Proc-Alive $replay)) {
      $replayRestarts += 1
      Append-Log $replayErr ("replay died -> restart #{0}" -f $replayRestarts)
      if ($replayRestarts -gt $maxRestarts) {
        $stopMsg = "STOP: replay exceeded max restarts ($maxRestarts)"
        break
      }
      Start-Sleep -Seconds $backoffReplay
      $backoffReplay = [Math]::Min(60, $backoffReplay * 2)
      $replay = Start-Replay -py $PythonExe -outLog $replayOut -errLog $replayErr -repoRoot $RepoRoot -rawDir $rawDir -cfgAbs $cfgAbs -dbAbs $dbAbs -jsonl $replayJsonl
    } else {
      $backoffReplay = 5
    }

    # --- dispatch periodic ---
    if ((Get-Date) -ge $nextDispatch) {
      try {
        Run-DispatchOnce `
          -py $PythonExe `
          -envPath $envPath `
          -dbAbs $dbAbs `
          -logDir $logDir `
          -outSummary $dispatchOut `
          -errSummary $dispatchErr `
          -sinceHours $DispatchSinceHours `
          -maxSubmit $DispatchMaxSubmit `
          -waitS $DispatchWaitS `
          -timeoutS $DispatchTimeoutS
      } catch {
        Append-Log $dispatchErr ("dispatch exception: {0}" -f $_.Exception.Message)
      }
      $nextDispatch = (Get-Date).AddSeconds([Math]::Max(10,$DispatchEveryS))
    }

    Start-Sleep -Seconds ([Math]::Max(1,$WatchdogS))
  }

  if (-not $stopMsg) {
    $stopMsg = ("TIME LIMIT reached ({0})" -f $until)
  }

} finally {

  $stopMsg | Out-File $stopReason -Encoding utf8

  Write-Host "Stopping processes..."
  Stop-Proc $replay
  Stop-Proc $consumer
  Write-Host "Done."

  try { $lockStream.Close() } catch {}
}
