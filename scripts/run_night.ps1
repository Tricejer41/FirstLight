param(
  [string]$EnvFile = ".env",
  [string]$Cfg = "config/n1.example.yaml",
  [string]$DB = "firstlight.sqlite",
  [string]$OutRoot = "alertDB",

  [double]$PollS = 2,
  [int]$PrintEvery = 200,

  # Duración máxima en horas (tu caso: 13)
  [double]$MaxHours = 13,

  # Dispatch loop params (sandbox)
  [switch]$NoDispatch,
  [switch]$DispatchDryRun,
  [double]$DispatchSinceHours = 13,   # ventana comparable con la noche completa
  [int]$DispatchMaxSubmit = 3,
  [int]$DispatchWaitS = 600,
  [int]$DispatchEveryS = 60
)

$ErrorActionPreference = "Stop"

# Repo root = carpeta padre de scripts/
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $repoRoot

$run = (Get-Date).ToString("yyyy-MM-dd_HH-mm-ss")
$raw = Join-Path $OutRoot ("raw\" + $run)
$log = Join-Path $OutRoot ("logs\" + $run)

New-Item -ItemType Directory -Force -Path $raw | Out-Null
New-Item -ItemType Directory -Force -Path $log | Out-Null

# Normaliza rutas útiles
$dbPath = (Resolve-Path (Join-Path $repoRoot $DB)).Path
$cfgPath = (Resolve-Path (Join-Path $repoRoot $Cfg)).Path
$envPath = (Resolve-Path (Join-Path $repoRoot $EnvFile)).Path

Write-Host "RUN=$run"
Write-Host "RAW=$raw"
Write-Host "LOG=$log"
Write-Host "DB=$dbPath"
Write-Host "CFG=$cfgPath"
Write-Host "ENV=$envPath"
Write-Host "MaxHours=$MaxHours"

$replayJsonl = Join-Path $log "replay.jsonl"
$consumerLog = Join-Path $log "consumer.log"
$dispatchLog = Join-Path $log "dispatch.log"

$replayProc = $null
$consumerProc = $null
$dispatchProc = $null

try {
  # 1) Replay watcher (background)
  Write-Host "Starting replay watcher..."
  $replayArgs = @(
    "scripts/replay_avro_dir.py", $raw,
    "--cfg", $cfgPath,
    "--db", $dbPath,
    "--follow",
    "--poll-s", "$PollS",
    "--jsonl", $replayJsonl,
    "--print-every", "$PrintEvery"
  )
  $replayProc = Start-Process -FilePath "python" -ArgumentList $replayArgs -WorkingDirectory $repoRoot -NoNewWindow -PassThru

  # 2) Dispatch loop (background)
  if (-not $NoDispatch) {
    Write-Host "Starting dispatch loop..."
    $dispatchScript = Join-Path $PSScriptRoot "dispatch_loop.ps1"

    $dispatchArgs = @(
      "-NoProfile",
      "-ExecutionPolicy", "Bypass",
      "-File", $dispatchScript,
      "-EnvFile", $envPath,
      "-DB", $dbPath,
      "-SinceHours", "$DispatchSinceHours",
      "-MaxSubmit", "$DispatchMaxSubmit",
      "-WaitS", "$DispatchWaitS",
      "-EveryS", "$DispatchEveryS"
    )

    if ($DispatchDryRun) { $dispatchArgs += "-DryRun" }

    # redirige salida a un log
    $dispatchProc = Start-Process -FilePath "powershell" -ArgumentList $dispatchArgs `
      -WorkingDirectory $repoRoot -NoNewWindow -PassThru `
      -RedirectStandardOutput $dispatchLog -RedirectStandardError $dispatchLog
  }

  # 3) fink_consumer (background)
  Write-Host "Starting fink_consumer at $((Get-Date).ToString('o'))"
  $consumerArgs = @(
    "--save",
    "-outdir", $raw,
    "-limit", "0"
  )
  $consumerProc = Start-Process -FilePath "fink_consumer" -ArgumentList $consumerArgs `
    -WorkingDirectory $repoRoot -NoNewWindow -PassThru `
    -RedirectStandardOutput $consumerLog -RedirectStandardError $consumerLog

  # 4) Espera hasta MaxHours (o hasta que consumer termine)
  $deadline = (Get-Date).AddHours($MaxHours)
  Write-Host "Running until $($deadline.ToString('o'))"

  while ((Get-Date) -lt $deadline) {
    if ($consumerProc -ne $null -and $consumerProc.HasExited) {
      Write-Host "fink_consumer exited early with code $($consumerProc.ExitCode)"
      break
    }
    Start-Sleep -Seconds 10
  }

} finally {
  Write-Host "Stopping processes..."

  if ($dispatchProc -ne $null -and -not $dispatchProc.HasExited) {
    Stop-Process -Id $dispatchProc.Id -Force -ErrorAction SilentlyContinue
  }

  if ($replayProc -ne $null -and -not $replayProc.HasExited) {
    Stop-Process -Id $replayProc.Id -Force -ErrorAction SilentlyContinue
  }

  if ($consumerProc -ne $null -and -not $consumerProc.HasExited) {
    Stop-Process -Id $consumerProc.Id -Force -ErrorAction SilentlyContinue
  }

  Write-Host "Done."
}
