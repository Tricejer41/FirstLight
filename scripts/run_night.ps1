$run = (Get-Date).ToString("yyyy-MM-dd_HH-mm-ss")
New-Item -ItemType Directory -Force -Path "alertDB/raw/$run" | Out-Null
New-Item -ItemType Directory -Force -Path "alertDB/logs/$run" | Out-Null

Write-Host "RUN=$run"
Write-Host "RAW=alertDB/raw/$run"
Write-Host "LOG=alertDB/logs/$run"

# 1) Arranca el watcher (no se cae si no hay datos)
Start-Process -NoNewWindow -FilePath "python" -ArgumentList @(
  "scripts/replay_avro_dir.py",
  "alertDB/raw/$run",
  "--cfg","config/n1.example.yaml",
  "--db","firstlight.sqlite",
  "--follow",
  "--poll-s","2",
  "--jsonl","alertDB/logs/$run/replay.jsonl",
  "--print-every","200",
  "--topic","hostless_ztf"
)

# 2) Loop infinito: si el broker te corta, se reinicia solo
while ($true) {
  Write-Host "Starting fink_consumer at $(Get-Date -Format o)"
  fink_consumer --save -outdir "alertDB/raw/$run" -limit 0
  Write-Host "fink_consumer exited. Sleeping 15s then restarting..."
  Start-Sleep -Seconds 15
}
