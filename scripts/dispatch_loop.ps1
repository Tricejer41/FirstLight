param(
  [string]$EnvFile = ".env",
  [string]$DB = "firstlight.sqlite",
  [double]$SinceHours = 24,
  [int]$MaxSubmit = 3,
  [int]$WaitS = 120,
  [int]$EveryS = 60,
  [switch]$DryRun
)

$dryArg = @()
if ($DryRun) { $dryArg = @("--dry-run") }

Write-Host "dispatch_loop: db=$DB since_hours=$SinceHours max_submit=$MaxSubmit every_s=$EveryS dry_run=$($DryRun.IsPresent)"

while ($true) {
  $ts = (Get-Date).ToString("s")
  Write-Host "[$ts] dispatch tick..."
  & python -m firstlight --env $EnvFile tns dispatch-sandbox `
      --db $DB `
      --since-hours $SinceHours `
      --max-submit $MaxSubmit `
      --wait-s $WaitS `
      @dryArg
  Start-Sleep -Seconds $EveryS
}
