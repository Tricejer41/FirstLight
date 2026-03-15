param(
  [string]$EnvPath = ".\.env",
  [string]$DbPath  = ".\firstlight.sqlite",
  [double]$SinceHours = 24,
  [int]$EveryS = 300,
  [int]$MaxSubmit = 3,
  [int]$WaitS = 60,
  [int]$PollS = 5,
  [int]$TimeoutS = 180,
  [switch]$SkipReply,
  [string]$Topic = "n1"
)

while ($true) {
  $args = @(
    "-m","firstlight",
    "--env",$EnvPath,
    "tns","dispatch-sandbox",
    "--db",$DbPath,
    "--since-hours",("{0}" -f $SinceHours),
    "--max-submit",("{0}" -f $MaxSubmit),
    "--wait-s",("{0}" -f $WaitS),
    "--poll-s",("{0}" -f $PollS),
    "--timeout-s",("{0}" -f $TimeoutS),
    "--topic",$Topic
  )
  if ($SkipReply) { $args += "--skip-reply" }

  python @args
  Start-Sleep -Seconds $EveryS
}
