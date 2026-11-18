# PowerShell version of scripts/local.sh (docker compose dev stack)
#   .\scripts\ps\local.ps1 up | check | seed | load | flink | results | logs | down
param([ValidateSet('up', 'check', 'seed', 'load', 'flink', 'results', 'logs', 'down')][string]$Action = 'up',
      [string]$Arg = '')
. "$PSScriptRoot\common.ps1"
Need @('docker', 'python')
Set-Location $Root
$Url = 'http://localhost:8080'

function WaitUp {
  Info 'waiting for the gateway and services'
  for ($i = 0; $i -lt 60; $i++) {
    try {
      Invoke-RestMethod "$Url/api/partitions/ring?counts=false" | Out-Null
      Invoke-RestMethod "$Url/api/users/U001" | Out-Null
      Invoke-RestMethod "$Url/api/resources/ROOM-001" | Out-Null
      Ok "stack is up: $Url"; return
    } catch { Start-Sleep 3 }
  }
  Die 'services not ready: .\scripts\ps\local.ps1 logs'
}

switch ($Action) {
  'up'      { Exec { docker compose up -d --build }; WaitUp }
  'check'   { python scripts/e2e_check.py --url $Url --rebalance }
  'seed'    { $n = if ($Arg) { $Arg } else { '200' }; python scripts/seed_bookings.py --url $Url --count $n }
  'load'    { Need @('k6'); $d = if ($Arg) { $Arg } else { '1m' }; k6 run -e BASE_URL=$Url -e DURATION=$d -e AVAIL_VUS=10 -e BOOK_VUS=4 loadtest/k6-load.js }
  'flink'   {
    if (-not (Test-Path 'analytics-flink/target/usage-analytics.jar')) { Need @('mvn'); Exec { mvn -B -q -f analytics-flink/pom.xml package } }
    Exec { docker compose --profile flink up -d flink-jobmanager flink-taskmanager }
    Start-Sleep 10
    Exec { docker compose exec flink-jobmanager flink run -d /opt/jobs/usage-analytics.jar --bootstrap kafka:9092 --window-seconds 20 }
    Ok 'Flink UI: http://localhost:8081   then: local.ps1 seed 50 ; local.ps1 results'
  }
  'results' { docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic analytics-results --from-beginning }
  'logs'    { if ($Arg) { docker compose logs -f $Arg } else { docker compose logs -f --tail 50 } }
  'down'    { docker compose --profile flink down -v }
}
