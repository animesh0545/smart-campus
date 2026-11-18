# PowerShell version of scripts/demo-events.sh
#   .\scripts\ps\demo-events.ps1            # booking -> REST, gRPC, Kafka + SQS -> Lambda
#   .\scripts\ps\demo-events.ps1 -Upload    # CSV upload -> S3 -> Lambda
param([switch]$Upload)
. "$PSScriptRoot\common.ps1"
Need @('aws', 'kubectl')
$u = if ($env:URL) { $env:URL } else { GatewayUrl }
if (-not $u) { Die 'gateway URL unknown (set $env:URL)' }
$Region = Region
$Fn = TfOut 'lambda_function_name'
$Notif = TfOut 'notifications_table'

function LatestNotice($user) {
  aws dynamodb query --region $Region --table-name $Notif --key-condition-expression 'userId = :u' `
    --expression-attribute-values "{`":u`":{`"S`":`"$user`"}}" --no-scan-index-forward --max-items 1 `
    --query 'Items[0].message.S' --output text
}

if ($Upload) {
  Info 'REST multipart upload -> Resource Registry -> RDS + S3 imports/ -> S3 event -> Lambda'
  $csv = Join-Path $Root 'samples\resources-import.csv'
  curl.exe -fsS -F "file=@$csv;filename=demo-import.csv" "$u/api/resources/import"
  Start-Sleep 8
  LatestNotice 'admin'
  exit 0
}

$day = (Get-Date).AddDays(1).ToString('yyyy-MM-dd')
$h = Get-Random -Minimum 8 -Maximum 20
$cid = "demo-$(Get-Random)"
$body = @{ resourceId = 'ROOM-007'; userId = 'U001'; date = $day; startTime = ('{0:D2}:00' -f $h); endTime = ('{0:D2}:00' -f ($h + 1)); purpose = 'demo' }
Info "1) REST POST /api/rooms/bookings (correlation id $cid)"
try { Invoke-RestMethod -Method Post "$u/api/rooms/bookings" -ContentType 'application/json' -Headers @{ 'x-correlation-id' = $cid } -Body ($body | ConvertTo-Json) | ConvertTo-Json }
catch { Warn $_.Exception.Message }
Info '2) overlapping slot -> 409 (gRPC Availability.Check found the conflict)'
$body.userId = 'U002'; $body.startTime = ('{0:D2}:30' -f $h); $body.endTime = ('{0:D2}:30' -f ($h + 1))
try { Invoke-RestMethod -Method Post "$u/api/rooms/bookings" -ContentType 'application/json' -Body ($body | ConvertTo-Json) }
catch { Write-Host "HTTP $($_.Exception.Response.StatusCode.value__) (expected 409)" }
Start-Sleep 5
Info "3) logs for $cid (gRPC hop visible in availability + partition-manager)"
foreach ($d in @('room-booking', 'availability', 'partition-manager')) {
  kubectl -n $NS logs "deploy/$d" --since=2m | Select-String $cid | Select-Object -Last 3
}
Info '4) Lambda log (SQS trigger)'
aws logs tail "/aws/lambda/$Fn" --since 5m --region $Region | Select-Object -Last 5
Info '5) notification stored by the Lambda'
LatestNotice 'U001'
Info '6) Kafka: .\scripts\ps\kafka-consume.ps1 booking-events'
