# PowerShell version of scripts/demo-hashing.sh
#   .\scripts\ps\demo-hashing.ps1 ring | simulate | lookup -Key "ROOM-001#2026-11-18" | join | leave
param([ValidateSet('ring', 'simulate', 'lookup', 'join', 'leave')][string]$Action = 'ring',
      [string]$Key = "ROOM-001#$(Get-Date -Format yyyy-MM-dd)", [string]$Node = 'node-3')
. "$PSScriptRoot\common.ps1"
$u = if ($env:URL) { $env:URL } else { GatewayUrl }
if (-not $u) { Die 'gateway URL unknown (set $env:URL)' }
function Show($o) { $o | ConvertTo-Json -Depth 8 }
switch ($Action) {
  'ring'     { Show (Invoke-RestMethod "$u/api/partitions/ring") }
  'simulate' { Show (Invoke-RestMethod "$u/api/partitions/simulate?keys=10000&nodes=3") }
  'lookup'   { Show (Invoke-RestMethod "$u/api/partitions/lookup?key=$([uri]::EscapeDataString($Key))") }
  'join'     { Info 'BEFORE'; Show (Invoke-RestMethod "$u/api/partitions/ring")
               Info "$Node joins"; Show (Invoke-RestMethod -Method Post "$u/api/partitions/nodes" -ContentType 'application/json' -Body (@{ nodeId = $Node } | ConvertTo-Json))
               Info 'AFTER'; Show (Invoke-RestMethod "$u/api/partitions/ring") }
  'leave'    { Show (Invoke-RestMethod -Method Delete "$u/api/partitions/nodes/$Node"); Show (Invoke-RestMethod "$u/api/partitions/ring") }
}
