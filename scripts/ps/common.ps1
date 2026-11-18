# Shared helpers for the PowerShell scripts (dot-sourced). PowerShell 7+ recommended.
$ErrorActionPreference = 'Stop'
$Root  = Resolve-Path (Join-Path $PSScriptRoot '..\..')
$Infra = Join-Path $Root 'infra'
$NS    = 'smart-campus'

function Info($m) { Write-Host "==> $m" -ForegroundColor Cyan }
function Ok($m)   { Write-Host "OK  $m" -ForegroundColor Green }
function Warn($m) { Write-Host "!   $m" -ForegroundColor Yellow }
function Die($m)  { Write-Host "ERR $m" -ForegroundColor Red; exit 1 }

function Need([string[]]$tools) {
  foreach ($t in $tools) { if (-not (Get-Command $t -ErrorAction SilentlyContinue)) { Die "missing tool: $t" } }
}

function TfOut($name) {
  try { (terraform "-chdir=$Infra" output -raw $name 2>$null) } catch { '' }
}

function Region { $r = TfOut 'region'; if ($r) { $r } elseif ($env:AWS_REGION) { $env:AWS_REGION } else { 'us-east-1' } }

function GatewayUrl {
  $h = kubectl -n $NS get svc gateway -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>$null
  if ($h) { "http://$h" } else { '' }
}

function Exec([scriptblock]$cmd) {
  & $cmd
  if ($LASTEXITCODE -ne 0) { Die "command failed: $cmd" }
}
