# PowerShell version of scripts/build-push.sh
#   .\scripts\ps\build-push.ps1
#   .\scripts\ps\build-push.ps1 -Services room-booking,availability
#   .\scripts\ps\build-push.ps1 -NoGit
param([string[]]$Services = @('user-service', 'resource-registry', 'room-booking', 'equipment-booking',
                              'availability', 'partition-manager', 'usage-analytics'),
      [switch]$NoGit)
. "$PSScriptRoot\common.ps1"
Need @('aws', 'docker', 'git', 'python')

$Registry = TfOut 'ecr_registry'
if (-not $Registry) { Die 'no ecr_registry output: run bootstrap first' }
$Region = Region
$Tag = "$(git -C $Root rev-parse --short HEAD)-$(Get-Date -Format HHmmss)"

Info "logging in to $Registry"
aws ecr get-login-password --region $Region | docker login --username AWS --password-stdin $Registry
if ($LASTEXITCODE -ne 0) { Die 'docker login failed' }

foreach ($s in $Services) {
  Info "building ${s}:$Tag"
  Exec { docker buildx build --platform linux/amd64 -f "$Root\services\Dockerfile" --build-arg "SERVICE=$s" `
           -t "$Registry/smart-campus/${s}:$Tag" -t "$Registry/smart-campus/${s}:latest" --push "$Root" }
}

Exec { python "$Root\scripts\set_image_tags.py" --registry $Registry --tag $Tag --only @Services }

if (-not $NoGit) {
  git -C $Root add gitops/apps/smart-campus/kustomization.yaml
  git -C $Root commit -m "deploy: $($Services -join ' ') -> $Tag [skip ci]"
  Exec { git -C $Root push }
  Ok 'pushed; ArgoCD syncs within ~1 min'
} else { Warn 'commit gitops/apps/smart-campus/kustomization.yaml yourself' }
