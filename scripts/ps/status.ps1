# PowerShell version of scripts/status.sh
param([switch]$Url)
. "$PSScriptRoot\common.ps1"
$u = GatewayUrl
if ($Url) { $u; exit 0 }
Info "Public URL: $u"
kubectl -n argocd get applications
kubectl -n $NS get pods -o wide
kubectl -n $NS get hpa
Info "MSK: $(TfOut 'msk_bootstrap_brokers')"
Info "EMR: $(TfOut 'emr_cluster_id')"
if ($u) {
  foreach ($p in @('/api/users/U001', '/api/resources/ROOM-001', '/api/partitions/ring?counts=false')) {
    try { Invoke-RestMethod "$u$p" | Out-Null; Ok $p } catch { Warn "$p failed" }
  }
}
