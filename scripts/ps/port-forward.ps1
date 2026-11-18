# PowerShell version of scripts/port-forward.sh
# ArgoCD http://localhost:8081 (admin / printed password), Grafana http://localhost:3000 (admin / campus-admin),
# Prometheus http://localhost:9090. Close the window or press Ctrl+C to stop.
. "$PSScriptRoot\common.ps1"
Need @('kubectl')
$b64 = kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' 2>$null
if ($b64) { Info "ArgoCD admin password: $([Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($b64)))" }
$jobs = @(
  Start-Job { kubectl -n argocd port-forward svc/argocd-server 8081:80 },
  Start-Job { kubectl -n monitoring port-forward svc/kps-grafana 3000:80 },
  Start-Job { kubectl -n monitoring port-forward svc/kps-prometheus 9090:9090 }
)
Ok 'ArgoCD :8081  Grafana :3000  Prometheus :9090  (Ctrl+C to stop)'
try { Wait-Job $jobs | Out-Null } finally { $jobs | Stop-Job; $jobs | Remove-Job -Force }
