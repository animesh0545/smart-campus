#!/usr/bin/env bash
# Open the internal UIs on localhost (Ctrl+C stops all).
#   ArgoCD      http://localhost:8081   user admin, password printed below
#   Grafana     http://localhost:3000   user admin, password campus-admin
#   Prometheus  http://localhost:9090
source "$(dirname "$0")/lib.sh"
need kubectl
PASS=$(kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' 2>/dev/null | base64 -d || true)
c_info "ArgoCD admin password: ${PASS:-<not found>}"
kubectl -n argocd port-forward svc/argocd-server 8081:80 >/dev/null &
kubectl -n monitoring port-forward svc/kps-grafana 3000:80 >/dev/null &
kubectl -n monitoring port-forward svc/kps-prometheus 9090:9090 >/dev/null &
trap 'kill $(jobs -p) 2>/dev/null' EXIT
c_ok "ArgoCD :8081  Grafana :3000  Prometheus :9090  (Ctrl+C to stop)"
wait
