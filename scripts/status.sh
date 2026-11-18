#!/usr/bin/env bash
# One-screen status of the whole system.  --url prints only the public URL.
source "$(dirname "$0")/lib.sh"
URL=$(gateway_url)
if [ "${1:-}" = "--url" ]; then echo "$URL"; exit 0; fi
need kubectl
c_info "Public URL (API gateway): ${URL:-<load balancer not ready yet>}"
c_info "ArgoCD applications";      kubectl -n argocd get applications 2>/dev/null || true
c_info "Pods";                     kubectl -n "$NS" get pods -o wide
c_info "HPAs";                     kubectl -n "$NS" get hpa
c_info "MSK brokers:  $(tf_out msk_bootstrap_brokers)"
c_info "EMR cluster:  $(tf_out emr_cluster_id)"
c_info "Lambda:       $(tf_out lambda_function_name)"
if [ -n "$URL" ]; then
  c_info "Smoke test"
  curl -fsS "$URL/api/users/U001" >/dev/null && c_ok "users" || c_warn "users"
  curl -fsS "$URL/api/resources/ROOM-001" >/dev/null && c_ok "resources" || c_warn "resources"
  curl -fsS "$URL/api/partitions/ring?counts=false" >/dev/null && c_ok "partition manager" || c_warn "partition manager"
fi
