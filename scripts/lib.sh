# shellcheck shell=bash
# Shared helpers for the bash scripts (sourced, not executed).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INFRA="$ROOT/infra"
NS=smart-campus

c_info()  { printf '\033[1;34m==> %s\033[0m\n' "$*"; }
c_ok()    { printf '\033[1;32m✔ %s\033[0m\n' "$*"; }
c_warn()  { printf '\033[1;33m! %s\033[0m\n' "$*"; }
die()     { printf '\033[1;31m✘ %s\033[0m\n' "$*" >&2; exit 1; }

need() { for b in "$@"; do command -v "$b" >/dev/null 2>&1 || die "missing tool: $b"; done; }

tf_out() { terraform -chdir="$INFRA" output -raw "$1" 2>/dev/null || true; }

gateway_url() {
  local host
  host=$(kubectl -n "$NS" get svc gateway -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || true)
  [ -n "$host" ] && echo "http://$host" || echo ""
}

region() { local r; r=$(tf_out region); echo "${r:-${AWS_REGION:-us-east-1}}"; }
