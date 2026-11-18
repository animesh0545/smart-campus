#!/usr/bin/env bash
# Build all (or some) service images for linux/amd64, push them to ECR, point
# the GitOps kustomization at the new tag, commit and push. ArgoCD deploys it.
# This is the local equivalent of .github/workflows/ci.yml (handy for the first
# deploy, or if Actions is slow). Works on Apple Silicon (cross-builds amd64).
#   ./scripts/build-push.sh                       # all 7 services
#   ./scripts/build-push.sh room-booking availability
#   NO_GIT=1 ./scripts/build-push.sh              # push images only, don't commit
source "$(dirname "$0")/lib.sh"
need aws docker git python3

ALL=(user-service resource-registry room-booking equipment-booking availability partition-manager usage-analytics)
if [ $# -gt 0 ]; then SERVICES=("$@"); else SERVICES=("${ALL[@]}"); fi

REGISTRY=$(tf_out ecr_registry)
[ -n "$REGISTRY" ] || die "no ecr_registry output: run ./scripts/bootstrap.sh first"
REGION=$(region)
TAG="$(git -C "$ROOT" rev-parse --short HEAD)-$(date +%H%M%S)"

c_info "logging in to $REGISTRY"
aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "$REGISTRY"

for svc in "${SERVICES[@]}"; do
  c_info "building $svc:$TAG"
  docker buildx build --platform linux/amd64 -f "$ROOT/services/Dockerfile" \
    --build-arg SERVICE="$svc" -t "$REGISTRY/smart-campus/$svc:$TAG" -t "$REGISTRY/smart-campus/$svc:latest" \
    --push "$ROOT"
done

python3 "$ROOT/scripts/set_image_tags.py" --registry "$REGISTRY" --tag "$TAG" --only "${SERVICES[@]}"

if [ -z "${NO_GIT:-}" ]; then
  git -C "$ROOT" add gitops/apps/smart-campus/kustomization.yaml
  git -C "$ROOT" commit -m "deploy: ${SERVICES[*]} -> $TAG [skip ci]" || true
  git -C "$ROOT" push
  c_ok "pushed; ArgoCD syncs within ~1 min (or click Refresh in the UI)"
else
  c_warn "NO_GIT set: commit gitops/apps/smart-campus/kustomization.yaml yourself"
fi
