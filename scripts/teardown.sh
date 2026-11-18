#!/usr/bin/env bash
# Destroy everything (run whenever you stop working).
# Order matters: the gateway's AWS load balancer is created by Kubernetes, not
# Terraform, so the ArgoCD apps are deleted first (cascading) and we wait for
# the load balancer to disappear; otherwise the VPC cannot be deleted.
#   ./scripts/teardown.sh
source "$(dirname "$0")/lib.sh"
need terraform kubectl aws

if kubectl get ns argocd >/dev/null 2>&1; then
  c_info "deleting ArgoCD root app (cascades to all apps, the gateway Service and its load balancer)"
  kubectl -n argocd delete application smart-campus-root --wait=false 2>/dev/null || true
  kubectl -n argocd delete applications --all --wait=false 2>/dev/null || true
  kubectl -n "$NS" delete svc gateway --wait=true --timeout=180s 2>/dev/null || true
  VPC=$(aws ec2 describe-vpcs --filters "Name=tag:Name,Values=smart-campus-vpc" --query 'Vpcs[0].VpcId' --output text 2>/dev/null || echo None)
  if [ "$VPC" != "None" ]; then
    for i in $(seq 1 30); do
      n=$(aws elb describe-load-balancers --query "length(LoadBalancerDescriptions[?VPCId=='$VPC'])" --output text 2>/dev/null || echo 0)
      m=$(aws elbv2 describe-load-balancers --query "length(LoadBalancers[?VpcId=='$VPC'])" --output text 2>/dev/null || echo 0)
      [ "$n" = "0" ] && [ "$m" = "0" ] && break
      c_info "waiting for load balancers to be deleted ($i/30)"; sleep 10
    done
  fi
fi

c_info "terraform destroy"
terraform -chdir="$INFRA" destroy -input=false -auto-approve

# security groups created by the Kubernetes cloud provider for the load balancer
VPC=$(aws ec2 describe-vpcs --filters "Name=tag:Name,Values=smart-campus-vpc" --query 'Vpcs[0].VpcId' --output text 2>/dev/null || echo None)
if [ "$VPC" != "None" ]; then
  c_warn "VPC $VPC still exists: removing leftover k8s-elb security groups, then retrying destroy"
  for sg in $(aws ec2 describe-security-groups --filters Name=vpc-id,Values="$VPC" Name=group-name,Values='k8s-*' --query 'SecurityGroups[].GroupId' --output text); do
    aws ec2 delete-security-group --group-id "$sg" || true
  done
  terraform -chdir="$INFRA" destroy -input=false -auto-approve
fi
c_ok "torn down. (State bucket and lock table remain; they cost ~nothing.)"
