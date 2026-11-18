# PowerShell version of scripts/teardown.sh (delete ArgoCD apps + load balancer first, then terraform destroy)
. "$PSScriptRoot\common.ps1"
Need @('terraform', 'kubectl', 'aws')

kubectl get ns argocd 2>$null | Out-Null
if ($LASTEXITCODE -eq 0) {
  Info 'deleting ArgoCD apps (cascades to the gateway load balancer)'
  kubectl -n argocd delete application smart-campus-root --wait=false 2>$null
  kubectl -n argocd delete applications --all --wait=false 2>$null
  kubectl -n $NS delete svc gateway --wait=true --timeout=180s 2>$null
  $vpc = aws ec2 describe-vpcs --filters 'Name=tag:Name,Values=smart-campus-vpc' --query 'Vpcs[0].VpcId' --output text
  if ($vpc -and $vpc -ne 'None') {
    for ($i = 1; $i -le 30; $i++) {
      $n = aws elb describe-load-balancers --query "length(LoadBalancerDescriptions[?VPCId=='$vpc'])" --output text
      $m = aws elbv2 describe-load-balancers --query "length(LoadBalancers[?VpcId=='$vpc'])" --output text
      if ($n -eq '0' -and $m -eq '0') { break }
      Info "waiting for load balancers ($i/30)"; Start-Sleep 10
    }
  }
}
terraform "-chdir=$Infra" destroy -input=false -auto-approve
$vpc = aws ec2 describe-vpcs --filters 'Name=tag:Name,Values=smart-campus-vpc' --query 'Vpcs[0].VpcId' --output text
if ($vpc -and $vpc -ne 'None') {
  Warn "VPC $vpc remains: removing k8s-* security groups and retrying"
  $sgs = aws ec2 describe-security-groups --filters "Name=vpc-id,Values=$vpc" 'Name=group-name,Values=k8s-*' --query 'SecurityGroups[].GroupId' --output text
  foreach ($sg in ($sgs -split '\s+' | Where-Object { $_ })) { aws ec2 delete-security-group --group-id $sg }
  terraform "-chdir=$Infra" destroy -input=false -auto-approve
}
Ok 'torn down'
