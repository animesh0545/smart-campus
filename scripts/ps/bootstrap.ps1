# PowerShell version of scripts/bootstrap.sh
#   .\scripts\ps\bootstrap.ps1                # phase 1 + phase 2
#   .\scripts\ps\bootstrap.ps1 -Phase phase1  # only VPC + MSK + EKS (slow parts)
#   .\scripts\ps\bootstrap.ps1 -Phase phase2
param([ValidateSet('all', 'phase1', 'phase2')][string]$Phase = 'all')
. "$PSScriptRoot\common.ps1"
Need @('aws', 'terraform', 'kubectl')

$Region  = if ($env:AWS_REGION) { $env:AWS_REGION } else { 'us-east-1' }
$Account = aws sts get-caller-identity --query Account --output text
if (-not $Account) { Die 'aws CLI not configured (run: aws configure)' }
$Bucket = "smart-campus-tfstate-$Account-$Region"
$Lock   = 'smart-campus-tflock'
if (-not (Test-Path (Join-Path $Infra 'terraform.tfvars'))) { Die 'create infra\terraform.tfvars first (copy terraform.tfvars.example)' }

aws s3api head-bucket --bucket $Bucket 2>$null
if ($LASTEXITCODE -ne 0) {
  Info "creating Terraform state bucket $Bucket"
  if ($Region -eq 'us-east-1') { Exec { aws s3api create-bucket --bucket $Bucket --region $Region | Out-Null } }
  else { Exec { aws s3api create-bucket --bucket $Bucket --region $Region --create-bucket-configuration LocationConstraint=$Region | Out-Null } }
  Exec { aws s3api put-bucket-versioning --bucket $Bucket --versioning-configuration Status=Enabled }
}
aws dynamodb describe-table --table-name $Lock --region $Region 2>$null | Out-Null
if ($LASTEXITCODE -ne 0) {
  Info "creating state lock table $Lock"
  Exec { aws dynamodb create-table --table-name $Lock --region $Region --attribute-definitions AttributeName=LockID,AttributeType=S --key-schema AttributeName=LockID,KeyType=HASH --billing-mode PAY_PER_REQUEST | Out-Null }
  Exec { aws dynamodb wait table-exists --table-name $Lock --region $Region }
}

@"
bucket         = "$Bucket"
key            = "smart-campus/terraform.tfstate"
region         = "$Region"
dynamodb_table = "$Lock"
encrypt        = true
"@ | Set-Content -Encoding ascii (Join-Path $Infra 'backend.hcl')

Info 'terraform init'
Exec { terraform "-chdir=$Infra" init -backend-config=backend.hcl -upgrade -input=false }

if ($Phase -in @('all', 'phase1')) {
  Info 'phase 1: VPC + MSK (~25 min) + EKS (~15 min)'
  Exec { terraform "-chdir=$Infra" apply -input=false -auto-approve -target=module.network -target=module.msk -target=module.eks }
  Ok 'phase 1 done'
  if ($Phase -eq 'phase1') { Info 'run .\scripts\ps\bootstrap.ps1 -Phase phase2 next'; exit 0 }
}

Info 'phase 2: everything else'
Exec { terraform "-chdir=$Infra" apply -input=false -auto-approve }
Exec { aws eks update-kubeconfig --region $Region --name (TfOut 'cluster_name') }
kubectl get nodes -o wide
Ok 'infrastructure ready'
Write-Host "`nGitHub repo variables:  AWS_ROLE_ARN=$(TfOut 'github_actions_role_arn')   ARTIFACTS_BUCKET=$(TfOut 'artifacts_bucket')"
Write-Host 'Then: git push (CI) or .\scripts\ps\build-push.ps1, and watch ArgoCD with .\scripts\ps\port-forward.ps1'
