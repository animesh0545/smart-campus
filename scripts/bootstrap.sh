#!/usr/bin/env bash
# One-shot provisioning (requirement a: everything through Terraform).
#   ./scripts/bootstrap.sh            # phase 1 (network + MSK + EKS, slow parts first) then phase 2 (rest)
#   ./scripts/bootstrap.sh --phase1   # only start the slow resources, come back later
#   ./scripts/bootstrap.sh --phase2   # finish (also used for every later change)
# Prerequisites: aws CLI (configured), terraform >= 1.6, kubectl, infra/terraform.tfvars
source "$(dirname "$0")/lib.sh"
need aws terraform kubectl

PHASE="${1:-all}"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT=$(aws sts get-caller-identity --query Account --output text) || die "aws CLI not configured (run: aws configure)"
STATE_BUCKET="smart-campus-tfstate-${ACCOUNT}-${REGION}"
LOCK_TABLE="smart-campus-tflock"

[ -f "$INFRA/terraform.tfvars" ] || die "create infra/terraform.tfvars first (copy terraform.tfvars.example)"

# ---------------------------------------------------------------- remote state (once)
if ! aws s3api head-bucket --bucket "$STATE_BUCKET" 2>/dev/null; then
  c_info "creating Terraform state bucket $STATE_BUCKET"
  if [ "$REGION" = "us-east-1" ]; then
    aws s3api create-bucket --bucket "$STATE_BUCKET" --region "$REGION" >/dev/null
  else
    aws s3api create-bucket --bucket "$STATE_BUCKET" --region "$REGION" \
      --create-bucket-configuration LocationConstraint="$REGION" >/dev/null
  fi
  aws s3api put-bucket-versioning --bucket "$STATE_BUCKET" --versioning-configuration Status=Enabled
  aws s3api put-public-access-block --bucket "$STATE_BUCKET" --public-access-block-configuration \
    BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
fi
if ! aws dynamodb describe-table --table-name "$LOCK_TABLE" --region "$REGION" >/dev/null 2>&1; then
  c_info "creating state lock table $LOCK_TABLE"
  aws dynamodb create-table --table-name "$LOCK_TABLE" --region "$REGION" \
    --attribute-definitions AttributeName=LockID,AttributeType=S \
    --key-schema AttributeName=LockID,KeyType=HASH --billing-mode PAY_PER_REQUEST >/dev/null
  aws dynamodb wait table-exists --table-name "$LOCK_TABLE" --region "$REGION"
fi

cat > "$INFRA/backend.hcl" <<EOF
bucket         = "$STATE_BUCKET"
key            = "smart-campus/terraform.tfstate"
region         = "$REGION"
dynamodb_table = "$LOCK_TABLE"
encrypt        = true
EOF

c_info "terraform init"
terraform -chdir="$INFRA" init -backend-config=backend.hcl -upgrade -input=false

# ---------------------------------------------------------------- phase 1: slow resources
if [ "$PHASE" = "all" ] || [ "$PHASE" = "--phase1" ]; then
  c_info "phase 1: VPC + MSK (~25 min) + EKS (~15 min), created in parallel"
  terraform -chdir="$INFRA" apply -input=false -auto-approve \
    -target=module.network -target=module.msk -target=module.eks
  c_ok "phase 1 done"
  [ "$PHASE" = "--phase1" ] && { c_info "run ./scripts/bootstrap.sh --phase2 next"; exit 0; }
fi

# ---------------------------------------------------------------- phase 2: everything else
c_info "phase 2: data stores, Lambda, ECR, CI role, ArgoCD, budgets"
terraform -chdir="$INFRA" apply -input=false -auto-approve

c_info "configuring kubectl"
aws eks update-kubeconfig --region "$REGION" --name "$(tf_out cluster_name)"
kubectl get nodes -o wide

c_ok "infrastructure ready"
cat <<EOF

Next steps
  1. GitHub repo -> Settings -> Secrets and variables -> Actions -> Variables:
       AWS_ROLE_ARN     = $(tf_out github_actions_role_arn)
       ARTIFACTS_BUCKET = $(tf_out artifacts_bucket)
  2. Build + push images and bump the GitOps tags (either):
       git push            (CI does it)   or   ./scripts/build-push.sh
  3. Watch ArgoCD deploy:   ./scripts/port-forward.sh   then http://localhost:8081
  4. Public URL:            ./scripts/status.sh
EOF
