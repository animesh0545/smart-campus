#!/usr/bin/env bash
# Turn the (expensive) EMR Flink cluster on or off through Terraform.
#   ./scripts/emr.sh up     # ~10 min, ~$0.48/hr
#   ./scripts/emr.sh down
# Other variables keep the values in infra/terraform.tfvars.
source "$(dirname "$0")/lib.sh"
need terraform
case "${1:-}" in
  up)   terraform -chdir="$INFRA" apply -input=false -auto-approve -var enable_emr=true
        c_ok "EMR cluster: $(tf_out emr_cluster_id). Next: ./scripts/flink-submit.sh" ;;
  down) terraform -chdir="$INFRA" apply -input=false -auto-approve -var enable_emr=false
        c_ok "EMR destroyed" ;;
  *)    die "usage: $0 up|down" ;;
esac
c_warn "set enable_emr in terraform.tfvars too, or the next plain 'terraform apply' will revert this"
