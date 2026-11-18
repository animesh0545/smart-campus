#!/usr/bin/env bash
# Build the Flink job, upload it to S3 and run it on EMR as an EMR step
# (Flink on YARN, detached). Requires EMR + MSK to be up (./scripts/emr.sh up).
#   ./scripts/flink-submit.sh            # build (if needed), upload, submit
#   ./scripts/flink-submit.sh --status   # list steps + YARN app state
source "$(dirname "$0")/lib.sh"
need aws

CLUSTER=$(tf_out emr_cluster_id)
BROKERS=$(tf_out msk_bootstrap_brokers)
BUCKET=$(tf_out artifacts_bucket)
REGION=$(region)
JAR="$ROOT/analytics-flink/target/usage-analytics.jar"
[ -n "$CLUSTER" ] || die "EMR is off. Run ./scripts/emr.sh up"
[ -n "$BROKERS" ] || die "MSK is off (enable_msk=false)"

if [ "${1:-}" = "--status" ]; then
  aws emr list-steps --cluster-id "$CLUSTER" --region "$REGION" \
    --query 'Steps[].[Name,Status.State,Status.Timeline.StartDateTime]' --output table
  exit 0
fi

if [ ! -f "$JAR" ] || [ -n "${REBUILD:-}" ]; then
  need mvn
  c_info "building Flink job"
  mvn -B -q -f "$ROOT/analytics-flink/pom.xml" package
fi

c_info "uploading jar to s3://$BUCKET/flink/"
aws s3 cp "$JAR" "s3://$BUCKET/flink/usage-analytics.jar" --region "$REGION"

STEPS=$(mktemp)
cat > "$STEPS" <<EOF
[
  {
    "Name": "usage-analytics-flink",
    "ActionOnFailure": "CONTINUE",
    "Type": "CUSTOM_JAR",
    "Jar": "command-runner.jar",
    "Args": [
      "bash", "-c",
      "aws s3 cp s3://$BUCKET/flink/usage-analytics.jar /home/hadoop/usage-analytics.jar && flink run -m yarn-cluster -d -ynm usage-analytics /home/hadoop/usage-analytics.jar --bootstrap $BROKERS --checkpoint-dir s3://$BUCKET/flink-checkpoints --window-seconds 60"
    ]
  }
]
EOF
c_info "submitting EMR step to $CLUSTER"
STEP_ID=$(aws emr add-steps --cluster-id "$CLUSTER" --region "$REGION" --steps "file://$STEPS" --query 'StepIds[0]' --output text)
rm -f "$STEPS"
c_info "step $STEP_ID: waiting until the detached Flink job is submitted"
aws emr wait step-complete --cluster-id "$CLUSTER" --step-id "$STEP_ID" --region "$REGION" \
  && c_ok "Flink job running on YARN (detached)" \
  || c_warn "step did not complete cleanly: see step logs in CloudWatch group $(tf_out flink_log_group) (stream */emr-steps*)"
cat <<EOF

Verify:
  * results topic:  ./scripts/kafka-consume.sh analytics-results
  * stored results: curl \$(./scripts/status.sh --url)/api/analytics/summary
  * Flink logs:     Grafana dashboard "Smart Campus - Serverless & Flink logs (CloudWatch)"
  * Flink UI:       EMR console -> cluster -> Applications -> Flink (or YARN RM proxy)
EOF
