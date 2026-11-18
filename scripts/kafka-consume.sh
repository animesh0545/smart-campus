#!/usr/bin/env bash
# Tail a Kafka topic on MSK from inside the cluster (MSK has no public endpoint).
# Starts a throw-away debugging pod: a client tool, not a service deployment,
# so it doesn't break the GitOps rule (requirement d covers service deployment).
#   ./scripts/kafka-consume.sh analytics-results
#   ./scripts/kafka-consume.sh booking-events
source "$(dirname "$0")/lib.sh"
need kubectl
TOPIC="${1:-analytics-results}"
BROKERS=$(tf_out msk_bootstrap_brokers)
[ -n "$BROKERS" ] || die "MSK is off"
kubectl -n "$NS" run "kafka-cli-$RANDOM" --rm -i --tty --restart=Never --image=apache/kafka:3.9.0 -- \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server "$BROKERS" --topic "$TOPIC" \
  --from-beginning --property print.timestamp=true
