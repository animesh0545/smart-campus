#!/usr/bin/env bash
# Local development stack (docker compose): no AWS needed.
#   ./scripts/local.sh up        build + start Postgres, ZooKeeper, Kafka, DynamoDB Local, 7 services, gateway
#   ./scripts/local.sh check     end-to-end functional check (scripts/e2e_check.py --rebalance)
#   ./scripts/local.sh seed      create ~200 bookings
#   ./scripts/local.sh load      short k6 load test (needs k6)
#   ./scripts/local.sh flink     build the Flink jar, start a local Flink 1.18 cluster, run the job on local Kafka
#   ./scripts/local.sh results   tail the analytics-results topic (Flink output)
#   ./scripts/local.sh logs [svc]
#   ./scripts/local.sh down      stop and delete everything
source "$(dirname "$0")/lib.sh"
need docker python3
cd "$ROOT" || exit 1
URL=http://localhost:8080
DC="docker compose"

wait_up() {
  c_info "waiting for the gateway and services"
  for _ in $(seq 1 60); do
    if curl -fsS "$URL/api/partitions/ring?counts=false" >/dev/null 2>&1 \
       && curl -fsS "$URL/api/users/U001" >/dev/null 2>&1 \
       && curl -fsS "$URL/api/resources/ROOM-001" >/dev/null 2>&1; then
      c_ok "stack is up: $URL"; return 0
    fi
    sleep 3
  done
  die "services not ready: ./scripts/local.sh logs"
}

case "${1:-up}" in
  up)      $DC up -d --build; wait_up
           echo "try: curl $URL/  |  ./scripts/local.sh check" ;;
  check)   python3 scripts/e2e_check.py --url "$URL" --rebalance ;;
  seed)    python3 scripts/seed_bookings.py --url "$URL" --count "${2:-200}" ;;
  load)    need k6; k6 run -e BASE_URL="$URL" -e DURATION="${2:-1m}" -e AVAIL_VUS=10 -e BOOK_VUS=4 loadtest/k6-load.js ;;
  flink)   if [ ! -f analytics-flink/target/usage-analytics.jar ]; then
             need mvn; c_info "building Flink job"; mvn -B -q -f analytics-flink/pom.xml package
           fi
           $DC --profile flink up -d flink-jobmanager flink-taskmanager
           sleep 10
           c_info "submitting job (20 s windows locally, so results show quickly)"
           $DC exec flink-jobmanager flink run -d /opt/jobs/usage-analytics.jar \
             --bootstrap kafka:9092 --window-seconds 20
           c_ok "Flink UI: http://localhost:8081   then: ./scripts/local.sh seed 50 && ./scripts/local.sh results" ;;
  results) $DC exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
             --topic analytics-results --from-beginning ;;
  logs)    if [ -n "${2:-}" ]; then $DC logs -f "$2"; else $DC logs -f --tail 50; fi ;;
  down)    $DC --profile flink down -v ;;
  *)       die "usage: $0 up|check|seed [n]|load [duration]|flink|results|logs [svc]|down" ;;
esac
