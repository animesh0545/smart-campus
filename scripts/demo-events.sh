#!/usr/bin/env bash
# Requirement b demo: one booking travels over REST -> gRPC -> Kafka + SQS -> Lambda.
#   ./scripts/demo-events.sh            # make a booking, then show every hop
#   ./scripts/demo-events.sh upload     # S3 CSV upload -> Lambda (second trigger)
source "$(dirname "$0")/lib.sh"
need aws kubectl
URL="${URL:-$(gateway_url)}"
[ -n "$URL" ] || die "gateway URL unknown"
REGION=$(region)
FN=$(tf_out lambda_function_name)
NOTIF=$(tf_out notifications_table)

if [ "${1:-}" = "upload" ]; then
  CSV=$(mktemp --suffix .csv 2>/dev/null || mktemp)
  printf 'id,name,type,building,capacity,features\nROOM-901,Demo Seminar Hall,ROOM,LTC,150,projector;ac\nEQ-901,Demo Camera,EQUIPMENT,NAB,1,portable\n' > "$CSV"
  c_info "REST multipart upload -> Resource Registry -> RDS + S3 imports/ -> S3 event -> Lambda"
  curl -fsS -F "file=@$CSV;filename=demo-import.csv" "$URL/api/resources/import" | python3 -m json.tool
  sleep 8
  aws dynamodb query --region "$REGION" --table-name "$NOTIF" --key-condition-expression "userId = :u" \
    --expression-attribute-values '{":u":{"S":"admin"}}' --no-scan-index-forward --max-items 1 \
    --query 'Items[0].message.S' --output text
  exit 0
fi

DAY=$(date -d '+1 day' +%F 2>/dev/null || date -v+1d +%F)
SLOT=$(( (RANDOM % 12) + 8 ))
CID="demo-$RANDOM"
c_info "1) REST: POST /api/rooms/bookings (correlation id $CID)"
curl -fsS -H "x-correlation-id: $CID" -H 'Content-Type: application/json' -X POST "$URL/api/rooms/bookings" \
  -d "{\"resourceId\":\"ROOM-007\",\"userId\":\"U001\",\"date\":\"$DAY\",\"startTime\":\"$(printf %02d $SLOT):00\",\"endTime\":\"$(printf %02d $((SLOT+1))):00\",\"purpose\":\"demo\"}" | python3 -m json.tool || true
c_info "2) same slot again -> 409 conflict (gRPC Availability.Check found it; CONFLICT event goes to Kafka)"
curl -sS -H 'Content-Type: application/json' -X POST "$URL/api/rooms/bookings" \
  -d "{\"resourceId\":\"ROOM-007\",\"userId\":\"U002\",\"date\":\"$DAY\",\"startTime\":\"$(printf %02d $SLOT):30\",\"endTime\":\"$(printf %02d $((SLOT+1))):30\"}" | python3 -m json.tool
sleep 5
c_info "3) logs for $CID across services (gRPC hop visible in availability + partition-manager)"
for d in room-booking availability partition-manager; do
  kubectl -n "$NS" logs deploy/$d --since=2m 2>/dev/null | grep "$CID" | tail -3 || true
done
c_info "4) Lambda (SQS trigger) CloudWatch log"
aws logs tail "/aws/lambda/$FN" --since 5m --region "$REGION" | tail -5
c_info "5) notification stored by Lambda in DynamoDB"
aws dynamodb query --region "$REGION" --table-name "$NOTIF" --key-condition-expression "userId = :u" \
  --expression-attribute-values '{":u":{"S":"U001"}}' --no-scan-index-forward --max-items 1 \
  --query 'Items[0].message.S' --output text
c_info "6) Kafka: ./scripts/kafka-consume.sh booking-events   (Flink output: analytics-results)"
