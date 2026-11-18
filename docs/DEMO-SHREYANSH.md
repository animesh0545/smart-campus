# Demo script: Shreyansh Kedawat (2022HS120897P)

**Tasks:** b (serverless + REST/gRPC/MQ), e (Flink on EMR), f (consistent hashing), g (storage)
**Services:** Partition Manager, Equipment Booking, Notification (Lambda), Usage Analytics (Flink)
**Time:** about 10–12 minutes.

### Before the examiner arrives (≈ 20 min ahead)
```bash
export URL=$(./scripts/status.sh --url)
./scripts/emr.sh up                         # ~10 min
./scripts/flink-submit.sh                   # Flink job running
python3 scripts/seed_bookings.py --url $URL --count 200   # data for the ring + events for Flink
./scripts/demo-hashing.sh leave node-3 || true            # start from 3 nodes
```

---

## 1. (f) Consistent hashing: 4 min (the scenario's concept, do it first)
1. **Concept on synthetic keys**: `./scripts/demo-hashing.sh simulate`
   * `join_one_node`: consistent hashing moves ≈ 25–28 % of 10 000 keys; `hash mod N` moves ≈ 75 %.
   * `balance_1_vnode` vs `balance_150_vnodes`: with 1 vnode one node gets most of the keys; with 150 vnodes the max/mean is about 1.1.
2. **The real ring**: `./scripts/demo-hashing.sh ring`: 3 members, ring share ≈ 33 % each, item counts per DynamoDB table, `node-3` listed as spare.
3. **Routing**: `./scripts/demo-hashing.sh lookup "ROOM-007#$(date +%F)"`: owner node, the vnode token, and the node `mod N` would have picked.
4. **ZooKeeper membership**: `./scripts/demo-hashing.sh zk` → `[node-0, node-1, node-2]`.
5. **Live join**: `./scripts/demo-hashing.sh join`
   * Show `keys_moved_pct` ≈ 25 % vs `mod_n_would_move_pct` ≈ 75 %, and `flows` all `node-x->node-3`.
   * `ring` again: 4 members, node-3 now has items; `zk` shows node-3.
   * Bookings are still readable: `curl $URL/api/rooms/bookings/ROOM-007/$(date +%F)`.
   * Logs of both Partition Manager replicas show "ring membership changed" (ZooKeeper watch fired in each).
6. (Optional) **leave**: `./scripts/demo-hashing.sh leave node-3` → `moved_only_from_leaving_node: true`.

## 2. (b) Serverless + all three communication mechanisms: 3 min
```bash
./scripts/demo-events.sh
```
Walk through the output:
1. **REST**: client → gateway → Room Booking (201).
2. Same slot again → **409**: Room Booking called Availability over **gRPC** (`Check`), which called the Partition Manager over **gRPC** (`Lookup`), then queried one DynamoDB node. The logs show `gRPC Check -> OK`, `gRPC Lookup -> OK` with the same correlation id.
3. **Message queues**: the CREATED event went to **Kafka** `booking-events` (MSK) and **SQS**.
4. **Lambda** (SQS trigger) log in CloudWatch → notification stored in DynamoDB `notifications` (+ SNS email if subscribed).
5. Second trigger, **S3**: `./scripts/demo-events.sh upload` → CSV goes to S3 `imports/` → Lambda → admin notice "… 2 rows".
6. Equipment Booking is the same flow: `curl -s -X POST $URL/api/equipment/bookings -H 'Content-Type: application/json' -d '{"resourceId":"EQ-003","userId":"U002","date":"'$(date +%F)'","startTime":"10:00","endTime":"12:00"}'`

## 3. (e) Flink on EMR: 3 min
1. EMR console (read-only): cluster `smart-campus-flink`, applications Hadoop + Flink, step `usage-analytics-flink` Completed. Or `./scripts/flink-submit.sh --status`.
2. Explain the job (`UsageAnalyticsJob.java`): KafkaSource → parse → `keyBy(resourceId)` → **1-minute tumbling window** with an AggregateFunction (state = per-resource accumulator, checkpointed to S3 every 30 s) → KafkaSink `analytics-results`.
3. Generate traffic: `python3 scripts/seed_bookings.py --url $URL --count 60`
4. Results topic: `./scripts/kafka-consume.sh analytics-results`: one JSON per active resource per minute (bookings, cancellations, conflicts, occupancyMinutes, uniqueUsers, utilisationPct).
5. Served: `curl -s "$URL/api/analytics/summary?minutes=30" | python3 -m json.tool` (top rooms by occupancy, top equipment, most conflicted).
6. Flink logs in Grafana → "Serverless & Flink logs (CloudWatch)" → panel "Flink window results" (`WINDOW_RESULT` lines).

## 4. (g) Storage: 1 min
* **RDS PostgreSQL**: users + resources (`curl $URL/api/users?role=faculty`); private subnet, SG only from EKS nodes; one DB per service (`users`, `resources`).
* **DynamoDB**: `aws dynamodb scan --table-name smart-campus-bookings-node-3 --select COUNT`; analytics + notifications tables.
* **S3**: `aws s3 ls s3://$(terraform -chdir=infra output -raw uploads_bucket)/imports/` and the artifacts bucket (`flink/usage-analytics.jar`, `flink-checkpoints/`, `emr-logs/`).

After the demo: `./scripts/emr.sh down`.

---

## Likely questions

**Why consistent hashing instead of `hash mod N`?** With mod N, changing N changes almost every key's owner (≈ 75 % here going 3→4), so nearly all data moves. On a ring, a new node takes over only the arcs just before its virtual nodes, so ≈ k/n keys move, and only into the new node.

**Why virtual nodes?** With one token per node the arcs are random lengths, so load is badly skewed (the 1-vnode simulation gives one node about 84 % of keys). 150 tokens per node average the arc lengths out (max/mean ≈ 1.1). When a node leaves, its load spreads over *all* remaining nodes, not just one neighbour.

**Why `resourceId#date` as the key?** The conflict check needs all bookings of one resource on one day; that is exactly one partition, so one Query and no scatter/gather. The date component spreads a busy room across many partitions, so it does not become a hot spot. It is the lecture's compound/hybrid key: hash part plus sort part (`startTime#bookingId`).

**How do the Partition Manager replicas agree on the ring?** Membership lives only in ZooKeeper. Each replica has a ChildrenWatch; ZooKeeper pushes changes and each replica rebuilds the same deterministic ring (same hash, same vnode count). Migrations take a ZooKeeper lock so only one runs.

**Do reads fail during migration?** No: copy first, flip membership, then delete the originals. A write that races with the copy is picked up by the final sweep.

**What if ZooKeeper dies?** Replicas keep serving from their last ring (lookups still work); joins and leaves are blocked. When it restarts empty, the Partition Manager re-seeds the initial members. Production would use a 3-node ensemble with persistent volumes.

**Replication / quorums?** Not implemented on our ring (time-boxed). The design would store each key on the next R=3 distinct nodes clockwise with W=2, R=2 (W + R > N). DynamoDB already keeps 3 AZ copies of each table.

**Why both Kafka and SQS?** Kafka is a replayable, ordered event log for stream processing (Flink reads it, many consumers possible). SQS is a work queue with per-message retries, visibility timeout and a DLQ, and it is the native Lambda trigger.

**What makes the Lambda asynchronous and event-driven? What about failures?** It is invoked by SQS and S3 events, never by the user's request. It returns `batchItemFailures`, so only failed messages are retried; after 3 receives a message goes to the DLQ. Writes are idempotent (conditional put on eventId).

**Why gRPC for Availability and the Partition Manager?** They are called on every booking: a typed protobuf contract, smaller payloads and HTTP/2 multiplexing make them cheaper than JSON/REST, and the `.proto` file is the shared interface between the two of us.

**Is the Flink aggregation really stateful?** Yes: per key and window Flink keeps an accumulator (counts, minutes, a set of user ids) in keyed state; it is checkpointed to S3 every 30 s, so after a failure the job restores the window state and resumes from the checkpointed Kafka offsets.

**Why processing-time windows?** Demo traffic is bursty; event-time windows only close when the watermark passes, which stalls when a Kafka partition is idle. Processing time emits every minute. With event time we would add `withIdleness()` on the watermark strategy.

**Exactly-once?** Exactly-once state via checkpoints; the Kafka sink is at-least-once. The consumer writes each result with key `resourceId + windowStart`, so a duplicate overwrites itself (idempotent).

**Why EMR and this Flink version?** The PDF asks for Flink on a managed cluster; EMR 7.2.0 (as in Lab 3) ships Flink 1.18.1, so the job builds against 1.18.1 and connector `3.1.0-1.18`.

**Why three storage products?** SQL (RDS) for relational, constrained data; NoSQL (DynamoDB) for high-throughput partitioned booking records and analytics results; object storage (S3) for raw uploads (which trigger the Lambda) and Flink artifacts/checkpoints.
