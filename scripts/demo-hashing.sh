#!/usr/bin/env bash
# Requirement f demo: consistent hashing in the Partition Manager.
#   ./scripts/demo-hashing.sh ring        # members, ring share, items per node
#   ./scripts/demo-hashing.sh simulate    # 10k synthetic keys: consistent hashing vs mod-N, vnode balance
#   ./scripts/demo-hashing.sh lookup ROOM-001#2026-11-18
#   ./scripts/demo-hashing.sh join        # node-3 joins: only ~1/4 of keys move, all to node-3
#   ./scripts/demo-hashing.sh leave       # node-3 leaves: only node-3's keys move back
#   ./scripts/demo-hashing.sh zk          # ZooKeeper view of membership (znodes)
source "$(dirname "$0")/lib.sh"
URL="${URL:-$(gateway_url)}"
[ -n "$URL" ] || die "gateway URL unknown (set URL=http://...)"
PP() { python3 -m json.tool; }

case "${1:-ring}" in
  ring)     curl -fsS "$URL/api/partitions/ring" | PP ;;
  simulate) curl -fsS "$URL/api/partitions/simulate?keys=${KEYS:-10000}&nodes=3" | PP ;;
  lookup)   curl -fsS --get "$URL/api/partitions/lookup" --data-urlencode "key=${2:-ROOM-001#$(date +%F)}" | PP ;;
  join)     c_info "ring BEFORE"; curl -fsS "$URL/api/partitions/ring" | PP
            c_info "node-3 joins (copy -> flip ZooKeeper membership -> sweep)"
            curl -fsS -X POST "$URL/api/partitions/nodes" -H 'Content-Type: application/json' \
                 -d '{"nodeId":"node-3"}' | PP
            c_info "ring AFTER"; curl -fsS "$URL/api/partitions/ring" | PP ;;
  leave)    curl -fsS -X DELETE "$URL/api/partitions/nodes/${2:-node-3}" | PP
            curl -fsS "$URL/api/partitions/ring" | PP ;;
  zk)       need kubectl
            kubectl -n "$NS" exec deploy/zookeeper -- bin/zkCli.sh -server 127.0.0.1:2181 ls /smart-campus/nodes 2>/dev/null | tail -1
            kubectl -n "$NS" exec deploy/zookeeper -- bin/zkCli.sh -server 127.0.0.1:2181 get /smart-campus/nodes/node-0 2>/dev/null | tail -1 ;;
  *)        die "usage: $0 ring|simulate|lookup <key>|join|leave [node]|zk" ;;
esac
