"""Partition Manager (owner: Shreyansh Kedawat). Requirement f: consistent hashing.

Acts as the routing tier (a partition-aware load balancer):
  * Storage nodes = DynamoDB tables smart-campus-bookings-node-0..3. node-0..2
    start in the ring; node-3 is a spare that can join at runtime.
  * Membership lives in ZooKeeper under /smart-campus/nodes (one znode per
    node). Every Partition Manager replica keeps a ChildrenWatch on it and
    rebuilds its in-memory HashRing when a node joins or leaves, so the
    replicas are stateless and stay consistent (ZooKeeper notifies the
    routing tier of every membership change).
  * Rebalancing runs under a ZooKeeper lock (only one replica migrates at a
    time) and moves ONLY the keys whose owner changed.

gRPC : PartitionService.Lookup / GetRing             (booking + availability hot path)
REST : GET  /api/partitions/ring                      membership, ring share, items per node
       GET  /api/partitions/lookup?key=ROOM-001#2026-11-18
       POST /api/partitions/nodes      {"nodeId":"node-3"}   join + migrate
       DELETE /api/partitions/nodes/{nodeId}                 leave + migrate
       GET  /api/partitions/simulate?keys=10000              consistent hashing vs mod-N, vnode balance
"""
import logging
import os
import socket
import threading
import time
from collections import Counter
from contextlib import asynccontextmanager
from typing import Dict, List

from fastapi import HTTPException
from kazoo.client import KazooClient
from kazoo.exceptions import LockTimeout, NodeExistsError, NoNodeError
from pydantic import BaseModel

from common import aws, config, grpc_util
from common.grpc_util import campus_pb2 as pb
from common.grpc_util import campus_pb2_grpc as pb_grpc
from common.hashring import HashRing, load_spread, mod_n_owner, movement_report
from common.observability import create_app, log_fields, setup_logging

log = setup_logging()

ROOT = "/smart-campus"
NODES_PATH = f"{ROOT}/nodes"
LOCK_PATH = f"{ROOT}/locks/rebalance"
HOSTNAME = socket.gethostname()


# ======================================================================= ring state
class RingState:
    """In-memory ring rebuilt from ZooKeeper watch callbacks."""

    def __init__(self, vnodes: int):
        self.vnodes = vnodes
        self.lock = threading.RLock()
        self.ring = HashRing(vnodes=vnodes)
        self.version = 0

    def rebuild(self, members: List[str]) -> None:
        known = [m for m in members if m in config.NODE_TABLES]
        unknown = set(members) - set(known)
        if unknown:
            log.warning(f"ignoring unknown nodes in ZooKeeper: {sorted(unknown)}")
        with self.lock:
            self.ring = HashRing(known, self.vnodes)
            self.version += 1
        log.info("ring membership changed",
                 extra=log_fields(nodes=sorted(known), ringVersion=self.version))

    def snapshot(self) -> HashRing:
        with self.lock:
            return self.ring.copy()

    def lookup(self, key: str):
        with self.lock:
            node, token = self.ring.lookup(key)
            return node, token, self.version


STATE = RingState(config.VNODES)
ZK = KazooClient(hosts=config.ZK_HOSTS, timeout=10.0)


def connect_zookeeper(max_wait: int = 180) -> None:
    deadline = time.time() + max_wait
    while True:
        try:
            ZK.start(timeout=15)
            break
        except Exception as exc:
            if time.time() > deadline:
                raise
            log.warning(f"waiting for ZooKeeper at {config.ZK_HOSTS}: {exc}")
            time.sleep(3)
    ZK.ensure_path(NODES_PATH)
    if not ZK.get_children(NODES_PATH):
        for node in config.INITIAL_NODES:
            try:
                ZK.create(f"{NODES_PATH}/{node}", config.NODE_TABLES[node].encode())
                log.info(f"seeded ring member {node}")
            except NodeExistsError:
                pass  # another replica seeded it first

    @ZK.ChildrenWatch(NODES_PATH)
    def _on_change(children):  # runs on every join/leave, in every replica
        STATE.rebuild(children)
        return True


def wait_for_membership(expected: set, timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if set(STATE.snapshot().nodes) == expected:
            return
        time.sleep(0.2)
    raise HTTPException(500, "ring did not converge after membership change")


# ======================================================================= storage helpers
def scan_all(table_name: str) -> List[dict]:
    tbl = aws.table(table_name)
    items, kwargs = [], {}
    while True:
        resp = tbl.scan(**kwargs)
        items.extend(resp["Items"])
        if "LastEvaluatedKey" not in resp:
            return items
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]


def count_items(table_name: str) -> int:
    tbl = aws.table(table_name)
    total, kwargs = 0, {"Select": "COUNT"}
    while True:
        resp = tbl.scan(**kwargs)
        total += resp["Count"]
        if "LastEvaluatedKey" not in resp:
            return total
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]


def copy_items(items: List[dict], target_ring: HashRing) -> Counter:
    """Put each item on its owner in target_ring. Returns per-target counts."""
    per_target: Dict[str, List[dict]] = {}
    for item in items:
        per_target.setdefault(target_ring.owner(item["pk"]), []).append(item)
    counts = Counter()
    for node, batch in per_target.items():
        with aws.table(config.NODE_TABLES[node]).batch_writer() as writer:
            for item in batch:
                writer.put_item(Item={**item, "node": node})
        counts[node] += len(batch)
    return counts


def sweep(source_nodes: List[str], new_ring: HashRing) -> Dict[str, object]:
    """After the ring flipped: move anything still sitting on the wrong node
    (also catches writes that raced with the first copy), then delete it from
    the source. Returns per-flow counts like {'node-1->node-3': 42}."""
    flows, moved_keys = Counter(), set()
    for src in source_nodes:
        table = config.NODE_TABLES[src]
        misplaced = [i for i in scan_all(table) if new_ring.owner(i["pk"]) != src]
        if not misplaced:
            continue
        for dst, n in copy_items(misplaced, new_ring).items():
            flows[f"{src}->{dst}"] += n
        with aws.table(table).batch_writer() as writer:
            for item in misplaced:
                writer.delete_item(Key={"pk": item["pk"], "sk": item["sk"]})
        moved_keys.update(i["pk"] for i in misplaced)
    return {"flows": dict(flows), "items_moved": sum(flows.values()), "keys_moved": len(moved_keys)}


def rebalance(old_ring: HashRing, new_ring: HashRing, apply_membership) -> Dict[str, object]:
    """Copy -> flip membership in ZooKeeper -> sweep/delete. Reads keep working
    throughout, because data exists on the new owner before routing switches."""
    started = time.time()
    snapshot = {n: scan_all(config.NODE_TABLES[n]) for n in old_ring.nodes}
    all_items = [i for items in snapshot.values() for i in items]
    all_keys = {i["pk"] for i in all_items}

    # 1. pre-copy items whose owner changes (originals stay, so reads still work)
    to_move = [i for n, items in snapshot.items() for i in items if new_ring.owner(i["pk"]) != n]
    copy_items(to_move, new_ring)

    # 2. flip membership in ZooKeeper -> every replica's watch rebuilds its ring
    apply_membership()
    wait_for_membership(set(new_ring.nodes))

    # 3. sweep: delete moved originals and pick up racing writes
    swept = sweep(old_ring.nodes, new_ring)

    keys_moved_ch = {k for k in all_keys if old_ring.owner(k) != new_ring.owner(k)}
    keys_moved_mod = {k for k in all_keys if mod_n_owner(k, old_ring.nodes) != mod_n_owner(k, new_ring.nodes)}
    total = max(len(all_keys), 1)
    changed = len(set(old_ring.nodes) ^ set(new_ring.nodes))
    return {
        "nodes_before": old_ring.nodes,
        "nodes_after": new_ring.nodes,
        "total_keys": len(all_keys),
        "total_items": len(all_items),
        "keys_moved": len(keys_moved_ch),
        "keys_moved_pct": round(100 * len(keys_moved_ch) / total, 2),
        "expected_k_over_n_pct": round(100 * changed / max(len(old_ring), len(new_ring)), 2),
        "mod_n_would_move_keys": len(keys_moved_mod),
        "mod_n_would_move_pct": round(100 * len(keys_moved_mod) / total, 2),
        "items_moved": swept["items_moved"],
        "flows": swept["flows"],
        "duration_s": round(time.time() - started, 2),
    }


def acquire_rebalance_lock():
    """ZooKeeper lock recipe: only one replica rebalances at a time."""
    lock = ZK.Lock(LOCK_PATH, identifier=HOSTNAME)
    try:
        lock.acquire(timeout=30)
    except LockTimeout:
        raise HTTPException(409, "another rebalance is in progress")
    return lock


# ======================================================================= gRPC
class PartitionServicer(pb_grpc.PartitionServiceServicer):
    def Lookup(self, request, context):
        import grpc
        try:
            node, token, version = STATE.lookup(request.key)
        except LookupError:
            context.abort(grpc.StatusCode.UNAVAILABLE, "ring is empty")
        return pb.LookupReply(key=request.key, node_id=node, table=config.NODE_TABLES[node],
                              token=str(token), ring_version=version)

    def GetRing(self, request, context):
        ring = STATE.snapshot()
        share = ring.ownership()
        return pb.RingReply(
            vnodes=ring.vnodes, ring_version=STATE.version,
            nodes=[pb.RingNode(node_id=n, table=config.NODE_TABLES[n], ownership=share.get(n, 0.0))
                   for n in ring.nodes])


_grpc_server = None


@asynccontextmanager
async def lifespan(_app):
    global _grpc_server
    aws.ensure_local_tables()
    connect_zookeeper()
    _grpc_server = grpc_util.serve(
        lambda s: pb_grpc.add_PartitionServiceServicer_to_server(PartitionServicer(), s))
    yield
    _grpc_server.stop(grace=5)
    ZK.stop()


app = create_app("Partition Manager", lifespan=lifespan)


class JoinRequest(BaseModel):
    nodeId: str


@app.get("/api/partitions/ring")
def ring_info(counts: bool = True):
    ring = STATE.snapshot()
    share = ring.ownership()
    nodes = []
    for n in ring.nodes:
        entry = {"nodeId": n, "table": config.NODE_TABLES[n],
                 "ringSharePct": round(100 * share.get(n, 0), 2)}
        if counts:
            entry["items"] = count_items(config.NODE_TABLES[n])
        nodes.append(entry)
    return {
        "ringVersion": STATE.version,
        "vnodesPerNode": ring.vnodes,
        "hash": "SHA-1 (ring 0 .. 2^160-1)",
        "members": nodes,
        "spareNodes": sorted(set(config.NODE_TABLES) - set(ring.nodes)),
        "answeredBy": HOSTNAME,
    }


@app.get("/api/partitions/lookup")
def lookup(key: str):
    try:
        node, token, version = STATE.lookup(key)
    except LookupError:
        raise HTTPException(503, "ring is empty")
    return {"key": key, "nodeId": node, "table": config.NODE_TABLES[node],
            "vnodeToken": str(token), "ringVersion": version,
            "modNOwner": mod_n_owner(key, STATE.snapshot().nodes)}


@app.post("/api/partitions/nodes")
def join(body: JoinRequest):
    node = body.nodeId
    if node not in config.NODE_TABLES:
        raise HTTPException(400, f"unknown node {node}; provisioned nodes: {sorted(config.NODE_TABLES)}")
    lock = acquire_rebalance_lock()
    try:
        old = STATE.snapshot()
        if node in old:
            raise HTTPException(409, f"{node} is already in the ring")
        new = old.copy()
        new.add_node(node)
        log.info(f"node {node} joining", extra=log_fields(nodesBefore=old.nodes))
        result = rebalance(old, new, lambda: ZK.create(f"{NODES_PATH}/{node}",
                                                       config.NODE_TABLES[node].encode()))
        log.info(f"node {node} joined", extra=log_fields(**{k: v for k, v in result.items() if k != "flows"}))
        return {"action": "join", "nodeId": node, **result}
    finally:
        lock.release()


@app.delete("/api/partitions/nodes/{node}")
def leave(node: str):
    lock = acquire_rebalance_lock()
    try:
        old = STATE.snapshot()
        if node not in old:
            raise HTTPException(404, f"{node} is not in the ring")
        if len(old) <= 1:
            raise HTTPException(400, "cannot remove the last node")
        new = old.copy()
        new.remove_node(node)

        def remove_member():
            try:
                ZK.delete(f"{NODES_PATH}/{node}")
            except NoNodeError:
                pass

        log.info(f"node {node} leaving", extra=log_fields(nodesBefore=old.nodes))
        result = rebalance(old, new, remove_member)
        # consistent hashing guarantee: only keys of the leaving node moved
        result["moved_only_from_leaving_node"] = all(f.startswith(f"{node}->") for f in result["flows"])
        return {"action": "leave", "nodeId": node, **result}
    finally:
        lock.release()


@app.get("/api/partitions/simulate")
def simulate(keys: int = 10000, nodes: int = 3, vnodes: int = config.VNODES):
    """Synthetic demo, independent of stored data: add one node to N nodes and
    count moved keys under consistent hashing vs hash-mod-N; compare key
    balance with 1 vnode vs `vnodes` vnodes per node."""
    if not (1 <= nodes <= 20 and 100 <= keys <= 200000):
        raise HTTPException(400, "1 <= nodes <= 20, 100 <= keys <= 200000")
    sample = [f"ROOM-{i % 500:03d}#2026-11-{(i // 500) % 28 + 1:02d}#{i}" for i in range(keys)]
    before = [f"node-{i}" for i in range(nodes)]
    after = before + [f"node-{nodes}"]
    return {
        "join_one_node": movement_report(sample, before, after, vnodes),
        "remove_one_node": movement_report(sample, before, before[:-1], vnodes) if nodes > 1 else None,
        "balance_1_vnode": load_spread(sample, before, 1),
        f"balance_{vnodes}_vnodes": load_spread(sample, before, vnodes),
    }


@app.get("/api/partitions/health/zookeeper")
def zk_health():
    return {"zookeeper": config.ZK_HOSTS, "state": ZK.state, "members": sorted(ZK.get_children(NODES_PATH)),
            "replica": HOSTNAME, "pid": os.getpid()}
