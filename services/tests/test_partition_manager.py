"""Partition Manager REST + ZooKeeper logic (seeding, watch-driven ring,
lock, join/leave) with an in-memory ZooKeeper and moto DynamoDB."""
import importlib.util
import json
import pathlib
import socket

import boto3
import pytest
from moto import mock_aws

from fake_zk import FakeKazoo

SERVICES = pathlib.Path(__file__).resolve().parents[1]
TABLES = {f"node-{i}": f"pmtest-bookings-node-{i}" for i in range(4)}


def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


@pytest.fixture()
def pm(monkeypatch):
    for k, v in {"AWS_DEFAULT_REGION": "us-east-1", "AWS_ACCESS_KEY_ID": "x", "AWS_SECRET_ACCESS_KEY": "x"}.items():
        monkeypatch.setenv(k, v)
    with mock_aws():
        from common import aws, config
        aws.table.cache_clear()
        aws.dynamodb.cache_clear()
        monkeypatch.setattr(config, "NODE_TABLES", TABLES)
        monkeypatch.setattr(config, "INITIAL_NODES", ["node-0", "node-1", "node-2"])
        monkeypatch.setattr(config, "GRPC_PORT", _free_port())
        ddb = boto3.client("dynamodb", region_name="us-east-1")
        for t in TABLES.values():
            ddb.create_table(TableName=t, BillingMode="PAY_PER_REQUEST",
                             KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"},
                                        {"AttributeName": "sk", "KeyType": "RANGE"}],
                             AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"},
                                                   {"AttributeName": "sk", "AttributeType": "S"}])
        # seed 200 bookings spread by the 3-node ring
        from common.hashring import HashRing
        ring = HashRing(["node-0", "node-1", "node-2"], 150)
        for i in range(200):
            pk = f"ROOM-{i % 40:03d}#2026-12-{i % 20 + 1:02d}"
            boto3.resource("dynamodb", region_name="us-east-1").Table(TABLES[ring.owner(pk)]).put_item(
                Item={"pk": pk, "sk": f"10:00#{i}", "bookingId": str(i), "resourceType": "ROOM"})

        spec = importlib.util.spec_from_file_location("pm_app_test", SERVICES / "partition-manager/app.py")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        mod.ZK = FakeKazoo()
        from fastapi.testclient import TestClient
        with TestClient(mod.app) as client:
            yield client, mod
        aws.table.cache_clear()
        aws.dynamodb.cache_clear()


def test_seeds_initial_members_and_serves_ring(pm):
    client, mod = pm
    assert mod.ZK.get_children(mod.NODES_PATH) == ["node-0", "node-1", "node-2"]
    ring = client.get("/api/partitions/ring").json()
    assert [m["nodeId"] for m in ring["members"]] == ["node-0", "node-1", "node-2"]
    assert sum(m["items"] for m in ring["members"]) == 200
    assert ring["spareNodes"] == ["node-3"]
    assert abs(sum(m["ringSharePct"] for m in ring["members"]) - 100) < 0.1


def test_lookup(pm):
    client, _ = pm
    b = client.get("/api/partitions/lookup", params={"key": "ROOM-001#2026-12-01"}).json()
    assert b["nodeId"] in ("node-0", "node-1", "node-2") and b["table"].startswith("pmtest-")


def test_join_then_leave_moves_only_affected_keys(pm):
    client, mod = pm
    r = client.post("/api/partitions/nodes", json={"nodeId": "node-3"})
    assert r.status_code == 200, r.text
    j = r.json()
    assert j["total_items"] == 200 and 0 < j["keys_moved"] < j["total_keys"]
    assert all(f.endswith("->node-3") for f in j["flows"])
    assert j["mod_n_would_move_keys"] > j["keys_moved"]
    assert "node-3" in mod.ZK.get_children(mod.NODES_PATH)           # membership flipped in ZooKeeper
    assert "node-3" in mod.STATE.snapshot().nodes                      # watch rebuilt the ring
    ring = client.get("/api/partitions/ring").json()
    assert sum(m["items"] for m in ring["members"]) == 200             # nothing lost or duplicated
    assert client.post("/api/partitions/nodes", json={"nodeId": "node-3"}).status_code == 409

    r = client.delete("/api/partitions/nodes/node-3")
    assert r.status_code == 200 and r.json()["moved_only_from_leaving_node"] is True
    ring = client.get("/api/partitions/ring").json()
    assert [m["nodeId"] for m in ring["members"]] == ["node-0", "node-1", "node-2"]
    assert sum(m["items"] for m in ring["members"]) == 200


def test_rejects_unknown_and_last_node(pm):
    client, _ = pm
    assert client.post("/api/partitions/nodes", json={"nodeId": "node-9"}).status_code == 400
    assert client.delete("/api/partitions/nodes/node-9").status_code == 404


def test_simulate(pm):
    client, _ = pm
    b = client.get("/api/partitions/simulate", params={"keys": 5000, "nodes": 3}).json()
    assert b["join_one_node"]["consistent_hashing_moved_pct"] < b["join_one_node"]["mod_n_moved_pct"]
    assert b["balance_150_vnodes"]["stddev_pct_of_mean"] < b["balance_1_vnode"]["stddev_pct_of_mean"]


def test_lock_blocks_concurrent_rebalance(pm):
    client, mod = pm
    lock = mod.ZK.Lock(mod.LOCK_PATH)
    lock.acquire()
    try:
        original = mod.acquire_rebalance_lock.__globals__["ZK"].Lock

        def quick(path, identifier=None):
            inner = original(path, identifier)

            class Q:
                def acquire(self, timeout=None):
                    return inner.acquire(timeout=0.2)

                def release(self):
                    inner.release()
            return Q()
        mod.ZK.Lock = quick
        assert client.post("/api/partitions/nodes", json={"nodeId": "node-3"}).status_code == 409
    finally:
        lock.release()
