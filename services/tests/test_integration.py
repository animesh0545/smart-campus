"""In-process integration test of the booking hot path, no Docker needed:
REST (Room Booking) -> gRPC (Availability) -> gRPC (Partition Manager) -> DynamoDB (moto),
plus a live rebalance (node-3 joins) through the Partition Manager's migration code."""
import importlib.util
import json
import os
import pathlib
import socket

import boto3
import pytest
from moto import mock_aws

SERVICES = pathlib.Path(__file__).resolve().parents[1]
TABLES = {f"node-{i}": f"test-bookings-node-{i}" for i in range(4)}


def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _load(name, rel):
    spec = importlib.util.spec_from_file_location(name, SERVICES / rel)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def stack():
    pm_port, av_port = _free_port(), _free_port()
    os.environ.update({
        "AWS_DEFAULT_REGION": "us-east-1", "AWS_ACCESS_KEY_ID": "x", "AWS_SECRET_ACCESS_KEY": "x",
        "NODE_TABLES": json.dumps(TABLES), "INITIAL_NODES": "node-0,node-1,node-2",
        "PARTITION_GRPC": f"localhost:{pm_port}", "AVAILABILITY_GRPC": f"localhost:{av_port}",
    })
    with mock_aws():
        from common import config
        config.NODE_TABLES = TABLES
        config.PARTITION_GRPC = f"localhost:{pm_port}"
        config.AVAILABILITY_GRPC = f"localhost:{av_port}"
        ddb = boto3.client("dynamodb", region_name="us-east-1")
        for t in TABLES.values():
            ddb.create_table(TableName=t, BillingMode="PAY_PER_REQUEST",
                             KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"},
                                        {"AttributeName": "sk", "KeyType": "RANGE"}],
                             AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"},
                                                   {"AttributeName": "sk", "AttributeType": "S"}])

        from common import grpc_util
        pm = _load("pm_app", "partition-manager/app.py")
        av = _load("av_app", "availability/app.py")
        pm.STATE.rebuild(["node-0", "node-1", "node-2"])  # what the ZooKeeper watch would do
        s1 = grpc_util.serve(lambda s: pm.pb_grpc.add_PartitionServiceServicer_to_server(
            pm.PartitionServicer(), s), port=pm_port)
        s2 = grpc_util.serve(lambda s: av.pb_grpc.add_AvailabilityServiceServicer_to_server(
            av.AvailabilityServicer(), s), port=av_port)

        rb = _load("rb_app", "room-booking/app.py")
        from common import peers
        peers.get_resource = lambda rid: {"id": rid, "name": rid, "type": "ROOM", "available": True}
        peers.get_user = lambda uid: {"id": uid}
        from fastapi.testclient import TestClient
        with TestClient(rb.app) as client:
            yield {"client": client, "pm": pm, "av": av}
        s1.stop(0)
        s2.stop(0)


def test_create_conflict_modify_cancel(stack):
    c = stack["client"]
    body = {"resourceId": "ROOM-001", "userId": "U001", "date": "2026-11-18",
            "startTime": "10:00", "endTime": "11:00"}
    r = c.post("/api/rooms/bookings", json=body)
    assert r.status_code == 201, r.text
    booking = r.json()
    assert booking["node"] in ("node-0", "node-1", "node-2")

    clash = c.post("/api/rooms/bookings", json={**body, "startTime": "10:30", "endTime": "11:30"})
    assert clash.status_code == 409
    assert clash.json()["conflicts"][0]["bookingId"] == booking["bookingId"]

    ok = c.post("/api/rooms/bookings", json={**body, "startTime": "11:00", "endTime": "12:00"})
    assert ok.status_code == 201  # touching intervals do not conflict

    url = f"/api/rooms/bookings/ROOM-001/2026-11-18/{booking['bookingId']}"
    moved = c.put(url, json={"date": "2026-11-19", "startTime": "09:00", "endTime": "10:00"})
    assert moved.status_code == 200, moved.text
    assert c.get(url).status_code == 404
    url2 = f"/api/rooms/bookings/ROOM-001/2026-11-19/{booking['bookingId']}"
    assert c.get(url2).json()["date"] == "2026-11-19"
    assert c.delete(url2).json()["status"] == "CANCELLED"

    mine = c.get("/api/rooms/users/U001/bookings").json()
    assert len(mine["nodesQueried"]) == 3


def test_validation(stack):
    c = stack["client"]
    bad = c.post("/api/rooms/bookings", json={"resourceId": "ROOM-001", "userId": "U001",
                                               "date": "18-11-2026", "startTime": "10:00", "endTime": "11:00"})
    assert bad.status_code == 400


def test_join_node_moves_only_affected_keys(stack):
    c, pm = stack["client"], stack["pm"]
    for i in range(60):
        r = c.post("/api/rooms/bookings", json={"resourceId": f"ROOM-{i:03d}", "userId": "U002",
                                                 "date": "2026-11-20", "startTime": "14:00", "endTime": "15:00"})
        assert r.status_code == 201
    old = pm.STATE.snapshot()
    new = old.copy()
    new.add_node("node-3")
    result = pm.rebalance(old, new, lambda: pm.STATE.rebuild(new.nodes))
    assert result["keys_moved"] < result["total_keys"]
    assert all(f.endswith("->node-3") for f in result["flows"])
    assert result["mod_n_would_move_keys"] >= result["keys_moved"]
    # every booking is still readable through normal routing after the move
    for i in range(60):
        r = c.get(f"/api/rooms/bookings/ROOM-{i:03d}/2026-11-20")
        assert len(r.json()["bookings"]) == 1, (i, r.json())
    # nothing left on the wrong node
    for n in new.nodes:
        for item in pm.scan_all(TABLES[n]):
            assert new.owner(item["pk"]) == n


def test_availability_rest_and_schedule(stack):
    from fastapi.testclient import TestClient
    av = stack["av"]
    client = TestClient(av.app)  # no lifespan: reuse the module-level gRPC server
    r = client.get("/api/availability", params={"resourceId": "ROOM-005", "date": "2026-11-20",
                                                "startTime": "14:30", "endTime": "15:30"})
    assert r.status_code == 200 and r.json()["available"] is False
    s = client.get("/api/availability/ROOM-005/2026-11-20/schedule").json()
    assert s["free"][0] == {"from": "08:00", "to": "14:00"}
