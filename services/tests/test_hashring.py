"""Unit tests for the consistent-hash ring (run: pytest services/tests)."""
import random

from common.hashring import HashRing, load_spread, mod_n_owner, movement_report, sha1_int, RING_SIZE

KEYS = [f"ROOM-{i % 300:03d}#2026-11-{i % 28 + 1:02d}#{i}" for i in range(20000)]


def test_lookup_is_deterministic_and_clockwise():
    ring = HashRing(["node-0", "node-1", "node-2"], vnodes=50)
    for k in KEYS[:500]:
        node, token = ring.lookup(k)
        assert ring.lookup(k) == (node, token)
        h = sha1_int(k)
        # token is the first vnode clockwise: either > h, or the wrap-around minimum
        assert token > h or token == min(ring._tokens)


def test_ownership_sums_to_one():
    ring = HashRing(["a", "b", "c", "d"], vnodes=150)
    share = ring.ownership()
    assert abs(sum(share.values()) - 1.0) < 1e-9
    assert all(0.15 < v < 0.35 for v in share.values())


def test_join_moves_about_k_over_n_and_only_to_new_node():
    before = HashRing(["node-0", "node-1", "node-2"], vnodes=150)
    after = before.copy()
    after.add_node("node-3")
    moved = [k for k in KEYS if before.owner(k) != after.owner(k)]
    assert all(after.owner(k) == "node-3" for k in moved)          # only to the new node
    assert 0.17 < len(moved) / len(KEYS) < 0.33                     # ~1/4 of keys


def test_leave_moves_only_keys_of_leaving_node():
    before = HashRing(["node-0", "node-1", "node-2", "node-3"], vnodes=150)
    after = before.copy()
    after.remove_node("node-1")
    for k in KEYS:
        if before.owner(k) != after.owner(k):
            assert before.owner(k) == "node-1"


def test_mod_n_moves_almost_everything():
    report = movement_report(KEYS, ["node-0", "node-1", "node-2"], ["node-0", "node-1", "node-2", "node-3"])
    assert report["mod_n_moved_pct"] > 60
    assert report["consistent_hashing_moved_pct"] < 33


def test_virtual_nodes_balance_load():
    one = load_spread(KEYS, ["n0", "n1", "n2"], vnodes=1)
    many = load_spread(KEYS, ["n0", "n1", "n2"], vnodes=150)
    assert many["stddev_pct_of_mean"] < one["stddev_pct_of_mean"]
    assert many["max_over_mean"] < 1.2


def test_remove_then_add_restores_same_mapping():
    ring = HashRing(["x", "y", "z"], vnodes=100)
    snapshot = {k: ring.owner(k) for k in KEYS[:2000]}
    ring.remove_node("y")
    ring.add_node("y")
    assert {k: ring.owner(k) for k in KEYS[:2000]} == snapshot


def test_mod_n_owner_matches_definition():
    nodes = ["node-2", "node-0", "node-1"]
    k = "ROOM-001#2026-11-18"
    assert mod_n_owner(k, nodes) == sorted(nodes)[sha1_int(k) % 3]
    assert RING_SIZE == 2 ** 160
