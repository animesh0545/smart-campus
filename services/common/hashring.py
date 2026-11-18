"""Consistent hashing with virtual nodes.

* Hash space: SHA-1, i.e. positions 0 .. 2^160 - 1, viewed as a ring.
* Every storage node is placed on the ring `vnodes` times
  (token = SHA-1("<node>#vn<i>")). Real systems use > 100 vnodes per node,
  so the default here is 150.
* Owner of a key: hash the key, walk clockwise to the first virtual node
  (bisect on the sorted token list, wrapping to index 0 past the end).
* Adding or removing a node only changes ownership of the arcs next to that
  node's virtual nodes, so on average only k/n keys move. With `hash mod N`
  nearly all keys move (compare `mod_n_owner`).
"""
from __future__ import annotations

import bisect
import hashlib
from typing import Dict, Iterable, List, Optional, Tuple

RING_SIZE = 2 ** 160


def sha1_int(value: str) -> int:
    return int(hashlib.sha1(value.encode("utf-8")).hexdigest(), 16)


class HashRing:
    def __init__(self, nodes: Iterable[str] = (), vnodes: int = 150):
        if vnodes < 1:
            raise ValueError("vnodes must be >= 1")
        self.vnodes = vnodes
        self._tokens: List[int] = []          # sorted ring positions
        self._token_owner: Dict[int, str] = {}
        self._nodes: set[str] = set()
        for node in nodes:
            self.add_node(node)

    # ------------------------------------------------------------ membership
    @property
    def nodes(self) -> List[str]:
        return sorted(self._nodes)

    def __contains__(self, node: str) -> bool:
        return node in self._nodes

    def __len__(self) -> int:
        return len(self._nodes)

    def _vnode_tokens(self, node: str) -> List[int]:
        return [sha1_int(f"{node}#vn{i}") for i in range(self.vnodes)]

    def add_node(self, node: str) -> None:
        if node in self._nodes:
            return
        self._nodes.add(node)
        for token in self._vnode_tokens(node):
            # SHA-1 collisions between vnodes are astronomically unlikely; keep first owner
            if token in self._token_owner:
                continue
            self._token_owner[token] = node
            bisect.insort(self._tokens, token)

    def remove_node(self, node: str) -> None:
        if node not in self._nodes:
            return
        self._nodes.discard(node)
        for token in self._vnode_tokens(node):
            if self._token_owner.get(token) == node:
                del self._token_owner[token]
                idx = bisect.bisect_left(self._tokens, token)
                if idx < len(self._tokens) and self._tokens[idx] == token:
                    self._tokens.pop(idx)

    def copy(self) -> "HashRing":
        clone = HashRing(vnodes=self.vnodes)
        clone._tokens = list(self._tokens)
        clone._token_owner = dict(self._token_owner)
        clone._nodes = set(self._nodes)
        return clone

    # ------------------------------------------------------------ lookup
    def lookup(self, key: str) -> Tuple[str, int]:
        """Return (owner node, token of the owning virtual node)."""
        if not self._tokens:
            raise LookupError("hash ring is empty")
        h = sha1_int(key)
        idx = bisect.bisect_right(self._tokens, h)   # first vnode clockwise (strictly after h)
        if idx == len(self._tokens):
            idx = 0                                  # wrap around the ring
        token = self._tokens[idx]
        return self._token_owner[token], token

    def owner(self, key: str) -> str:
        return self.lookup(key)[0]

    # ------------------------------------------------------------ analysis
    def ownership(self) -> Dict[str, float]:
        """Fraction of the hash space owned by each node.

        A vnode owns the arc (previous token, its token], because keys walk
        clockwise to the first token after them.
        """
        share = {n: 0 for n in self._nodes}
        if not self._tokens:
            return {}
        for i, token in enumerate(self._tokens):
            prev = self._tokens[i - 1] if i > 0 else self._tokens[-1] - RING_SIZE
            share[self._token_owner[token]] += token - prev
        return {n: v / RING_SIZE for n, v in share.items()}


def mod_n_owner(key: str, nodes: List[str]) -> str:
    """Naive `hash(key) mod N` placement, for comparison in the demo."""
    ordered = sorted(nodes)
    return ordered[sha1_int(key) % len(ordered)]


def movement_report(keys: Iterable[str], before: List[str], after: List[str],
                    vnodes: int = 150) -> Dict[str, object]:
    """How many keys change owner when membership goes from `before` to `after`,
    with consistent hashing vs hash-mod-N."""
    keys = list(keys)
    ring_before = HashRing(before, vnodes)
    ring_after = HashRing(after, vnodes)
    moved_ch = sum(1 for k in keys if ring_before.owner(k) != ring_after.owner(k))
    moved_mod = sum(1 for k in keys if mod_n_owner(k, before) != mod_n_owner(k, after))
    total = max(len(keys), 1)
    changed = len(set(before) ^ set(after))
    n_max = max(len(before), len(after))
    return {
        "keys": len(keys),
        "nodes_before": sorted(before),
        "nodes_after": sorted(after),
        "consistent_hashing_moved": moved_ch,
        "consistent_hashing_moved_pct": round(100 * moved_ch / total, 2),
        "mod_n_moved": moved_mod,
        "mod_n_moved_pct": round(100 * moved_mod / total, 2),
        "expected_k_over_n_pct": round(100 * changed / n_max, 2),
    }


def load_spread(keys: Iterable[str], nodes: List[str], vnodes: int) -> Dict[str, object]:
    """Keys per node for a given vnode count (shows why virtual nodes balance load)."""
    ring = HashRing(nodes, vnodes)
    counts = {n: 0 for n in nodes}
    for k in keys:
        counts[ring.owner(k)] += 1
    values = list(counts.values())
    mean = sum(values) / len(values)
    std = (sum((v - mean) ** 2 for v in values) / len(values)) ** 0.5
    return {
        "vnodes": vnodes,
        "keys_per_node": counts,
        "max_over_mean": round(max(values) / mean, 3) if mean else None,
        "stddev_pct_of_mean": round(100 * std / mean, 2) if mean else None,
    }


def owner_or_none(ring: Optional[HashRing], key: str) -> Optional[str]:
    try:
        return ring.owner(key) if ring else None
    except LookupError:
        return None
