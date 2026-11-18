"""Synchronous REST calls to peer services (requirement b: REST), with a
small TTL cache so load tests are not dominated by look-ups that rarely change."""
import logging
import threading
import time
from typing import Optional

import httpx

from . import config
from .observability import correlation_id

log = logging.getLogger("peers")
_client = httpx.Client(timeout=httpx.Timeout(3.0, connect=2.0))
_cache: dict = {}
_lock = threading.Lock()
TTL_SECONDS = 60


class PeerUnavailable(Exception):
    pass


def _get_cached(url: str) -> Optional[dict]:
    now = time.monotonic()
    with _lock:
        hit = _cache.get(url)
        if hit and now - hit[0] < TTL_SECONDS:
            return hit[1]
    try:
        resp = _client.get(url, headers={"x-correlation-id": correlation_id.get()})
    except httpx.HTTPError as exc:
        raise PeerUnavailable(f"{url}: {exc}") from exc
    if resp.status_code == 404:
        return None
    if resp.status_code >= 500:
        raise PeerUnavailable(f"{url}: HTTP {resp.status_code}")
    body = resp.json()
    with _lock:
        _cache[url] = (now, body)
    return body


def get_resource(resource_id: str) -> Optional[dict]:
    return _get_cached(f"{config.RESOURCE_REGISTRY_URL}/api/resources/{resource_id}")


def get_user(user_id: str) -> Optional[dict]:
    return _get_cached(f"{config.USER_SERVICE_URL}/api/users/{user_id}")
