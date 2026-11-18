"""In-memory stand-in for kazoo's KazooClient, covering the subset the
Partition Manager uses: start/stop, ensure_path, get_children, create, delete,
ChildrenWatch (fires immediately, then on every child change) and Lock.
Lets tests (and a ZooKeeper-less local run) exercise the membership logic."""
import threading

from kazoo.exceptions import LockTimeout, NodeExistsError, NoNodeError


class FakeKazoo:
    def __init__(self, *args, **kwargs):
        self.nodes = {"/": b""}
        self.watches = {}
        self.state = "CONNECTED"
        self._lock = threading.RLock()
        self._locks = {}

    def start(self, timeout=None):
        self.state = "CONNECTED"

    def stop(self):
        self.state = "LOST"

    def ensure_path(self, path):
        parts = path.strip("/").split("/")
        for i in range(1, len(parts) + 1):
            self.nodes.setdefault("/" + "/".join(parts[:i]), b"")

    def _children(self, path):
        prefix = path.rstrip("/") + "/"
        return sorted(p[len(prefix):] for p in self.nodes if p.startswith(prefix) and "/" not in p[len(prefix):])

    def get_children(self, path):
        if path not in self.nodes:
            raise NoNodeError(path)
        return self._children(path)

    def _fire(self, parent):
        for fn in list(self.watches.get(parent, [])):
            fn(self._children(parent))

    def create(self, path, value=b""):
        with self._lock:
            if path in self.nodes:
                raise NodeExistsError(path)
            parent = path.rsplit("/", 1)[0] or "/"
            self.ensure_path(parent)
            self.nodes[path] = value
        self._fire(parent)

    def delete(self, path):
        with self._lock:
            if path not in self.nodes:
                raise NoNodeError(path)
            del self.nodes[path]
        self._fire(path.rsplit("/", 1)[0] or "/")

    def get(self, path):
        return self.nodes[path], None

    def ChildrenWatch(self, path):
        def register(fn):
            self.watches.setdefault(path, []).append(fn)
            fn(self._children(path))
            return fn
        return register

    def Lock(self, path, identifier=None):
        lk = self._locks.setdefault(path, threading.Lock())

        class _L:
            def acquire(self_inner, timeout=None):
                if not lk.acquire(timeout=timeout if timeout is not None else -1):
                    raise LockTimeout(path)
                return True

            def release(self_inner):
                lk.release()
        return _L()
