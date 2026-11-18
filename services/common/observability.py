"""Logging, metrics and the FastAPI app factory shared by all services.

* Logs: one JSON object per line on stdout. Alloy on EKS ships them to Loki
  (requirement h). Each line carries the service name and a correlation id
  that is passed between services over HTTP headers and gRPC metadata.
* Metrics: Prometheus counters/histograms on /metrics, scraped through a
  PodMonitor (RPS, error rate, latency panels in Grafana).
"""
import contextvars
import json
import logging
import sys
import time
import uuid
from datetime import datetime, timezone

from fastapi import FastAPI, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

from . import config

correlation_id: contextvars.ContextVar[str] = contextvars.ContextVar("correlation_id", default="-")

# ------------------------------------------------------------------ logging
class _ContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.cid = correlation_id.get()
        return True


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "service": config.SERVICE_NAME,
            "logger": record.name,
            "cid": getattr(record, "cid", "-"),
            "msg": record.getMessage(),
        }
        fields = getattr(record, "fields", None)
        if isinstance(fields, dict):
            payload.update(fields)
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    root = logging.getLogger()
    root.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    handler.addFilter(_ContextFilter())
    root.addHandler(handler)
    root.setLevel(level)
    for noisy in ("uvicorn.access", "kazoo.client", "botocore", "urllib3", "httpx"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    return logging.getLogger(config.SERVICE_NAME)


def log_fields(**fields) -> dict:
    """Usage: log.info("booking created", extra=log_fields(bookingId=...))"""
    return {"fields": fields}


# ------------------------------------------------------------------ metrics
HTTP_REQUESTS = Counter(
    "http_requests_total", "HTTP requests handled", ["service", "method", "route", "status"]
)
HTTP_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency",
    ["service", "method", "route"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)
GRPC_REQUESTS = Counter(
    "grpc_server_requests_total", "gRPC requests handled", ["service", "method", "code"]
)
GRPC_LATENCY = Histogram(
    "grpc_server_handling_seconds",
    "gRPC handler latency",
    ["service", "method"],
    buckets=(0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
)
EVENTS_PUBLISHED = Counter(
    "events_published_total", "Events published to brokers", ["service", "broker", "type", "result"]
)

_SKIP_ROUTES = {"/metrics", "/health"}


def create_app(title: str, **kwargs) -> FastAPI:
    """FastAPI app with correlation ids, access logs, metrics and /health."""
    app = FastAPI(title=title, **kwargs)
    log = logging.getLogger("access")

    @app.middleware("http")
    async def _observe(request: Request, call_next):
        cid = request.headers.get("x-correlation-id") or uuid.uuid4().hex[:16]
        token = correlation_id.set(cid)
        start = time.perf_counter()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            response.headers["x-correlation-id"] = cid
            return response
        finally:
            elapsed = time.perf_counter() - start
            route = request.scope.get("route")
            path = getattr(route, "path", "unmatched")
            if path not in _SKIP_ROUTES:
                HTTP_REQUESTS.labels(config.SERVICE_NAME, request.method, path, str(status)).inc()
                HTTP_LATENCY.labels(config.SERVICE_NAME, request.method, path).observe(elapsed)
                level = logging.WARNING if status >= 500 else logging.INFO
                log.log(
                    level,
                    f"{request.method} {request.url.path} -> {status}",
                    extra=log_fields(method=request.method, route=path, status=status,
                                     duration_ms=round(elapsed * 1000, 1)),
                )
            correlation_id.reset(token)

    @app.get("/health", include_in_schema=False)
    def health():
        return {"status": "UP", "service": config.SERVICE_NAME}

    @app.get("/metrics", include_in_schema=False)
    def metrics():
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return app
