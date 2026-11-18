"""Usage Analytics (owner: Shreyansh Kedawat).

The stream processing itself is the Flink job on EMR (analytics-flink/):
    booking-events --(1-minute tumbling window per resource)--> analytics-results
This EKS component is the service's serving side:
  * a Kafka consumer that persists every window result from `analytics-results`
    into DynamoDB (high-throughput semi-structured data, requirement g)
  * REST API under /api/analytics: room occupancy, equipment utilisation,
    booking conflicts.
"""
import json
import logging
import threading
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from boto3.dynamodb.conditions import Key
from fastapi import HTTPException
from prometheus_client import Counter

from common import aws, config, events
from common.observability import create_app, log_fields, setup_logging

log = setup_logging()
RESULTS_CONSUMED = Counter("analytics_results_consumed_total", "Window results stored", ["service"])
RECENT = deque(maxlen=100)
_stop = threading.Event()


def _to_dynamo(value):
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, dict):
        return {k: _to_dynamo(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_to_dynamo(v) for v in value]
    return value


def _from_dynamo(value):
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, dict):
        return {k: _from_dynamo(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_from_dynamo(v) for v in value]
    return value


def store_result(result: dict) -> None:
    if not result.get("resourceId") or not result.get("windowStart"):
        return
    aws.table(config.ANALYTICS_TABLE).put_item(Item=_to_dynamo(result))
    RECENT.appendleft(result)
    RESULTS_CONSUMED.labels(config.SERVICE_NAME).inc()
    log.info("window result stored", extra=log_fields(resourceId=result["resourceId"],
                                                      windowStart=result["windowStart"],
                                                      bookings=result.get("bookings"),
                                                      conflicts=result.get("conflicts")))


def consume_forever() -> None:
    from confluent_kafka import Consumer

    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "group.id": "usage-analytics-sink",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([config.ANALYTICS_TOPIC])
    log.info(f"consuming {config.ANALYTICS_TOPIC}")
    try:
        while not _stop.is_set():
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.warning(f"kafka error: {msg.error()}")
                continue
            try:
                store_result(json.loads(msg.value()))
            except Exception:
                log.exception("could not store analytics result")
    finally:
        consumer.close()


@asynccontextmanager
async def lifespan(_app):
    aws.ensure_local_tables()
    if config.KAFKA_BOOTSTRAP:
        events.ensure_topics([config.BOOKING_EVENTS_TOPIC, config.ANALYTICS_TOPIC])
        threading.Thread(target=consume_forever, name="analytics-consumer", daemon=True).start()
    else:
        log.warning("KAFKA_BOOTSTRAP not set: analytics consumer disabled")
    yield
    _stop.set()


app = create_app("Usage Analytics", lifespan=lifespan)


def _scan_since(minutes: int):
    since = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()
    tbl = aws.table(config.ANALYTICS_TABLE)
    items, kwargs = [], {"FilterExpression": "windowStart >= :s", "ExpressionAttributeValues": {":s": since}}
    while True:
        resp = tbl.scan(**kwargs)
        items.extend(resp["Items"])
        if "LastEvaluatedKey" not in resp:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    return [_from_dynamo(i) for i in items]


@app.get("/api/analytics/live")
def live(limit: int = 20):
    """Most recent window results consumed by this pod (straight off Kafka)."""
    return {"results": list(RECENT)[:limit]}


@app.get("/api/analytics/resources/{resource_id}")
def by_resource(resource_id: str, limit: int = 60):
    resp = aws.table(config.ANALYTICS_TABLE).query(
        KeyConditionExpression=Key("resourceId").eq(resource_id), ScanIndexForward=False, Limit=limit)
    windows = [_from_dynamo(i) for i in resp["Items"]]
    if not windows:
        raise HTTPException(404, "no analytics for this resource yet")
    return {"resourceId": resource_id, "windows": windows}


@app.get("/api/analytics/summary")
def summary(minutes: int = 60, top: int = 10):
    """Room occupancy, equipment utilisation and booking conflicts over the last N minutes."""
    windows = _scan_since(minutes)
    per_resource = {}
    for w in windows:
        r = per_resource.setdefault(w["resourceId"], {
            "resourceId": w["resourceId"], "resourceType": w.get("resourceType"), "bookings": 0,
            "cancellations": 0, "conflicts": 0, "occupancyMinutes": 0, "windows": 0})
        for f in ("bookings", "cancellations", "conflicts", "occupancyMinutes"):
            r[f] += int(w.get(f, 0) or 0)
        r["windows"] += 1
    rows = list(per_resource.values())
    rooms = [r for r in rows if r["resourceType"] == "ROOM"]
    equipment = [r for r in rows if r["resourceType"] == "EQUIPMENT"]
    return {
        "minutes": minutes,
        "windowsAnalysed": len(windows),
        "totals": {
            "bookings": sum(r["bookings"] for r in rows),
            "cancellations": sum(r["cancellations"] for r in rows),
            "conflicts": sum(r["conflicts"] for r in rows),
            "roomOccupancyMinutesBooked": sum(r["occupancyMinutes"] for r in rooms),
            "equipmentMinutesBooked": sum(r["occupancyMinutes"] for r in equipment),
        },
        "topRoomsByOccupancy": sorted(rooms, key=lambda r: -r["occupancyMinutes"])[:top],
        "topEquipmentByUtilisation": sorted(equipment, key=lambda r: -r["occupancyMinutes"])[:top],
        "mostConflicted": sorted(rows, key=lambda r: -r["conflicts"])[:top],
    }
