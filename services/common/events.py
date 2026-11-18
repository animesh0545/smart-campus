"""Asynchronous messaging (requirement b: message queues).

Every booking change is published to:
  * Kafka topic `booking-events` (MSK): consumed by the Flink job (Usage Analytics)
  * SQS queue `booking-notifications`: triggers the Notification Lambda
Conflicts go to Kafka only (analytics counts them; users aren't notified).

Publishing is best-effort and never fails the HTTP request: the booking is
already stored, so a broker problem is logged and counted as a metric.
"""
import json
import logging
import threading
import uuid
from datetime import datetime, timezone
from typing import Optional

from . import aws, config
from .observability import EVENTS_PUBLISHED, correlation_id

log = logging.getLogger("events")

_producer = None
_producer_lock = threading.Lock()


def _kafka_producer():
    global _producer
    if not config.KAFKA_BOOTSTRAP:
        return None
    with _producer_lock:
        if _producer is None:
            from confluent_kafka import Producer

            _producer = Producer({
                "bootstrap.servers": config.KAFKA_BOOTSTRAP,
                "client.id": config.SERVICE_NAME,
                "linger.ms": 20,
                "acks": "all",
                "message.timeout.ms": 10000,
            })
        return _producer


def ensure_topics(topics, partitions: int = 3) -> None:
    """Create Kafka topics if missing (idempotent, safe to call on startup)."""
    if not config.KAFKA_BOOTSTRAP:
        log.warning("KAFKA_BOOTSTRAP not set: skipping topic creation")
        return
    try:
        from confluent_kafka.admin import AdminClient, NewTopic

        admin = AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP})
        existing = set(admin.list_topics(timeout=10).topics.keys())
        new = [NewTopic(t, num_partitions=partitions, replication_factor=config.KAFKA_REPLICATION)
               for t in topics if t not in existing]
        if not new:
            return
        for topic, fut in admin.create_topics(new).items():
            try:
                fut.result(timeout=15)
                log.info(f"created kafka topic {topic}")
            except Exception as exc:  # TOPIC_ALREADY_EXISTS when two pods race
                log.info(f"topic {topic}: {exc}")
    except Exception:
        log.exception("could not ensure kafka topics")


def build_event(event_type: str, booking: dict, resource_type: str) -> dict:
    duration = _minutes(booking.get("startTime"), booking.get("endTime"))
    return {
        "eventId": uuid.uuid4().hex,
        "type": event_type,               # CREATED | MODIFIED | CANCELLED | CONFLICT
        "bookingId": booking.get("bookingId"),
        "resourceId": booking.get("resourceId"),
        "resourceType": resource_type,    # ROOM | EQUIPMENT
        "userId": booking.get("userId"),
        "date": booking.get("date"),
        "startTime": booking.get("startTime"),
        "endTime": booking.get("endTime"),
        "durationMinutes": duration,
        "partitionNode": booking.get("node"),
        "source": config.SERVICE_NAME,
        "correlationId": correlation_id.get(),
        "ts": datetime.now(timezone.utc).isoformat(),
    }


def _minutes(start: Optional[str], end: Optional[str]) -> int:
    try:
        sh, sm = map(int, start.split(":"))
        eh, em = map(int, end.split(":"))
        return max(0, (eh * 60 + em) - (sh * 60 + sm))
    except Exception:
        return 0


def _delivery(err, msg):
    etype = "unknown"
    try:
        etype = json.loads(msg.value()).get("type", "unknown")
    except Exception:
        pass
    if err is not None:
        EVENTS_PUBLISHED.labels(config.SERVICE_NAME, "kafka", etype, "error").inc()
        log.error(f"kafka delivery failed: {err}")
    else:
        EVENTS_PUBLISHED.labels(config.SERVICE_NAME, "kafka", etype, "ok").inc()


def publish_kafka(event: dict) -> None:
    producer = _kafka_producer()
    if producer is None:
        EVENTS_PUBLISHED.labels(config.SERVICE_NAME, "kafka", event["type"], "skipped").inc()
        return
    try:
        producer.produce(config.BOOKING_EVENTS_TOPIC, key=event.get("resourceId") or "",
                         value=json.dumps(event), on_delivery=_delivery)
        producer.poll(0)
    except Exception:
        EVENTS_PUBLISHED.labels(config.SERVICE_NAME, "kafka", event["type"], "error").inc()
        log.exception("kafka produce failed")


def publish_sqs(event: dict) -> None:
    if not config.SQS_QUEUE_URL:
        EVENTS_PUBLISHED.labels(config.SERVICE_NAME, "sqs", event["type"], "skipped").inc()
        return
    try:
        aws.sqs().send_message(QueueUrl=config.SQS_QUEUE_URL, MessageBody=json.dumps(event))
        EVENTS_PUBLISHED.labels(config.SERVICE_NAME, "sqs", event["type"], "ok").inc()
    except Exception:
        EVENTS_PUBLISHED.labels(config.SERVICE_NAME, "sqs", event["type"], "error").inc()
        log.exception("sqs send failed")


def publish(event: dict, notify: bool = True) -> None:
    """Kafka always (analytics); SQS when the user should get a notification."""
    publish_kafka(event)
    if notify:
        publish_sqs(event)
    log.info(f"event {event['type']} {event.get('bookingId')}",
             extra={"fields": {"eventType": event["type"], "resourceId": event.get("resourceId"),
                               "bookingId": event.get("bookingId")}})


def flush(timeout: float = 5.0) -> None:
    if _producer is not None:
        _producer.flush(timeout)
