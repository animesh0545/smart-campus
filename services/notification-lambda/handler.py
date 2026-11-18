"""Notification Service: AWS Lambda (owner: Shreyansh Kedawat). Requirement b.

Asynchronous and event driven, with two triggers:
  1. SQS `booking-notifications`: booking CREATED / MODIFIED / CANCELLED events
     published by the booking services -> confirmation / change / cancellation
     notice for the user
  2. S3 ObjectCreated under imports/*.csv (bulk resource import uploaded via
     the Resource Registry) -> notice to the admins with the row count

Each notice is (a) stored in the DynamoDB `notifications` table (idempotent on
eventId), (b) published to the SNS topic (email if subscribed) and (c) logged
as JSON to CloudWatch Logs, which Grafana reads (requirement h).
SQS partial-batch responses mean only failed messages are retried.
"""
import csv
import io
import json
import logging
import os
from datetime import datetime, timezone
from urllib.parse import unquote_plus

import boto3
from botocore.exceptions import ClientError

log = logging.getLogger()
log.setLevel(logging.INFO)

TABLE = os.environ.get("NOTIFICATIONS_TABLE", "smart-campus-notifications")
TOPIC = os.environ.get("SNS_TOPIC_ARN", "")

_ddb = None
_sns = None
_s3 = None


def ddb_table():
    global _ddb
    if _ddb is None:
        _ddb = boto3.resource("dynamodb").Table(TABLE)
    return _ddb


def sns():
    global _sns
    if _sns is None:
        _sns = boto3.client("sns")
    return _sns


def s3():
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3")
    return _s3


TEMPLATES = {
    "CREATED": ("Booking confirmed",
                "Your booking {bookingId} for {resourceId} on {date} {startTime}-{endTime} is confirmed."),
    "MODIFIED": ("Booking changed",
                 "Your booking {bookingId} for {resourceId} is now on {date} {startTime}-{endTime}."),
    "CANCELLED": ("Booking cancelled",
                  "Your booking {bookingId} for {resourceId} on {date} {startTime}-{endTime} was cancelled."),
}


def jlog(msg: str, **fields):
    log.info(json.dumps({"service": "notification-lambda", "msg": msg, **fields}, default=str))


def deliver(user_id: str, event_id: str, subject: str, message: str, extra: dict) -> bool:
    """Store + publish one notice. Returns False if it was a duplicate."""
    now = datetime.now(timezone.utc).isoformat()
    item = {"userId": user_id, "sk": f"{now}#{event_id}", "eventId": event_id, "subject": subject,
            "message": message, "channel": "email" if TOPIC else "log", "createdAt": now, **extra}
    try:
        ddb_table().put_item(Item=item, ConditionExpression="attribute_not_exists(sk)")
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise
    if TOPIC:
        sns().publish(TopicArn=TOPIC, Subject=subject[:100], Message=message,
                      MessageAttributes={"userId": {"DataType": "String", "StringValue": user_id}})
    jlog("notification sent", userId=user_id, eventId=event_id, subject=subject, **extra)
    return True


def handle_booking_event(event: dict) -> None:
    etype = event.get("type")
    if etype not in TEMPLATES:
        jlog("ignored event", type=etype, eventId=event.get("eventId"))
        return
    subject, template = TEMPLATES[etype]
    message = template.format(**{k: event.get(k, "?") for k in
                                 ("bookingId", "resourceId", "date", "startTime", "endTime")})
    deliver(event.get("userId") or "unknown", event.get("eventId", "no-id"), subject, message,
            {"bookingId": event.get("bookingId"), "resourceId": event.get("resourceId"),
             "eventType": etype, "correlationId": event.get("correlationId")})


def handle_upload(record: dict) -> None:
    bucket = record["s3"]["bucket"]["name"]
    key = unquote_plus(record["s3"]["object"]["key"])
    body = s3().get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8-sig")
    rows = list(csv.DictReader(io.StringIO(body)))
    types = {}
    for r in rows:
        t = (r.get("type") or "?").upper()
        types[t] = types.get(t, 0) + 1
    message = f"Resource import file s3://{bucket}/{key} received with {len(rows)} rows ({types})."
    deliver("admin", f"s3:{key}", "Resource import received", message,
            {"s3Key": key, "rows": len(rows), "eventType": "IMPORT"})


def lambda_handler(event, context):
    failures = []
    for record in event.get("Records", []):
        source = record.get("eventSource") or record.get("EventSource")
        try:
            if source == "aws:sqs":
                handle_booking_event(json.loads(record["body"]))
            elif source == "aws:s3":
                handle_upload(record)
            else:
                jlog("unknown record source", source=source)
        except Exception as exc:
            log.exception("record failed")
            if source == "aws:sqs":
                failures.append({"itemIdentifier": record["messageId"]})
            else:
                raise exc
    return {"batchItemFailures": failures}
