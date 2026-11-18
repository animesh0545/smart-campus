"""Booking logic shared by Room Booking and Equipment Booking.

They are two separate microservices (separate images, deployments, scaling
and owners) with the same booking life cycle, so the code is written once:

  POST   /bookings                               create  -> CREATED event
  GET    /bookings/{resourceId}/{date}           list a resource's day
  GET    /bookings/{resourceId}/{date}/{id}      one booking
  PUT    /bookings/{resourceId}/{date}/{id}      modify  -> MODIFIED event
  DELETE /bookings/{resourceId}/{date}/{id}      cancel  -> CANCELLED event
  GET    /users/{userId}/bookings                scatter/gather over all partitions

Write path (shows every communication mechanism):
  REST  -> Resource Registry + User Service    (validate references)
  gRPC  -> Availability.Check                  (conflict detection)
  gRPC  -> PartitionService.Lookup             (consistent hashing: which table)
  DynamoDB put on the owning partition table
  Kafka booking-events (analytics) + SQS (notification Lambda), in the background
"""
import logging
import re
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

import grpc
from boto3.dynamodb.conditions import Key
from fastapi import BackgroundTasks, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from . import aws, events, grpc_util, peers
from .grpc_util import campus_pb2 as pb
from .observability import create_app, log_fields

log = logging.getLogger("booking")

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
TIME_RE = re.compile(r"^([01]\d|2[0-3]):[0-5]\d$")

ALLOWED_TYPES = {"ROOM": {"ROOM", "LAB"}, "EQUIPMENT": {"EQUIPMENT"}}


class BookingIn(BaseModel):
    resourceId: str = Field(..., examples=["ROOM-001"])
    userId: str = Field(..., examples=["U001"])
    date: str = Field(..., examples=["2026-11-18"])
    startTime: str = Field(..., examples=["10:00"])
    endTime: str = Field(..., examples=["11:00"])
    purpose: str = ""


class BookingUpdate(BaseModel):
    date: Optional[str] = None
    startTime: Optional[str] = None
    endTime: Optional[str] = None
    purpose: Optional[str] = None


def partition_key(resource_id: str, date: str) -> str:
    """Compound key (lecture: 'hybrid' partitioning): resourceId#date is hashed;
    the sort key startTime#bookingId orders a resource's day inside the partition."""
    return f"{resource_id}#{date}"


def _validate_slot(date: str, start: str, end: str) -> None:
    if not DATE_RE.match(date or ""):
        raise HTTPException(400, "date must be YYYY-MM-DD")
    if not TIME_RE.match(start or "") or not TIME_RE.match(end or ""):
        raise HTTPException(400, "startTime/endTime must be HH:MM (24h)")
    if start >= end:
        raise HTTPException(400, "startTime must be before endTime")


def _public(item: dict) -> dict:
    return {k: v for k, v in item.items() if k not in ("pk", "sk")}


def build_booking_app(kind: str, prefix: str, title: str, on_startup=None):
    allowed = ALLOWED_TYPES[kind]

    @asynccontextmanager
    async def lifespan(_app):
        if on_startup:
            on_startup()
        yield
        events.flush()

    app = create_app(title, lifespan=lifespan)

    # ------------------------------------------------------------ helpers
    def route(pk: str):
        """Ask the Partition Manager (routing tier) which node owns the key."""
        try:
            reply = grpc_util.partition_stub().Lookup(
                pb.LookupRequest(key=pk), timeout=2.0, metadata=grpc_util.metadata())
        except grpc.RpcError as exc:
            raise HTTPException(503, f"partition manager unavailable ({exc.code().name})")
        return reply.node_id, reply.table

    def check_availability(resource_id, date, start, end, exclude=""):
        try:
            return grpc_util.availability_stub().Check(
                pb.AvailabilityRequest(resource_id=resource_id, date=date, start_time=start,
                                       end_time=end, exclude_booking_id=exclude),
                timeout=3.0, metadata=grpc_util.metadata())
        except grpc.RpcError as exc:
            raise HTTPException(503, f"availability service unavailable ({exc.code().name})")

    def validate_refs(resource_id: str, user_id: Optional[str]) -> dict:
        try:
            resource = peers.get_resource(resource_id)
            user = peers.get_user(user_id) if user_id else {"id": None}
        except peers.PeerUnavailable as exc:
            raise HTTPException(503, str(exc))
        if resource is None:
            raise HTTPException(404, f"resource {resource_id} not found")
        if resource.get("type") not in allowed:
            raise HTTPException(400, f"{resource_id} is a {resource.get('type')}, not bookable here")
        if not resource.get("available", True):
            raise HTTPException(409, f"{resource_id} is marked unavailable")
        if user is None:
            raise HTTPException(404, f"user {user_id} not found")
        return resource

    def find(resource_id: str, date: str, booking_id: str):
        pk = partition_key(resource_id, date)
        node, table = route(pk)
        items = aws.table(table).query(KeyConditionExpression=Key("pk").eq(pk))["Items"]
        for item in items:
            if item.get("bookingId") == booking_id and item.get("resourceType") == kind:
                return item, node, table
        raise HTTPException(404, f"booking {booking_id} not found")

    def conflict_response(body: dict, reply, bg: BackgroundTasks):
        ev = events.build_event("CONFLICT", {**body, "bookingId": None, "node": reply.node_id}, kind)
        bg.add_task(events.publish, ev, False)  # analytics only, no user notification
        log.info("booking conflict", extra=log_fields(resourceId=body["resourceId"], date=body["date"],
                                                       conflicts=len(reply.conflicts)))
        return JSONResponse(status_code=409, background=bg, content={
            "error": "requested slot conflicts with existing bookings",
            "conflicts": [{"bookingId": c.booking_id, "startTime": c.start_time,
                           "endTime": c.end_time} for c in reply.conflicts],
            "partitionNode": reply.node_id,
        })

    # ------------------------------------------------------------ routes
    @app.post(f"{prefix}/bookings", status_code=201)
    def create_booking(body: BookingIn, bg: BackgroundTasks):
        _validate_slot(body.date, body.startTime, body.endTime)
        resource = validate_refs(body.resourceId, body.userId)

        reply = check_availability(body.resourceId, body.date, body.startTime, body.endTime)
        if not reply.available:
            return conflict_response(body.model_dump(), reply, bg)

        pk = partition_key(body.resourceId, body.date)
        node, table = route(pk)
        booking_id = uuid.uuid4().hex[:12]
        item = {
            "pk": pk,
            "sk": f"{body.startTime}#{booking_id}",
            "bookingId": booking_id,
            "resourceId": body.resourceId,
            "resourceName": resource.get("name", body.resourceId),
            "resourceType": kind,
            "userId": body.userId,
            "date": body.date,
            "startTime": body.startTime,
            "endTime": body.endTime,
            "purpose": body.purpose,
            "status": "CONFIRMED",
            "createdAt": datetime.now(timezone.utc).isoformat(),
            "node": node,
        }
        aws.table(table).put_item(Item=item)
        bg.add_task(events.publish, events.build_event("CREATED", item, kind))
        log.info("booking created", extra=log_fields(bookingId=booking_id, pk=pk, node=node, table=table))
        return JSONResponse(status_code=201, background=bg, content=_public(item))

    @app.get(f"{prefix}/bookings/{{resource_id}}/{{date}}")
    def list_bookings(resource_id: str, date: str, include_cancelled: bool = False):
        pk = partition_key(resource_id, date)
        node, table = route(pk)
        items = aws.table(table).query(KeyConditionExpression=Key("pk").eq(pk))["Items"]
        items = [i for i in items if i.get("resourceType") == kind
                 and (include_cancelled or i.get("status") != "CANCELLED")]
        return {"resourceId": resource_id, "date": date, "partitionNode": node, "table": table,
                "bookings": [_public(i) for i in items]}

    @app.get(f"{prefix}/bookings/{{resource_id}}/{{date}}/{{booking_id}}")
    def get_booking(resource_id: str, date: str, booking_id: str):
        item, node, _ = find(resource_id, date, booking_id)
        return {**_public(item), "partitionNode": node}

    @app.put(f"{prefix}/bookings/{{resource_id}}/{{date}}/{{booking_id}}")
    def modify_booking(resource_id: str, date: str, booking_id: str, body: BookingUpdate,
                       bg: BackgroundTasks):
        old, old_node, old_table = find(resource_id, date, booking_id)
        if old.get("status") == "CANCELLED":
            raise HTTPException(409, "booking is cancelled")
        new_date = body.date or old["date"]
        new_start = body.startTime or old["startTime"]
        new_end = body.endTime or old["endTime"]
        _validate_slot(new_date, new_start, new_end)

        reply = check_availability(resource_id, new_date, new_start, new_end, exclude=booking_id)
        if not reply.available:
            return conflict_response({**old, "date": new_date, "startTime": new_start,
                                      "endTime": new_end}, reply, bg)

        new_pk = partition_key(resource_id, new_date)
        new_node, new_table = route(new_pk)  # a date change may move the booking to another node
        new_item = {**old, "pk": new_pk, "sk": f"{new_start}#{booking_id}", "date": new_date,
                    "startTime": new_start, "endTime": new_end, "node": new_node,
                    "purpose": body.purpose if body.purpose is not None else old.get("purpose", ""),
                    "modifiedAt": datetime.now(timezone.utc).isoformat()}
        aws.table(new_table).put_item(Item=new_item)
        if (new_item["pk"], new_item["sk"]) != (old["pk"], old["sk"]) or new_table != old_table:
            aws.table(old_table).delete_item(Key={"pk": old["pk"], "sk": old["sk"]})
        bg.add_task(events.publish, events.build_event("MODIFIED", new_item, kind))
        log.info("booking modified", extra=log_fields(bookingId=booking_id, fromNode=old_node, toNode=new_node))
        return JSONResponse(status_code=200, background=bg,
                            content={**_public(new_item), "movedFromNode": old_node if old_node != new_node else None})

    @app.delete(f"{prefix}/bookings/{{resource_id}}/{{date}}/{{booking_id}}")
    def cancel_booking(resource_id: str, date: str, booking_id: str, bg: BackgroundTasks):
        item, node, table = find(resource_id, date, booking_id)
        if item.get("status") == "CANCELLED":
            return _public(item)
        aws.table(table).update_item(
            Key={"pk": item["pk"], "sk": item["sk"]},
            UpdateExpression="SET #s = :c, cancelledAt = :t",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":c": "CANCELLED", ":t": datetime.now(timezone.utc).isoformat()},
        )
        item["status"] = "CANCELLED"
        bg.add_task(events.publish, events.build_event("CANCELLED", item, kind))
        log.info("booking cancelled", extra=log_fields(bookingId=booking_id, node=node))
        return JSONResponse(status_code=200, background=bg, content=_public(item))

    @app.get(f"{prefix}/users/{{user_id}}/bookings")
    def bookings_for_user(user_id: str, limit: int = 50):
        """userId is not the partition key, so this is a scatter/gather query over
        every partition node (lecture: secondary index partitioned by document)."""
        try:
            ring = grpc_util.partition_stub().GetRing(pb.Empty(), timeout=2.0, metadata=grpc_util.metadata())
        except grpc.RpcError as exc:
            raise HTTPException(503, f"partition manager unavailable ({exc.code().name})")
        found, per_node = [], {}
        for node in ring.nodes:
            resp = aws.table(node.table).scan(
                FilterExpression="userId = :u AND resourceType = :k",
                ExpressionAttributeValues={":u": user_id, ":k": kind})
            per_node[node.node_id] = len(resp["Items"])
            found.extend(_public(i) for i in resp["Items"])
        found.sort(key=lambda b: (b["date"], b["startTime"]))
        return {"userId": user_id, "nodesQueried": per_node, "bookings": found[:limit]}

    return app
