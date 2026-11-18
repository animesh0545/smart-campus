"""Availability Service (owner: Animesh Bhargava).

Checks time-slot availability and finds conflicting reservations.
  * gRPC  AvailabilityService.Check  <- called by both booking services on every write
  * REST  GET /api/availability       <- public read path (k6 load test drives this)
  * REST  GET /api/availability/{resourceId}/{date}/schedule  <- day view + free slots
It asks the Partition Manager which DynamoDB node owns resourceId#date, then
runs a single-partition Query on that node. Stateless, scaled by an HPA.
"""
import logging
from contextlib import asynccontextmanager

import grpc
from boto3.dynamodb.conditions import Key
from fastapi import HTTPException, Query

from common import aws, grpc_util
from common.booking_core import TIME_RE, DATE_RE, partition_key
from common.grpc_util import campus_pb2 as pb
from common.grpc_util import campus_pb2_grpc as pb_grpc
from common.observability import create_app, setup_logging

log = setup_logging()

DAY_START, DAY_END = "08:00", "22:00"


def lookup_node(pk: str):
    reply = grpc_util.partition_stub().Lookup(pb.LookupRequest(key=pk), timeout=2.0,
                                              metadata=grpc_util.metadata())
    return reply.node_id, reply.table


def day_bookings(resource_id: str, date: str):
    pk = partition_key(resource_id, date)
    node, table = lookup_node(pk)
    items = aws.table(table).query(KeyConditionExpression=Key("pk").eq(pk))["Items"]
    active = [i for i in items if i.get("status") != "CANCELLED"]
    active.sort(key=lambda i: i["startTime"])
    return node, table, active


def find_conflicts(resource_id, date, start, end, exclude=""):
    node, _, items = day_bookings(resource_id, date)
    # two intervals overlap iff a.start < b.end and b.start < a.end
    conflicts = [i for i in items
                 if i.get("bookingId") != exclude and i["startTime"] < end and start < i["endTime"]]
    return node, conflicts


class AvailabilityServicer(pb_grpc.AvailabilityServiceServicer):
    def Check(self, request, context):
        try:
            node, conflicts = find_conflicts(request.resource_id, request.date, request.start_time,
                                             request.end_time, request.exclude_booking_id)
        except grpc.RpcError as exc:
            context.abort(grpc.StatusCode.UNAVAILABLE, f"partition manager: {exc.code().name}")
        return pb.AvailabilityReply(
            available=not conflicts,
            node_id=node,
            conflicts=[pb.Conflict(booking_id=c["bookingId"], start_time=c["startTime"],
                                   end_time=c["endTime"], user_id=c.get("userId", ""))
                       for c in conflicts],
        )


_grpc_server = None


@asynccontextmanager
async def lifespan(_app):
    global _grpc_server
    aws.ensure_local_tables()
    _grpc_server = grpc_util.serve(
        lambda s: pb_grpc.add_AvailabilityServiceServicer_to_server(AvailabilityServicer(), s))
    yield
    _grpc_server.stop(grace=5)


app = create_app("Availability Service", lifespan=lifespan)


def _validate(date, start=None, end=None):
    if not DATE_RE.match(date):
        raise HTTPException(400, "date must be YYYY-MM-DD")
    for t in (start, end):
        if t is not None and not TIME_RE.match(t):
            raise HTTPException(400, "times must be HH:MM")
    if start and end and start >= end:
        raise HTTPException(400, "startTime must be before endTime")


@app.get("/api/availability")
def check(resourceId: str = Query(...), date: str = Query(...),
          startTime: str = Query(...), endTime: str = Query(...)):
    _validate(date, startTime, endTime)
    try:
        node, conflicts = find_conflicts(resourceId, date, startTime, endTime)
    except grpc.RpcError as exc:
        raise HTTPException(503, f"partition manager unavailable ({exc.code().name})")
    return {
        "resourceId": resourceId, "date": date, "startTime": startTime, "endTime": endTime,
        "available": not conflicts, "partitionNode": node,
        "conflicts": [{"bookingId": c["bookingId"], "startTime": c["startTime"],
                       "endTime": c["endTime"], "userId": c.get("userId")} for c in conflicts],
    }


@app.get("/api/availability/{resource_id}/{date}/schedule")
def schedule(resource_id: str, date: str):
    _validate(date)
    try:
        node, table, items = day_bookings(resource_id, date)
    except grpc.RpcError as exc:
        raise HTTPException(503, f"partition manager unavailable ({exc.code().name})")
    free, cursor = [], DAY_START
    for b in items:
        if b["startTime"] > cursor:
            free.append({"from": cursor, "to": min(b["startTime"], DAY_END)})
        cursor = max(cursor, b["endTime"])
    if cursor < DAY_END:
        free.append({"from": cursor, "to": DAY_END})
    return {
        "resourceId": resource_id, "date": date, "partitionNode": node, "table": table,
        "booked": [{"bookingId": b["bookingId"], "startTime": b["startTime"], "endTime": b["endTime"],
                    "type": b.get("resourceType")} for b in items],
        "free": free,
    }
