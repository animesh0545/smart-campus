"""Room Booking Service (owner: Animesh Bhargava).

Create / modify / cancel reservations of rooms and labs. Public REST API via
the gateway at /api/rooms; calls Availability and the Partition Manager over
gRPC; publishes booking events to Kafka (MSK) and SQS. Scaled by an HPA.
"""
from common import aws, events
from common.booking_core import build_booking_app
from common.config import BOOKING_EVENTS_TOPIC
from common.observability import setup_logging

setup_logging()


def _startup():
    aws.ensure_local_tables()
    events.ensure_topics([BOOKING_EVENTS_TOPIC])


app = build_booking_app(kind="ROOM", prefix="/api/rooms", title="Room Booking Service", on_startup=_startup)
