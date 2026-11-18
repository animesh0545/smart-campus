"""Equipment Booking Service (owner: Shreyansh Kedawat).

Reservations for shared equipment (projectors, oscilloscopes, VR kits...).
Same booking life cycle as rooms, but its own service, image and deployment,
under /api/equipment. Calls Availability + Partition Manager over gRPC and
emits the same events, tagged resourceType=EQUIPMENT.
"""
from common import aws, events
from common.booking_core import build_booking_app
from common.config import BOOKING_EVENTS_TOPIC
from common.observability import setup_logging

setup_logging()


def _startup():
    aws.ensure_local_tables()
    events.ensure_topics([BOOKING_EVENTS_TOPIC])


app = build_booking_app(kind="EQUIPMENT", prefix="/api/equipment", title="Equipment Booking Service", on_startup=_startup)
