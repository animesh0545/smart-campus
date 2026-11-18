"""Resource Registry (owner: Animesh Bhargava).

Rooms, labs and equipment with availability attributes (building, capacity,
features, opening hours, available flag). Own PostgreSQL database `resources`
on RDS. Bulk import: POST /api/resources/import with a CSV file
  1. stores the raw file in S3 under imports/   -> S3 event triggers the
     Notification Lambda (serverless, event driven)
  2. upserts the rows into PostgreSQL
"""
import csv
import io
import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import File, HTTPException, UploadFile
from pydantic import BaseModel, Field

from common import aws, config, db
from common.observability import create_app, log_fields, setup_logging

log = setup_logging()
DB = "resources"
TYPES = ("ROOM", "LAB", "EQUIPMENT")
BUILDINGS = ["NAB", "FD-I", "FD-II", "FD-III", "LTC"]


def _seed_rows():
    rows = []
    for i in range(1, 25):
        rows.append((f"ROOM-{i:03d}", f"{BUILDINGS[i % 5]} Room {100 + i}", "ROOM", BUILDINGS[i % 5],
                     [40, 60, 120, 200][i % 4], ["projector", "ac"] if i % 2 else ["whiteboard"]))
    for i in range(1, 7):
        rows.append((f"LAB-{i:03d}", f"Computing Lab {i}", "LAB", "NAB", 60, ["pcs", "projector"]))
    equipment = ["Projector", "Oscilloscope", "VR Headset", "3D Printer", "DSLR Camera",
                 "Portable PA System", "Logic Analyzer", "Drone Kit", "Spectrum Analyzer", "Laptop Cart"]
    for i, name in enumerate(equipment, start=1):
        rows.append((f"EQ-{i:03d}", name, "EQUIPMENT", BUILDINGS[i % 5], 1, ["portable"]))
    return rows


def init_schema():
    db.ensure_database(DB)
    db.query(DB, """
        CREATE TABLE IF NOT EXISTS resources (
            id          TEXT PRIMARY KEY,
            name        TEXT NOT NULL,
            type        TEXT NOT NULL CHECK (type IN ('ROOM','LAB','EQUIPMENT')),
            building    TEXT,
            capacity    INTEGER NOT NULL DEFAULT 1,
            features    JSONB NOT NULL DEFAULT '[]',
            open_time   TEXT NOT NULL DEFAULT '08:00',
            close_time  TEXT NOT NULL DEFAULT '22:00',
            available   BOOLEAN NOT NULL DEFAULT TRUE,
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        )""")
    for rid, name, rtype, building, cap, features in _seed_rows():
        db.query(DB, "INSERT INTO resources (id, name, type, building, capacity, features) "
                     "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (id) DO NOTHING",
                 (rid, name, rtype, building, cap, json.dumps(features)))
    log.info("resources schema ready")


@asynccontextmanager
async def lifespan(_app):
    init_schema()
    yield


app = create_app("Resource Registry", lifespan=lifespan)


class ResourceIn(BaseModel):
    id: str = Field(..., examples=["ROOM-101"])
    name: str
    type: str = Field(..., examples=["ROOM"])
    building: Optional[str] = None
    capacity: int = 1
    features: List[str] = []
    open_time: str = "08:00"
    close_time: str = "22:00"
    available: bool = True


class ResourcePatch(BaseModel):
    name: Optional[str] = None
    building: Optional[str] = None
    capacity: Optional[int] = None
    features: Optional[List[str]] = None
    open_time: Optional[str] = None
    close_time: Optional[str] = None
    available: Optional[bool] = None


def _upsert(r: ResourceIn):
    if r.type not in TYPES:
        raise HTTPException(400, f"type must be one of {TYPES}")
    return db.query(DB, """
        INSERT INTO resources (id, name, type, building, capacity, features, open_time, close_time, available)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name, type=EXCLUDED.type, building=EXCLUDED.building,
            capacity=EXCLUDED.capacity, features=EXCLUDED.features, open_time=EXCLUDED.open_time,
            close_time=EXCLUDED.close_time, available=EXCLUDED.available, updated_at=now()
        RETURNING *""",
        (r.id, r.name, r.type, r.building, r.capacity, json.dumps(r.features), r.open_time, r.close_time,
         r.available), one=True)


@app.get("/api/resources")
def list_resources(type: Optional[str] = None, building: Optional[str] = None,
                   available: Optional[bool] = None, min_capacity: int = 0):
    sql, params = "SELECT * FROM resources WHERE capacity >= %s", [min_capacity]
    if type:
        sql += " AND type = %s"
        params.append(type.upper())
    if building:
        sql += " AND building = %s"
        params.append(building)
    if available is not None:
        sql += " AND available = %s"
        params.append(available)
    rows = db.query(DB, sql + " ORDER BY id", params)
    return {"count": len(rows), "resources": rows}


@app.get("/api/resources/{resource_id}")
def get_resource(resource_id: str):
    row = db.query(DB, "SELECT * FROM resources WHERE id=%s", (resource_id,), one=True)
    if not row:
        raise HTTPException(404, "resource not found")
    return row


@app.post("/api/resources", status_code=201)
def create_resource(body: ResourceIn):
    row = _upsert(body)
    log.info("resource upserted", extra=log_fields(resourceId=body.id, type=body.type))
    return row


@app.patch("/api/resources/{resource_id}")
def update_resource(resource_id: str, body: ResourcePatch):
    fields = {k: v for k, v in body.model_dump().items() if v is not None}
    if "features" in fields:
        fields["features"] = json.dumps(fields["features"])
    if not fields:
        return get_resource(resource_id)
    sets = ", ".join(f"{k}=%s" for k in fields)
    row = db.query(DB, f"UPDATE resources SET {sets}, updated_at=now() WHERE id=%s RETURNING *",
                   (*fields.values(), resource_id), one=True)
    if not row:
        raise HTTPException(404, "resource not found")
    return row


@app.delete("/api/resources/{resource_id}", status_code=204)
def delete_resource(resource_id: str):
    db.query(DB, "DELETE FROM resources WHERE id=%s", (resource_id,))


@app.post("/api/resources/import")
def import_csv(file: UploadFile = File(...)):
    """CSV columns: id,name,type,building,capacity,features (features ';'-separated)."""
    raw = file.file.read()
    try:
        reader = csv.DictReader(io.StringIO(raw.decode("utf-8-sig")))
        rows = list(reader)
    except Exception:
        raise HTTPException(400, "file is not a valid UTF-8 CSV")
    required = {"id", "name", "type"}
    if not rows or not required.issubset(rows[0].keys()):
        raise HTTPException(400, f"CSV must have header columns {sorted(required)}")

    s3_key = None
    if config.UPLOADS_BUCKET:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        s3_key = f"imports/{stamp}-{(file.filename or 'resources.csv').replace('/', '_')}"
        if not s3_key.endswith(".csv"):
            s3_key += ".csv"
        aws.s3().put_object(Bucket=config.UPLOADS_BUCKET, Key=s3_key, Body=raw, ContentType="text/csv")

    imported, errors = 0, []
    for n, r in enumerate(rows, start=2):
        try:
            _upsert(ResourceIn(
                id=r["id"].strip(), name=r["name"].strip(), type=r["type"].strip().upper(),
                building=(r.get("building") or "").strip() or None,
                capacity=int(r.get("capacity") or 1),
                features=[f.strip() for f in (r.get("features") or "").split(";") if f.strip()],
            ))
            imported += 1
        except Exception as exc:
            errors.append({"line": n, "error": str(getattr(exc, "detail", exc))})
    log.info("csv import", extra=log_fields(imported=imported, errors=len(errors), s3Key=s3_key))
    return {"imported": imported, "errors": errors, "s3Key": s3_key,
            "s3Bucket": config.UPLOADS_BUCKET or None}
