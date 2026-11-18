"""User Service (owner: Animesh Bhargava).

Campus users and roles (student / faculty / staff / admin), stored in its own
PostgreSQL database `users` on RDS (database-per-service). Public REST API
under /api/users; the booking services call GET /api/users/{id} to validate.
"""
import re
import uuid
from contextlib import asynccontextmanager
from typing import Optional

import psycopg
from fastapi import HTTPException
from pydantic import BaseModel, Field

from common import db
from common.observability import create_app, log_fields, setup_logging

log = setup_logging()
DB = "users"
ROLES = ("student", "faculty", "staff", "admin")
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

SEED = [
    ("U001", "Animesh Bhargava", "animesh@pilani.bits-pilani.ac.in", "student", "CSIS"),
    ("U002", "Shreyansh Kedawat", "shreyansh@pilani.bits-pilani.ac.in", "student", "CSIS"),
    ("U003", "Prof. Hari Babu", "haribabu@pilani.bits-pilani.ac.in", "faculty", "CSIS"),
    ("U004", "Lab Admin", "labadmin@pilani.bits-pilani.ac.in", "admin", "Timetable Division"),
] + [
    (f"U{i:03d}", f"Student {i}", f"student{i}@pilani.bits-pilani.ac.in", "student",
     ["CSIS", "EEE", "MECH", "CHEM"][i % 4])
    for i in range(5, 21)
]


def init_schema():
    db.ensure_database(DB)
    db.query(DB, """
        CREATE TABLE IF NOT EXISTS users (
            id          TEXT PRIMARY KEY,
            name        TEXT NOT NULL,
            email       TEXT NOT NULL UNIQUE,
            role        TEXT NOT NULL CHECK (role IN ('student','faculty','staff','admin')),
            department  TEXT,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        )""")
    for row in SEED:
        db.query(DB, "INSERT INTO users (id, name, email, role, department) VALUES (%s,%s,%s,%s,%s) "
                     "ON CONFLICT (id) DO NOTHING", row)
    log.info("users schema ready", extra=log_fields(seeded=len(SEED)))


@asynccontextmanager
async def lifespan(_app):
    init_schema()
    yield


app = create_app("User Service", lifespan=lifespan)


class UserIn(BaseModel):
    name: str = Field(..., min_length=1)
    email: str
    role: str = "student"
    department: Optional[str] = None


class UserPatch(BaseModel):
    name: Optional[str] = None
    role: Optional[str] = None
    department: Optional[str] = None


def _check(role: Optional[str], email: Optional[str] = None):
    if role is not None and role not in ROLES:
        raise HTTPException(400, f"role must be one of {ROLES}")
    if email is not None and not EMAIL_RE.match(email):
        raise HTTPException(400, "invalid email")


@app.get("/api/users")
def list_users(role: Optional[str] = None, limit: int = 100, offset: int = 0):
    _check(role)
    if role:
        rows = db.query(DB, "SELECT * FROM users WHERE role=%s ORDER BY id LIMIT %s OFFSET %s",
                        (role, limit, offset))
    else:
        rows = db.query(DB, "SELECT * FROM users ORDER BY id LIMIT %s OFFSET %s", (limit, offset))
    return {"count": len(rows), "users": rows}


@app.get("/api/users/{user_id}")
def get_user(user_id: str):
    row = db.query(DB, "SELECT * FROM users WHERE id=%s", (user_id,), one=True)
    if not row:
        raise HTTPException(404, "user not found")
    return row


@app.post("/api/users", status_code=201)
def create_user(body: UserIn):
    _check(body.role, body.email)
    user_id = "U" + uuid.uuid4().hex[:8].upper()
    try:
        row = db.query(DB, "INSERT INTO users (id, name, email, role, department) VALUES (%s,%s,%s,%s,%s) "
                           "RETURNING *", (user_id, body.name, body.email, body.role, body.department), one=True)
    except psycopg.errors.UniqueViolation:
        raise HTTPException(409, "email already registered")
    log.info("user created", extra=log_fields(userId=user_id, role=body.role))
    return row


@app.patch("/api/users/{user_id}")
def update_user(user_id: str, body: UserPatch):
    _check(body.role)
    fields = {k: v for k, v in body.model_dump().items() if v is not None}
    if not fields:
        return get_user(user_id)
    sets = ", ".join(f"{k}=%s" for k in fields)
    row = db.query(DB, f"UPDATE users SET {sets} WHERE id=%s RETURNING *", (*fields.values(), user_id), one=True)
    if not row:
        raise HTTPException(404, "user not found")
    return row


@app.delete("/api/users/{user_id}", status_code=204)
def delete_user(user_id: str):
    db.query(DB, "DELETE FROM users WHERE id=%s", (user_id,))
