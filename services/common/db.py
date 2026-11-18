"""PostgreSQL (RDS) access with the database-per-service pattern: one RDS
instance, but User Service and Resource Registry each own a separate database
created on first start."""
import logging
import threading
import time

import psycopg
from psycopg.rows import dict_row

from . import config

log = logging.getLogger("db")
_local = threading.local()


def _conninfo(dbname: str) -> str:
    return (f"host={config.DB_HOST} port={config.DB_PORT} user={config.DB_USER} "
            f"password={config.DB_PASSWORD} dbname={dbname} connect_timeout=5")


def ensure_database(dbname: str, retries: int = 30) -> None:
    """CREATE DATABASE if missing (waits for RDS/Postgres to accept connections)."""
    for attempt in range(1, retries + 1):
        try:
            with psycopg.connect(_conninfo("postgres"), autocommit=True) as conn:
                exists = conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,)).fetchone()
                if not exists:
                    conn.execute(f'CREATE DATABASE "{dbname}"')
                    log.info(f"created database {dbname}")
                return
        except psycopg.errors.DuplicateDatabase:
            return
        except Exception as exc:
            log.warning(f"postgres not ready ({attempt}/{retries}): {exc}")
            time.sleep(3)
    raise RuntimeError("could not connect to PostgreSQL")


def connection(dbname: str) -> psycopg.Connection:
    """One autocommit connection per worker thread, reopened if it broke."""
    conn = getattr(_local, dbname, None)
    if conn is None or conn.closed or conn.broken:
        conn = psycopg.connect(_conninfo(dbname), autocommit=True, row_factory=dict_row)
        setattr(_local, dbname, conn)
    return conn


def query(dbname: str, sql: str, params=None, one: bool = False):
    try:
        cur = connection(dbname).execute(sql, params or ())
    except psycopg.OperationalError:
        setattr(_local, dbname, None)  # retry once on a fresh connection
        cur = connection(dbname).execute(sql, params or ())
    if cur.description is None:
        return None
    return cur.fetchone() if one else cur.fetchall()
