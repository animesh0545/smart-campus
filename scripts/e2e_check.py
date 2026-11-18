#!/usr/bin/env python3
"""End-to-end functional check of the whole system through the public gateway.
Standard library only, so it runs the same on macOS, Linux, WSL and Windows.

    python3 scripts/e2e_check.py --url http://localhost:8080              # docker compose
    python3 scripts/e2e_check.py --url http://<elb-hostname>              # AWS
    python3 scripts/e2e_check.py --url ... --rebalance                    # also join + leave node-3

Each check prints PASS/FAIL; the exit code is the number of failures (0 = all good).
Bookings use a random date 60-300 days ahead, so repeated runs don't collide.
"""
import argparse
import json
import random
import sys
import time
import urllib.error
import urllib.request
import uuid
from datetime import date, timedelta

RESULTS = []


def call(method, url, body=None, headers=None, raw=None, timeout=30):
    data = raw if raw is not None else (json.dumps(body).encode() if body is not None else None)
    hdrs = {"Content-Type": "application/json"} if body is not None else {}
    hdrs.update(headers or {})
    req = urllib.request.Request(url, data=data, method=method, headers=hdrs)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            text = r.read().decode() or "{}"
            return r.status, (json.loads(text) if text.strip().startswith(("{", "[")) else text)
    except urllib.error.HTTPError as e:
        text = e.read().decode() or "{}"
        try:
            return e.code, json.loads(text)
        except ValueError:
            return e.code, text
    except Exception as e:
        return 0, {"error": str(e)}


def check(name, ok, detail=""):
    RESULTS.append(bool(ok))
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + (f"   ({detail})" if detail and not ok else ""))
    return ok


def section(title):
    print(f"\n== {title}")


def multipart(field, filename, content: bytes):
    boundary = uuid.uuid4().hex
    body = (f"--{boundary}\r\nContent-Disposition: form-data; name=\"{field}\"; filename=\"{filename}\"\r\n"
            f"Content-Type: text/csv\r\n\r\n").encode() + content + f"\r\n--{boundary}--\r\n".encode()
    return body, {"Content-Type": f"multipart/form-data; boundary={boundary}"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--rebalance", action="store_true", help="join node-3 and remove it again")
    args = ap.parse_args()
    base = args.url.rstrip("/")
    day = (date.today() + timedelta(days=random.randint(60, 300))).isoformat()
    room = f"ROOM-{random.randint(1, 24):03d}"
    print(f"target {base}   test date {day}   room {room}")

    section("gateway")
    s, b = call("GET", f"{base}/")
    check("GET / lists routes", s == 200 and "routes" in str(b), f"{s} {b}")

    section("User Service (RDS)")
    s, b = call("GET", f"{base}/api/users/U001")
    check("GET seeded user U001", s == 200 and b.get("id") == "U001", f"{s} {b}")
    email = f"e2e-{uuid.uuid4().hex[:8]}@pilani.bits-pilani.ac.in"
    s, b = call("POST", f"{base}/api/users", {"name": "E2E Tester", "email": email, "role": "staff"})
    check("POST new user", s == 201 and b.get("id"), f"{s} {b}")
    s, _ = call("POST", f"{base}/api/users", {"name": "dup", "email": email})
    check("duplicate email rejected (409)", s == 409, s)
    s, _ = call("GET", f"{base}/api/users/U-DOES-NOT-EXIST")
    check("unknown user -> 404", s == 404, s)

    section("Resource Registry (RDS + S3)")
    s, b = call("GET", f"{base}/api/resources/{room}")
    check(f"GET {room}", s == 200 and b.get("type") == "ROOM", f"{s} {b}")
    s, b = call("GET", f"{base}/api/resources?type=EQUIPMENT")
    check("list equipment", s == 200 and b.get("count", 0) >= 10, f"{s} {b}")
    csv = b"id,name,type,building,capacity,features\nROOM-950,E2E Hall,ROOM,LTC,90,projector\n"
    raw, hdr = multipart("file", "e2e-import.csv", csv)
    s, b = call("POST", f"{base}/api/resources/import", raw=raw, headers=hdr)
    check("CSV import (S3 upload on AWS triggers Lambda)", s == 200 and b.get("imported") == 1, f"{s} {b}")

    section("Availability (REST + gRPC -> Partition Manager -> DynamoDB)")
    q = f"resourceId={room}&date={day}&startTime=10:00&endTime=11:00"
    s, b = call("GET", f"{base}/api/availability?{q}")
    check("slot free before booking", s == 200 and b.get("available") is True, f"{s} {b}")

    section("Room Booking (REST -> gRPC -> DynamoDB -> Kafka/SQS)")
    body = {"resourceId": room, "userId": "U001", "date": day, "startTime": "10:00", "endTime": "11:00",
            "purpose": "e2e"}
    s, bk = call("POST", f"{base}/api/rooms/bookings", body, headers={"x-correlation-id": "e2e-check"})
    ok = check("create booking -> 201", s == 201 and bk.get("bookingId"), f"{s} {bk}")
    if ok:
        bid, node = bk["bookingId"], bk.get("node")
        check("booking stored on a ring node", str(node).startswith("node-"), node)
        s, b = call("POST", f"{base}/api/rooms/bookings", {**body, "startTime": "10:30", "endTime": "11:30"})
        check("overlapping slot -> 409 conflict", s == 409 and b.get("conflicts"), f"{s} {b}")
        s, b = call("POST", f"{base}/api/rooms/bookings", {**body, "startTime": "11:00", "endTime": "12:00"})
        check("adjacent slot allowed -> 201", s == 201, f"{s} {b}")
        s, b = call("GET", f"{base}/api/availability?{q}")
        check("slot now unavailable", s == 200 and b.get("available") is False, f"{s} {b}")
        s, b = call("GET", f"{base}/api/availability/{room}/{day}/schedule")
        check("day schedule shows booked + free", s == 200 and b.get("booked") and b.get("free"), f"{s} {b}")
        url = f"{base}/api/rooms/bookings/{room}/{day}/{bid}"
        s, b = call("PUT", url, {"startTime": "14:00", "endTime": "15:00"})
        check("modify booking time -> 200", s == 200 and b.get("startTime") == "14:00", f"{s} {b}")
        s, b = call("GET", url)
        check("GET booking reflects change", s == 200 and b.get("startTime") == "14:00", f"{s} {b}")
        s, b = call("DELETE", url)
        check("cancel booking", s == 200 and b.get("status") == "CANCELLED", f"{s} {b}")
        s, b = call("GET", f"{base}/api/rooms/users/U001/bookings")
        check("user bookings (scatter/gather over all nodes)", s == 200 and b.get("nodesQueried"), f"{s} {b}")
    s, _ = call("POST", f"{base}/api/rooms/bookings", {**body, "date": "18-11-2026"})
    check("bad date -> 400", s == 400, s)
    s, _ = call("POST", f"{base}/api/rooms/bookings", {**body, "resourceId": "EQ-001"})
    check("equipment id on room service -> 400", s == 400, s)

    section("Equipment Booking")
    eq = f"EQ-{random.randint(1, 10):03d}"
    s, b = call("POST", f"{base}/api/equipment/bookings",
                {"resourceId": eq, "userId": "U002", "date": day, "startTime": "09:00", "endTime": "10:30"})
    check(f"book {eq} -> 201", s == 201 and b.get("resourceType") == "EQUIPMENT", f"{s} {b}")
    s, b = call("GET", f"{base}/api/equipment/bookings/{eq}/{day}")
    check("list equipment bookings for the day", s == 200 and len(b.get("bookings", [])) >= 1, f"{s} {b}")

    section("Partition Manager (consistent hashing)")
    s, ring = call("GET", f"{base}/api/partitions/ring")
    check("ring has members", s == 200 and len(ring.get("members", [])) >= 2, f"{s} {ring}")
    s, b = call("GET", f"{base}/api/partitions/lookup?key={room}%23{day}")
    check("lookup returns owner + mod-N owner", s == 200 and b.get("nodeId") and b.get("modNOwner"), f"{s} {b}")
    s, b = call("GET", f"{base}/api/partitions/simulate?keys=10000&nodes=3")
    if check("simulate", s == 200, f"{s} {b}"):
        j = b["join_one_node"]
        check(f"consistent hashing moves fewer keys than mod-N "
              f"({j['consistent_hashing_moved_pct']}% vs {j['mod_n_moved_pct']}%)",
              j["consistent_hashing_moved_pct"] < j["mod_n_moved_pct"])
    if args.rebalance:
        spares = ring.get("spareNodes", [])
        if not spares:
            check("a spare node exists for the join test", False, "no spare (node-3 already in ring?)")
        else:
            node = spares[0]
            s, b = call("POST", f"{base}/api/partitions/nodes", {"nodeId": node}, timeout=300)
            check(f"{node} joins; keys moved only into it",
                  s == 200 and all(f.endswith(f"->{node}") for f in b.get("flows", {})), f"{s} {b}")
            if s == 200:
                print(f"        moved {b['keys_moved_pct']}% of keys (mod-N would move {b['mod_n_would_move_pct']}%)")
            s, b = call("DELETE", f"{base}/api/partitions/nodes/{node}", timeout=300)
            check(f"{node} leaves; keys moved only out of it",
                  s == 200 and b.get("moved_only_from_leaving_node") is True, f"{s} {b}")

    section("Usage Analytics")
    s, b = call("GET", f"{base}/api/analytics/live")
    check("analytics API up", s == 200, f"{s} {b}")
    s, b = call("GET", f"{base}/api/analytics/summary?minutes=60")
    check("summary endpoint", s == 200 and "totals" in b, f"{s} {b}")
    print("        (window results appear only while the Flink job runs: EMR, or ./scripts/local.sh flink)")

    fails = RESULTS.count(False)
    print(f"\n{len(RESULTS) - fails}/{len(RESULTS)} checks passed")
    return fails


if __name__ == "__main__":
    t = time.time()
    rc = main()
    print(f"done in {time.time() - t:.1f}s")
    sys.exit(min(rc, 100))
