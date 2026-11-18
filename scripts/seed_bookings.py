#!/usr/bin/env python3
"""Create a realistic set of bookings through the public API (standard library only).

    python3 scripts/seed_bookings.py --url http://<gateway> --count 300

Gives the consistent-hashing demo data to move, gives Flink events to window,
and gives the Lambda notifications to send. Conflicts (HTTP 409) are expected
and counted; they also feed the analytics conflict count.
"""
import argparse
import json
import random
import sys
import urllib.error
import urllib.request
from collections import Counter
from datetime import date, timedelta

ROOMS = [f"ROOM-{i:03d}" for i in range(1, 25)] + [f"LAB-{i:03d}" for i in range(1, 7)]
EQUIPMENT = [f"EQ-{i:03d}" for i in range(1, 11)]
USERS = [f"U{i:03d}" for i in range(1, 21)]


def post(url, body):
    req = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST",
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, {}
    except Exception as e:  # network
        return 0, {"error": str(e)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True, help="gateway base URL, e.g. http://abc.elb.amazonaws.com")
    ap.add_argument("--count", type=int, default=300)
    ap.add_argument("--days", type=int, default=5)
    args = ap.parse_args()
    base = args.url.rstrip("/")
    stats, nodes = Counter(), Counter()
    for n in range(args.count):
        equipment = random.random() < 0.25
        resource = random.choice(EQUIPMENT if equipment else ROOMS)
        day = date.today() + timedelta(days=random.randrange(args.days))
        start = random.randrange(8, 20)
        dur = random.choice([1, 1, 2])
        body = {"resourceId": resource, "userId": random.choice(USERS), "date": day.isoformat(),
                "startTime": f"{start:02d}:00", "endTime": f"{min(start + dur, 22):02d}:00",
                "purpose": "seed"}
        path = "/api/equipment/bookings" if equipment else "/api/rooms/bookings"
        code, resp = post(base + path, body)
        stats[code] += 1
        if code == 201:
            nodes[resp.get("node")] += 1
        if (n + 1) % 50 == 0:
            print(f"{n + 1}/{args.count} {dict(stats)}", flush=True)
    print("status codes:", dict(stats))
    print("bookings per partition node:", dict(nodes))
    return 0 if stats[201] else 1


if __name__ == "__main__":
    sys.exit(main())
