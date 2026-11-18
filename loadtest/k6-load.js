// Requirement c: sustained load that makes the HPAs scale out.
//   BASE_URL=http://<gateway-lb> k6 run loadtest/k6-load.js
// Two scenarios hit the two autoscaled services through the public gateway:
//   * availability : GET /api/availability   (read path, Availability Service)
//   * bookings     : POST /api/rooms/bookings (write path, Room Booking -> gRPC -> DynamoDB -> Kafka/SQS)
// 409 (slot already taken) is an expected business outcome, not an error.
// Watch it scale:  kubectl -n smart-campus get hpa -w
import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const DURATION = __ENV.DURATION || '5m';
const AVAIL_VUS = parseInt(__ENV.AVAIL_VUS || '40');
const BOOK_VUS = parseInt(__ENV.BOOK_VUS || '15');

export const options = {
  scenarios: {
    availability: {
      executor: 'ramping-vus', exec: 'availability', startVUs: 0,
      stages: [
        { duration: '1m', target: AVAIL_VUS },
        { duration: DURATION, target: AVAIL_VUS },
        { duration: '30s', target: 0 },
      ],
    },
    bookings: {
      executor: 'ramping-vus', exec: 'booking', startVUs: 0,
      stages: [
        { duration: '1m', target: BOOK_VUS },
        { duration: DURATION, target: BOOK_VUS },
        { duration: '30s', target: 0 },
      ],
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.05'],
    'http_req_duration{scenario:availability}': ['p(95)<1500'],
  },
};

const ROOMS = [];
for (let i = 1; i <= 24; i++) ROOMS.push(`ROOM-${String(i).padStart(3, '0')}`);
for (let i = 1; i <= 6; i++) ROOMS.push(`LAB-${String(i).padStart(3, '0')}`);
const USERS = [];
for (let i = 1; i <= 20; i++) USERS.push(`U${String(i).padStart(3, '0')}`);

const pick = (arr) => arr[Math.floor(Math.random() * arr.length)];
const pad = (n) => String(n).padStart(2, '0');

function randomSlot() {
  const d = new Date(Date.now() + Math.floor(Math.random() * 7) * 86400000);
  const date = `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}`;
  const startH = 8 + Math.floor(Math.random() * 12);           // 08..19
  const startM = Math.random() < 0.5 ? 0 : 30;
  const durMin = [30, 60, 90, 120][Math.floor(Math.random() * 4)];
  const endTotal = Math.min(startH * 60 + startM + durMin, 22 * 60);
  return {
    date,
    startTime: `${pad(startH)}:${pad(startM)}`,
    endTime: `${pad(Math.floor(endTotal / 60))}:${pad(endTotal % 60)}`,
  };
}

const ok409 = http.expectedStatuses({ min: 200, max: 299 }, 409);

export function availability() {
  const s = randomSlot();
  const url = `${BASE}/api/availability?resourceId=${pick(ROOMS)}&date=${s.date}&startTime=${s.startTime}&endTime=${s.endTime}`;
  const res = http.get(url, { tags: { name: 'GET /api/availability' } });
  check(res, { 'availability 200': (r) => r.status === 200 });
  sleep(0.1);
}

export function booking() {
  const s = randomSlot();
  const body = JSON.stringify({ resourceId: pick(ROOMS), userId: pick(USERS), purpose: 'k6 load test', ...s });
  const res = http.post(`${BASE}/api/rooms/bookings`, body, {
    headers: { 'Content-Type': 'application/json' },
    responseCallback: ok409,
    tags: { name: 'POST /api/rooms/bookings' },
  });
  check(res, { 'booking 201 or 409': (r) => r.status === 201 || r.status === 409 });
  sleep(0.2);
}
