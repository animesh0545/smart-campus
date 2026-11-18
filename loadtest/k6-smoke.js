// Quick end-to-end smoke test (30 s, 2 VUs): every public route answers.
//   BASE_URL=http://<gateway-lb> k6 run loadtest/k6-smoke.js
import http from 'k6/http';
import { check } from 'k6';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
export const options = { vus: 2, duration: '30s', thresholds: { checks: ['rate>0.95'] } };

export default function () {
  const today = new Date().toISOString().slice(0, 10);
  check(http.get(`${BASE}/api/users/U001`), { 'user 200': (r) => r.status === 200 });
  check(http.get(`${BASE}/api/resources?type=ROOM`), { 'resources 200': (r) => r.status === 200 });
  check(http.get(`${BASE}/api/availability?resourceId=ROOM-001&date=${today}&startTime=09:00&endTime=10:00`),
        { 'availability 200': (r) => r.status === 200 });
  check(http.get(`${BASE}/api/partitions/ring?counts=false`), { 'ring 200': (r) => r.status === 200 });
  check(http.get(`${BASE}/api/equipment/bookings/EQ-001/${today}`), { 'equipment 200': (r) => r.status === 200 });
  check(http.get(`${BASE}/api/analytics/live`), { 'analytics 200': (r) => r.status === 200 });
}
