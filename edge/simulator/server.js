/** Dependency-free dynamic Industrial IoT telemetry simulator. */
const http = require('node:http');
const { URL } = require('node:url');

const machines = new Map([
  ['CNC-01', { machine_name: 'CNC Milling Machine', baseTemp: 63, basePower: 18, tick: 0, wear: Math.random() * 2.5 }],
  ['PUMP-01', { machine_name: 'Hydraulic Pump', baseTemp: 58, basePower: 12, tick: 0, wear: Math.random() * 2.5 }],
  ['CONV-01', { machine_name: 'Conveyor Drive', baseTemp: 52, basePower: 9, tick: 0, wear: Math.random() * 2.5 }],
]);

function clamp(value, min, max) { return Math.max(min, Math.min(max, value)); }
function round(value, digits = 2) { return Number(value.toFixed(digits)); }

function reading(machineId, machine) {
  machine.tick += 1;
  const cycle = Math.sin(machine.tick / 7);
  const load = Math.sin(machine.tick / 11 + 0.8);
  machine.wear = clamp(machine.wear + (Math.random() * 0.53 - 0.18), 0, 22);
  const noise = (amount) => (Math.random() - 0.5) * amount;
  const temperature = machine.baseTemp + 3.8 * cycle + machine.wear * 0.55 + noise(0.9);
  const vibration = 1.25 + 0.28 * load + machine.wear * 0.13 + noise(0.12);
  const pressure = 6.5 + 0.35 * cycle - machine.wear * 0.025 + noise(0.08);
  const severity = Math.max((temperature - 78) / 10, (vibration - 3.4) / 0.8, (machine.wear - 13) / 8);
  const status = severity >= 0.7 ? 'critical' : severity >= 0.15 ? 'warning' : 'healthy';
  const health = clamp(100 - machine.wear * 3.3 - Math.max(0, temperature - 70) * 1.4 - Math.max(0, vibration - 2.5) * 7, 0, 100);
  return {
    timestamp: new Date().toISOString(), machine_id: machineId, machine_name: machine.machine_name, status,
    health_score: round(health, 1), temperature_c: round(temperature), vibration_mm_s: round(vibration, 3),
    pressure_bar: round(pressure), power_kw: round(machine.basePower + 4 * load + machine.wear * 0.2 + noise(0.7)),
    rpm: Math.round(1450 + 45 * cycle + noise(10)),
    production_rate_units_hr: Math.max(0, Math.round(500 - machine.wear * 5 - Math.abs(cycle) * 15 + noise(8))),
  };
}

function send(response, status, data) {
  response.writeHead(status, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
  response.end(JSON.stringify(data, null, 2));
}

const server = http.createServer((request, response) => {
  if (request.method === 'OPTIONS') return send(response, 204, {});
  const { pathname } = new URL(request.url, `http://${request.headers.host}`);
  if (request.method === 'GET' && pathname === '/health') return send(response, 200, { status: 'ok' });
  if (request.method === 'GET' && pathname === '/api/v1/machines') {
    return send(response, 200, [...machines].map(([machine_id, m]) => ({ machine_id, machine_name: m.machine_name })));
  }
  if (request.method === 'GET' && pathname === '/api/v1/telemetry') {
    return send(response, 200, [...machines].map(([id, machine]) => reading(id, machine)));
  }
  const match = pathname.match(/^\/api\/v1\/telemetry\/([^/]+)$/);
  if (request.method === 'GET' && match) {
    const id = decodeURIComponent(match[1]).toUpperCase();
    const machine = machines.get(id);
    return machine ? send(response, 200, reading(id, machine)) : send(response, 404, { detail: `Unknown machine: ${id}` });
  }
  return send(response, 404, { detail: 'Not found' });
});

const port = Number(process.env.PORT || 8000);
server.listen(port, () => console.log(`IoT simulator is running at http://127.0.0.1:${port}`));
