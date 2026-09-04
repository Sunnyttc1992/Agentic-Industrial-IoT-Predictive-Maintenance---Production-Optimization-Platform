/**
 * IoT gateway: converts raw device telemetry into validated, enriched events.
 * Input:  industrial/raw/telemetry/{machineId}
 * Output: industrial/processed/telemetry/{machineId}
 * Alerts: industrial/alerts/{machineId}
 */
const mqtt = require('mqtt');

const brokerUrl = process.env.MQTT_BROKER_URL || 'mqtt://127.0.0.1:1883';
const rawPrefix = (process.env.RAW_TOPIC_PREFIX || 'industrial/raw').replace(/\/$/, '');
const processedPrefix = (process.env.PROCESSED_TOPIC_PREFIX || 'industrial/processed').replace(/\/$/, '');
const alertPrefix = (process.env.ALERT_TOPIC_PREFIX || 'industrial/alerts').replace(/\/$/, '');
const gatewayId = process.env.GATEWAY_ID || 'gateway-factory-01';
const client = mqtt.connect(brokerUrl, {
  clientId: `${gatewayId}-${Math.random().toString(16).slice(2, 10)}`,
  will: { topic: `${processedPrefix}/gateway/${gatewayId}/status`, payload: 'offline', retain: true },
});

function validate(reading) {
  const required = ['timestamp', 'machine_id', 'temperature_c', 'vibration_mm_s', 'pressure_bar', 'power_kw', 'rpm'];
  const missing = required.filter((field) => reading[field] === undefined || reading[field] === null);
  if (missing.length) return `Missing fields: ${missing.join(', ')}`;
  if (!Number.isFinite(reading.temperature_c) || !Number.isFinite(reading.vibration_mm_s)) return 'Sensor values must be numeric';
  return null;
}

function alertsFor(reading) {
  const alerts = [];
  if (reading.temperature_c >= 78) alerts.push({ type: 'high_temperature', severity: 'critical', value: reading.temperature_c, threshold: 78 });
  if (reading.vibration_mm_s >= 3.4) alerts.push({ type: 'high_vibration', severity: 'critical', value: reading.vibration_mm_s, threshold: 3.4 });
  if (reading.status === 'warning' && !alerts.length) alerts.push({ type: 'degraded_health', severity: 'warning', health_score: reading.health_score });
  return alerts;
}

client.on('connect', () => {
  const topic = `${rawPrefix}/telemetry/#`;
  client.subscribe(topic, { qos: 1 }, (error) => {
    if (error) return console.error(`Gateway subscription failed: ${error.message}`);
    client.publish(`${processedPrefix}/gateway/${gatewayId}/status`, 'online', { qos: 1, retain: true });
    console.log(`Gateway ${gatewayId} listening on ${topic}`);
  });
});

client.on('message', (topic, payload) => {
  let reading;
  try {
    reading = JSON.parse(payload.toString());
  } catch {
    return console.error(`Discarded invalid JSON from ${topic}`);
  }
  const invalidReason = validate(reading);
  if (invalidReason) {
    console.error(`Discarded ${reading.machine_id || 'unknown'}: ${invalidReason}`);
    return client.publish(`${processedPrefix}/invalid`, JSON.stringify({ gateway_id: gatewayId, source_topic: topic, reason: invalidReason, received_at: new Date().toISOString() }), { qos: 1 });
  }
  const enriched = {
    ...reading,
    gateway_id: gatewayId,
    schema_version: '1.0',
    received_at: new Date().toISOString(),
    source_topic: topic,
  };
  const outputTopic = `${processedPrefix}/telemetry/${reading.machine_id}`;
  client.publish(outputTopic, JSON.stringify(enriched), { qos: 1 });
  for (const alert of alertsFor(reading)) {
    client.publish(`${alertPrefix}/${reading.machine_id}`, JSON.stringify({ ...alert, machine_id: reading.machine_id, timestamp: reading.timestamp, gateway_id: gatewayId }), { qos: 1 });
  }
  console.log(`Forwarded ${reading.machine_id} → ${outputTopic}`);
});

client.on('error', (error) => console.error(`MQTT error: ${error.message}`));
process.on('SIGINT', () => client.end());
process.on('SIGTERM', () => client.end());
