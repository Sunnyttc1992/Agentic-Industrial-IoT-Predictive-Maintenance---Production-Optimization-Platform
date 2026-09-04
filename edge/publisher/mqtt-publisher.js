/** Polls the simulator API and publishes each new reading to MQTT. */
const mqtt = require('mqtt');

const brokerUrl = process.env.MQTT_BROKER_URL || 'mqtt://127.0.0.1:1883';
const apiUrl = process.env.SIMULATOR_API_URL || 'http://127.0.0.1:8000/api/v1/telemetry';
const topicPrefix = (process.env.MQTT_TOPIC_PREFIX || 'industrial/raw').replace(/\/$/, '');
const intervalMs = Number(process.env.PUBLISH_INTERVAL_MS || 5000);

const client = mqtt.connect(brokerUrl, {
  clientId: `iot-simulator-publisher-${Math.random().toString(16).slice(2, 10)}`,
  will: { topic: `${topicPrefix}/simulator/status`, payload: 'offline', retain: true },
});

async function publishTelemetry() {
  try {
    const response = await fetch(apiUrl);
    if (!response.ok) throw new Error(`Simulator returned HTTP ${response.status}`);
    const readings = await response.json();
    for (const telemetry of readings) {
      const topic = `${topicPrefix}/telemetry/${telemetry.machine_id}`;
      client.publish(topic, JSON.stringify(telemetry), { qos: 1 }, (error) => {
        if (error) console.error(`Publish failed for ${telemetry.machine_id}:`, error.message);
      });
      console.log(`Published ${topic} | ${telemetry.status} | ${telemetry.temperature_c}°C`);
    }
  } catch (error) {
    console.error(`Telemetry polling failed: ${error.message}`);
  }
}

client.on('connect', async () => {
  console.log(`Connected to MQTT broker: ${brokerUrl}`);
  client.publish(`${topicPrefix}/simulator/status`, 'online', { qos: 1, retain: true });
  await publishTelemetry();
  setInterval(publishTelemetry, intervalMs);
});
client.on('error', (error) => console.error(`MQTT error: ${error.message}`));

function close() {
  client.publish(`${topicPrefix}/simulator/status`, 'offline', { qos: 1, retain: true }, () => client.end());
}
process.on('SIGINT', close);
process.on('SIGTERM', close);
