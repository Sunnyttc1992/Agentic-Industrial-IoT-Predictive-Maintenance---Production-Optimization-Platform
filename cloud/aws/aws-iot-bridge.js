/**
 * Securely forwards locally processed telemetry to AWS IoT Core.
 * AWS IoT Core's Topic Rule then writes it directly to Kinesis Data Streams.
 */
const fs = require('node:fs');
const mqtt = require('mqtt');

const required = ['AWS_IOT_ENDPOINT', 'AWS_IOT_CERT_PATH', 'AWS_IOT_KEY_PATH', 'AWS_IOT_CA_PATH'];
const missing = required.filter((name) => !process.env[name]);
if (missing.length) throw new Error(`Missing required environment variables: ${missing.join(', ')}`);

const localBroker = process.env.LOCAL_MQTT_BROKER_URL || 'mqtt://127.0.0.1:1883';
const localTopic = process.env.LOCAL_PROCESSED_TOPIC || 'industrial/processed/telemetry/#';
const awsTopicPrefix = (process.env.AWS_IOT_TOPIC_PREFIX || 'factory/processed/telemetry').replace(/\/$/, '');
const gatewayId = process.env.GATEWAY_ID || 'gateway-factory-01';

const awsClient = mqtt.connect(`mqtts://${process.env.AWS_IOT_ENDPOINT}:8883`, {
  clientId: `${gatewayId}-cloud-bridge`,
  cert: fs.readFileSync(process.env.AWS_IOT_CERT_PATH),
  key: fs.readFileSync(process.env.AWS_IOT_KEY_PATH),
  ca: fs.readFileSync(process.env.AWS_IOT_CA_PATH),
  protocolVersion: 4,
  clean: false,
  reconnectPeriod: 5000,
});
const localClient = mqtt.connect(localBroker);

awsClient.on('connect', () => console.log(`Connected securely to AWS IoT Core: ${process.env.AWS_IOT_ENDPOINT}`));
awsClient.on('error', (error) => console.error(`AWS IoT connection error: ${error.message}`));

localClient.on('connect', () => {
  localClient.subscribe(localTopic, { qos: 1 }, (error) => {
    if (error) return console.error(`Local subscription failed: ${error.message}`);
    console.log(`Cloud bridge listening on ${localTopic}`);
  });
});
localClient.on('message', (sourceTopic, payload) => {
  let telemetry;
  try { telemetry = JSON.parse(payload.toString()); } catch { return console.error(`Discarded invalid JSON from ${sourceTopic}`); }
  if (!telemetry.machine_id) return console.error(`Discarded message without machine_id from ${sourceTopic}`);
  const targetTopic = `${awsTopicPrefix}/${telemetry.machine_id}`;
  awsClient.publish(targetTopic, JSON.stringify(telemetry), { qos: 1 }, (error) => {
    if (error) console.error(`AWS publish failed for ${telemetry.machine_id}: ${error.message}`);
    else console.log(`Forwarded ${telemetry.machine_id} → AWS IoT topic ${targetTopic}`);
  });
});
localClient.on('error', (error) => console.error(`Local MQTT error: ${error.message}`));
process.on('SIGINT', () => { localClient.end(); awsClient.end(); });
process.on('SIGTERM', () => { localClient.end(); awsClient.end(); });
