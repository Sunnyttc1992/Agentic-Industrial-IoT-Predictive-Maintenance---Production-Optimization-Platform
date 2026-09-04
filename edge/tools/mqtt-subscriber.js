/** Simple inspection client for all telemetry emitted by mqtt-publisher.js. */
const mqtt = require('mqtt');

const brokerUrl = process.env.MQTT_BROKER_URL || 'mqtt://127.0.0.1:1883';
const topicPrefix = (process.env.MQTT_TOPIC_PREFIX || 'industrial').replace(/\/$/, '');
const client = mqtt.connect(brokerUrl);

client.on('connect', () => {
  const topic = `${topicPrefix}/#`;
  client.subscribe(topic, { qos: 1 }, (error) => {
    if (error) return console.error(`Subscription failed: ${error.message}`);
    console.log(`Listening on ${topic}`);
  });
});
client.on('message', (topic, payload) => console.log(`${topic}\n${payload.toString()}\n`));
client.on('error', (error) => console.error(`MQTT error: ${error.message}`));
