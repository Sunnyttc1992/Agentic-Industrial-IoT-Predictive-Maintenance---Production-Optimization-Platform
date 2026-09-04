# Target architecture

```text
Sensors → MQTT → Edge gateway → AWS IoT Core → Kinesis
                                           ├→ Digital twin (Eclipse Ditto)
                                           └→ Stream processing → S3 lakehouse → ML / agents → Dashboard and alerts
```

| Repository area | Architecture responsibility | Current state |
| --- | --- | --- |
| `edge/` | Sensor simulation, MQTT publisher, IoT gateway | Implemented locally |
| `cloud/aws/` | Mutual-TLS AWS IoT bridge and Kinesis rule | Implemented; requires AWS account setup |
| `stream-processing/` | Flink / Spark streaming | Planned scaffold |
| `data-platform/` | S3 lake, Airflow, dbt, quality | Planned scaffold |
| `analytics-ml/` | Time series and predictive maintenance | Planned scaffold |
| `knowledge/` | Manuals and vector retrieval | Planned scaffold |
| `agents/` | Supervisor and specialist agents | Planned scaffold |
| `apps/` | Dashboard, API, alerts, approvals | Planned scaffold |

The event contract in `contracts/telemetry.schema.json` is the interface between layers. Preserve backwards compatibility when adding telemetry fields.
