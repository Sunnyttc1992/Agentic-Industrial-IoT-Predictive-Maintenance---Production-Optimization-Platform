# API documentation

Run the local service with:

```powershell
python -m uvicorn src.api.main:app --reload
```

Interactive OpenAPI documentation is available at `/docs`.

| Endpoint | Purpose |
| --- | --- |
| `GET /health` | Service, local-artifact, and real-time MQTT connection health |
| `GET /api/v1/machines` | Latest validated live telemetry, falling back to Silver by machine |
| `GET /api/v1/predictions` | Latest live supervised fault prediction, falling back to model output |
| `GET /api/v1/alerts` | Current sensor-rule alerts and acknowledgement state |
| `POST /api/v1/agents/analyze` | Run the LangGraph supervisor on a telemetry event |
| `GET /api/v1/approvals` | List local recommendation review records |
| `POST /api/v1/approvals/{approval_id}` | Record an approval or rejection |
| `WS /ws/operations` | Initial snapshot plus pushed MQTT telemetry, model, alert, and agent-review updates |

Alerts can be acknowledged through `POST /api/v1/alerts/{alert_id}/acknowledge`.
Acknowledgements and approvals are local in-memory records, not machine-control
commands.

Set `REALTIME_MQTT_ENABLED=true` to start the AWS IoT Core subscriber when the
API starts. The `realtime` section of `/health` reports its connection state,
message counters, and the last safe-to-display error.
