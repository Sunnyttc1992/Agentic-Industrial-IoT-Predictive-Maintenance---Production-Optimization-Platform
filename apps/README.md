# Application layer

Dashboard, API, and alerting services expose telemetry, predictions, recommendations, and approval workflows to operators.

## Local operations console

Run the Phase 6 service from the repository root:

```powershell
python -m uvicorn src.api.main:app --reload
```

Open `http://127.0.0.1:8000` for the dashboard or `http://127.0.0.1:8000/docs`
for the API. The application reads local Silver telemetry and model-output
Parquet files. Alert acknowledgements and agent approvals are intentionally
in-memory for local development; they do not actuate equipment.

## Live MQTT analytics

The console can also subscribe directly to the simulator's AWS IoT Core topic.
Copy the real-time settings from `.env.example` into `.env` (the repository's
local configuration is already enabled) and run the simulator and API in
separate terminals:

```powershell
# Terminal 1: publishes ten simulated machine readings every five seconds
python -m src.main

# Terminal 2: subscribes, scores, and pushes updates to the browser
python -m uvicorn src.api.main:app --reload
```

The API uses a separate `AWS_IOT_DASHBOARD_CLIENT_ID`, so it does not disconnect
the simulator. The dashboard opens `/ws/operations`; each valid telemetry event
updates the latest reading, five-observation rolling features, fault prediction,
rule-based alert, and (when review is required) one human approval request.

The device certificate's AWS IoT policy must allow `iot:Connect` for the
dashboard client ID plus `iot:Subscribe` and `iot:Receive` for
`sunny/factory1/machines/+/telemetry`. If the feed cannot connect, inspect the
`realtime.connection_status` and `realtime.last_error` fields at `/health`.

Use the following policy statements as a least-privilege template, replacing
`<region>` and `<account-id>`, then attach the policy to the certificate used
by the dashboard. In AWS IoT policy ARNs, use `*` rather than the MQTT `+`
wildcard:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "iot:Connect",
      "Resource": "arn:aws:iot:<region>:<account-id>:client/factory-operations-console"
    },
    {
      "Effect": "Allow",
      "Action": "iot:Subscribe",
      "Resource": "arn:aws:iot:<region>:<account-id>:topicfilter/sunny/factory1/machines/*/telemetry"
    },
    {
      "Effect": "Allow",
      "Action": "iot:Receive",
      "Resource": "arn:aws:iot:<region>:<account-id>:topic/sunny/factory1/machines/*/telemetry"
    }
  ]
}
```
