# Industrial IoT Multi-Agent Platform

Python-first reference implementation for industrial telemetry ingestion, AWS streaming, data pipelines, predictive maintenance, digital twins, RAG, and human-approved operational agents.

## Repository layout

```text
src/             Application packages: simulator, ingestion, streaming, pipelines, ML, twins, RAG, agents, API
infrastructure/  Terraform modules and CloudFormation templates
data/            Raw, sample, manual, and maintenance-log inputs
workflows/       Glue, Airflow, and Step Functions definitions
tests/           Unit, integration, and end-to-end tests
monitoring/      CloudWatch, dashboards, and alerts
docs/            Architecture and product documentation
```

Start with `copy .env.example .env`, then install with `make install` and run the basic simulator using `make run-simulator`. The local Node MQTT proof of concept remains temporarily below during migration to the Python modules in `src/`.

This repository follows an edge-to-cloud industrial architecture. The runnable foundation is the edge telemetry simulator, local MQTT gateway, and AWS IoT Core to Kinesis ingestion path. See [the architecture map](docs/architecture.md) for component ownership and maturity.

```text
edge/              Sensors, MQTT publisher, and gateway
cloud/             AWS IoT Core and Kinesis integration
stream-processing/ Flink or Spark streaming jobs
data-platform/     S3 lakehouse, Airflow, dbt, and data quality
analytics-ml/      Time-series and predictive-maintenance models
knowledge/         Manuals and vector retrieval
agents/            Supervisor and operational agents
apps/              Dashboard, API, and alerts
contracts/         Shared data contracts
```

This starter service exposes dynamic telemetry for three industrial machines. Every call produces a new reading with realistic operating cycles, noise, and gradually increasing wear. It uses only Node.js—no package installation is required.

## Run

```powershell
npm start
```

## Call the API

```powershell
# All machines (call repeatedly to observe changing values)
Invoke-RestMethod http://127.0.0.1:8000/api/v1/telemetry

# One machine
Invoke-RestMethod http://127.0.0.1:8000/api/v1/telemetry/CNC-01

# Available machines
Invoke-RestMethod http://127.0.0.1:8000/api/v1/machines
```

The API runs at `http://127.0.0.1:8000`.

## Operations console

The Phase 6 application layer exposes latest validated telemetry, model
predictions, actionable alerts, and human-reviewed agent recommendations.

```powershell
python -m uvicorn src.api.main:app --reload
```

Open `http://127.0.0.1:8000` for the local dashboard or
`http://127.0.0.1:8000/docs` for the interactive API. The application is
decision-support only: acknowledging an alert or approving a recommendation
never sends a command to equipment.

With `REALTIME_MQTT_ENABLED=true` in `.env`, the API also subscribes to the
Python simulator's AWS IoT Core topic and sends each validated analytics update
to the browser over WebSockets. See [the application runbook](apps/README.md)
for the two-terminal command sequence and required AWS IoT permissions.

## MQTT streaming

First start a local MQTT broker (Docker Desktop required):

```powershell
docker compose up -d
```

Install the publisher dependency once, then keep the simulator API running in one terminal and run the publisher in a second terminal:

```powershell
npm install
npm run mqtt:publish
```

Run the gateway in a third terminal. It validates and enriches raw messages before publishing events for the downstream maintenance and optimization services:

```powershell
npm run mqtt:gateway
```

The pipeline is:

```
Simulator API → publisher → industrial/raw/telemetry/{machine} → IoT gateway → industrial/processed/telemetry/{machine}
                                                                  └──────────→ industrial/alerts/{machine}
```

Raw telemetry is published every five seconds to:

```
industrial/raw/telemetry/CNC-01
industrial/raw/telemetry/PUMP-01
industrial/raw/telemetry/CONV-01
```

Use a fourth terminal to observe all messages:

```powershell
npm run mqtt:subscribe
```

Configuration is optional and uses environment variables: `MQTT_BROKER_URL`, `SIMULATOR_API_URL`, `MQTT_TOPIC_PREFIX`, and `PUBLISH_INTERVAL_MS`.

## AWS IoT Core → Kinesis Data Streams

The gateway does not need AWS access keys. The cloud bridge authenticates to AWS IoT Core using its X.509 device certificate over MQTT/TLS, and the AWS IoT Rule securely forwards each message to Kinesis.

Deploy the Kinesis stream, IoT rule, and least-privilege IAM role:

```powershell
aws cloudformation deploy --stack-name industrial-iot-streaming --template-file cloud/aws/infrastructure/aws-iot-kinesis.yaml --capabilities CAPABILITY_IAM
```

In AWS IoT Core, create a certificate, activate it, and attach the CloudFormation output policy named `GatewayIotPolicyName`. Download its certificate, private key, and Amazon Root CA 1 into a local `certs` folder. Copy `.env.aws.example` values into your PowerShell session, using your actual certificate paths. Get the endpoint with:

```powershell
aws iot describe-endpoint --endpoint-type iot:Data-ATS --query endpointAddress --output text
```

Then start the bridge:

```powershell
npm run aws:bridge
```

It forwards `industrial/processed/telemetry/{machine}` to AWS IoT Core topic `factory/processed/telemetry/{machine}`. The deployed IoT rule writes each message to the Kinesis stream using the MQTT topic as the partition key.

`apps/api/fastapi-service.py` and `apps/api/requirements.txt` retain an optional FastAPI version; the Node simulator in `edge/simulator/` is the current local runtime.
