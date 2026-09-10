from fastapi.testclient import TestClient

from src.api.main import app


client = TestClient(app)


def critical_event() -> dict:
    return {
        "machine_id": "press_03",
        "timestamp": "2026-09-04T15:01:00+00:00",
        "temperature_c": 94.0,
        "vibration_mm_s": 3.1,
        "current_amp": 12.0,
        "pressure_bar": 5.6,
        "rpm": 3300,
        "status": "RUNNING",
    }


def test_dashboard_and_health_are_available():
    dashboard = client.get("/")
    health = client.get("/health")

    assert dashboard.status_code == 200
    assert "Industrial IoT Operations Console" in dashboard.text
    assert health.json()["status"] == "ok"
    assert "silver_telemetry_available" in health.json()["data"]


def test_machine_prediction_and_alert_collections_have_stable_shapes():
    machines = client.get("/api/v1/machines")
    predictions = client.get("/api/v1/predictions")
    alerts = client.get("/api/v1/alerts")

    assert machines.status_code == 200
    assert set(machines.json()) == {"count", "machines"}
    assert predictions.status_code == 200
    assert set(predictions.json()) == {"count", "predictions"}
    assert alerts.status_code == 200
    assert set(alerts.json()) == {"count", "alerts"}


def test_agent_analysis_creates_reviewable_approval_without_actuating():
    analysis = client.post("/api/v1/agents/analyze", json=critical_event())

    assert analysis.status_code == 200
    body = analysis.json()
    assert body["review_required"] is True
    assert body["action_event"]["event_type"] == "OVERHEATING_CRITICAL"
    assert body["approval"]["status"] == "PENDING"
    assert body["maintenance_recommendation"]["execution_status"] == (
        "PENDING_HUMAN_REVIEW"
    )

    approval_id = body["approval"]["approval_id"]
    decision = client.post(
        f"/api/v1/approvals/{approval_id}",
        json={"decision": "APPROVED", "reviewer": "operator@example.com"},
    )

    assert decision.status_code == 200
    assert decision.json()["status"] == "APPROVED"
    assert decision.json()["reviewer"] == "operator@example.com"


def test_unknown_approval_returns_not_found():
    response = client.post(
        "/api/v1/approvals/missing",
        json={"decision": "REJECTED", "reviewer": "operator@example.com"},
    )

    assert response.status_code == 404
