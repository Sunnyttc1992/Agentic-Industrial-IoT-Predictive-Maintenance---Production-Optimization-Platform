"""Tests for the low-latency MQTT-to-dashboard analytics path."""

from __future__ import annotations

import asyncio

from fastapi.testclient import TestClient

from src.api.main import app
from src.api.realtime import LiveAnalyticsPipeline
from src.streaming.stream_processor import process as process_stream_event


class BroadcastSpy:
    def __init__(self) -> None:
        self.messages: list[dict] = []

    async def broadcast(self, message: dict) -> None:
        self.messages.append(message)


class StaticPredictor:
    def predict(self, event: dict, features: dict) -> dict:
        return {
            "machine_id": event["machine_id"],
            "timestamp": event["timestamp"],
            "model_available": True,
            "fault_probability": 0.12,
            "predicted_fault": 0,
            "predicted_fault_type": "NORMAL",
            "alert_threshold": 0.15,
            "anomaly_detected": False,
            "anomaly_score": 0.2,
            "model_error": None,
        }


def telemetry_event(**overrides: object) -> dict:
    event = {
        "machine_id": "realtime_test_machine",
        "timestamp": "2099-01-01T00:00:00+00:00",
        "temperature_c": 67.0,
        "vibration_mm_s": 2.0,
        "current_amp": 12.0,
        "pressure_bar": 6.0,
        "rpm": 3200,
        "status": "RUNNING",
    }
    event.update(overrides)
    return event


def test_live_event_updates_dashboard_payload_without_parquet_refresh():
    broadcaster = BroadcastSpy()
    pipeline = LiveAnalyticsPipeline(broadcaster, predictor=StaticPredictor())

    result = asyncio.run(pipeline.process_event(telemetry_event()))

    assert result is not None
    assert result["accepted"] is True
    assert len(broadcaster.messages) == 1
    message = broadcaster.messages[0]
    assert message["type"] == "realtime_update"
    assert message["telemetry"]["machine_id"] == "realtime_test_machine"
    assert message["prediction"]["fault_probability"] == 0.12
    assert message["alert"] is None


def test_live_pipeline_rejects_duplicate_and_creates_one_reviewable_alert():
    broadcaster = BroadcastSpy()
    agent_calls: list[dict] = []

    def agent_runner(event: dict) -> dict:
        agent_calls.append(event)
        action_event = process_stream_event(event)
        return {
            "action_event": action_event,
            "selected_agents": action_event["agent_targets"],
            "review_required": True,
        }

    pipeline = LiveAnalyticsPipeline(
        broadcaster,
        predictor=StaticPredictor(),
        agent_runner=agent_runner,
    )
    event = telemetry_event(
        machine_id="realtime_critical_machine",
        timestamp="2099-01-01T00:00:01+00:00",
        temperature_c=95.0,
    )

    accepted = asyncio.run(pipeline.process_event(event))
    duplicate = asyncio.run(pipeline.process_event(event))

    assert accepted is not None and accepted["accepted"] is True
    assert accepted["alert"]["severity"] == "CRITICAL"
    assert accepted["agent_review"]["approval"]["status"] == "PENDING"
    assert duplicate == {"accepted": False, "reason": "duplicate_or_stale_event"}
    assert len(agent_calls) == 1
    assert len(broadcaster.messages) == 1


def test_live_pipeline_rejects_out_of_range_sensor_values():
    broadcaster = BroadcastSpy()
    pipeline = LiveAnalyticsPipeline(broadcaster, predictor=StaticPredictor())

    result = asyncio.run(
        pipeline.process_event(
            telemetry_event(
                machine_id="realtime_invalid_machine",
                timestamp="2099-01-01T00:00:02+00:00",
                pressure_bar=-1,
            )
        )
    )

    assert result is not None
    assert result["accepted"] is False
    assert "Out-of-range" in result["reason"]
    assert broadcaster.messages == []


def test_operations_websocket_sends_a_current_snapshot(monkeypatch):
    monkeypatch.setenv("REALTIME_MQTT_ENABLED", "false")
    with TestClient(app) as client:
        with client.websocket_connect("/ws/operations") as websocket:
            message = websocket.receive_json()

    assert message["type"] == "snapshot"
    assert {"machines", "predictions", "alerts", "approvals", "realtime"} <= set(message)
