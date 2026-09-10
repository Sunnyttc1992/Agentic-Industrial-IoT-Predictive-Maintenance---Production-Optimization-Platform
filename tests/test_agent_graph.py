import pytest

from src.agents.graph import run_supervisor
from src.streaming.stream_processor import process


def normal_event() -> dict:
    return {
        "machine_id": "press_01",
        "timestamp": "2026-09-04T15:00:00+00:00",
        "temperature_c": 62.0,
        "vibration_mm_s": 2.4,
        "current_amp": 10.5,
        "pressure_bar": 5.8,
        "rpm": 3300,
        "status": "RUNNING",
    }


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


def warning_event() -> dict:
    return {
        "machine_id": "press_02",
        "timestamp": "2026-09-04T15:00:30+00:00",
        "temperature_c": 78.0,
        "vibration_mm_s": 2.4,
        "current_amp": 10.5,
        "pressure_bar": 5.8,
        "rpm": 3300,
        "status": "RUNNING",
    }


def test_stream_event_has_actionable_metadata():
    result = process(critical_event())

    assert result["event_type"] == "OVERHEATING_CRITICAL"
    assert result["agent_targets"] == [
        "maintenance",
        "root_cause",
        "production",
    ]
    assert result["review_required"] is True


def test_supervisor_does_not_call_agents_for_normal_telemetry():
    result = run_supervisor(normal_event())

    assert result["review_required"] is False
    assert result["selected_agents"] == []
    assert "maintenance_recommendation" not in result
    assert len(result["audit_trail"]) == 1


def test_supervisor_sends_warning_events_to_maintenance_and_production():
    result = run_supervisor(warning_event())

    assert result["selected_agents"] == ["maintenance", "production"]
    assert "root_cause_analysis" not in result
    assert result["maintenance_recommendation"]["priority"] == "WARNING"
    assert result["production_recommendation"]["operating_mode"] == (
        "CONTINUE_WITH_HEIGHTENED_MONITORING"
    )


def test_supervisor_fans_out_critical_events_to_all_specialists():
    result = run_supervisor(critical_event())

    assert result["review_required"] is True
    assert result["selected_agents"] == [
        "maintenance",
        "root_cause",
        "production",
    ]
    assert result["maintenance_recommendation"]["review_required"] is True
    assert result["root_cause_analysis"]["hypotheses"][0]["triggered_rule"] == (
        "OVERHEATING_CRITICAL"
    )
    assert result["production_recommendation"]["operating_mode"] == (
        "HOLD_OR_REDUCE_LOAD_PENDING_REVIEW"
    )
    assert {entry["actor"] for entry in result["audit_trail"]} == {
        "supervisor",
        "maintenance",
        "root_cause",
        "production",
    }


def test_supervisor_rejects_incomplete_telemetry():
    with pytest.raises(ValueError, match="Missing required sensor fields"):
        run_supervisor({"machine_id": "press_01"})
