from src.streaming.stream_processor import process


def test_normal_machine_event():
    event = {
        "machine_id": "press_01",
        "timestamp": "2026-09-04T15:00:00+00:00",
        "temperature_c": 62.0,
        "vibration_mm_s": 2.4,
        "current_amp": 10.5,
        "pressure_bar": 5.8,
        "rpm": 3300,
        "status": "RUNNING",
    }

    result = process(event)

    assert result["anomaly_detected"] is False
    assert result["severity"] == "NORMAL"
    assert result["event_type"] == "NORMAL_OPERATION"
    assert result["agent_targets"] == []
    assert result["review_required"] is False
    assert result["agent_route"] == "none"


def test_critical_overheating_event():
    event = {
        "machine_id": "press_03",
        "timestamp": "2026-09-04T15:01:00+00:00",
        "temperature_c": 94.0,
        "vibration_mm_s": 3.1,
        "current_amp": 12.0,
        "pressure_bar": 5.6,
        "rpm": 3300,
        "status": "RUNNING",
    }

    result = process(event)

    assert result["anomaly_detected"] is True
    assert result["severity"] == "CRITICAL"
    assert "OVERHEATING_CRITICAL" in result["triggered_rules"]
    assert result["event_type"] == "OVERHEATING_CRITICAL"
    assert result["review_required"] is True
    assert result["agent_route"] == "maintenance_and_root_cause"
    
