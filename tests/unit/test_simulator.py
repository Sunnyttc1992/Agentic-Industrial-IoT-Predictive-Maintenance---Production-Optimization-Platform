from src.simulator.machine_simulator import next_reading

def test_simulator_returns_required_fields():
    assert {"machine_id", "temperature_c", "vibration_mm_s"}.issubset(next_reading())
