import pytest

from src.simulator.config import MACHINE_IDS
from src.simulator.machine_simulator import generate_all_readings, next_reading


def test_next_reading_uses_the_default_configured_machine():
    assert next_reading()["machine_id"] == MACHINE_IDS[0]


def test_next_reading_supports_a_configured_machine():
    assert next_reading("press_03")["machine_id"] == "press_03"


def test_next_reading_rejects_unknown_machine():
    with pytest.raises(ValueError, match="Unknown machine_id"):
        next_reading("unknown-machine")


def test_generate_all_readings_covers_every_configured_machine():
    readings = generate_all_readings()

    assert {reading["machine_id"] for reading in readings} == set(MACHINE_IDS)
