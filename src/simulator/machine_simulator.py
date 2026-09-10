"""Compatibility entry point for generating simulated machine telemetry.

The simulator package now exposes ``Machine`` and ``Factory`` directly, while
the Makefile and existing consumers still invoke ``machine_simulator``. This
module keeps that public entry point stable without duplicating simulation
logic.
"""

from __future__ import annotations

import json

from .config import MACHINE_IDS
from .factory import Factory
from .machine import Machine


def next_reading(machine_id: str | None = None) -> dict:
    """Generate one telemetry reading for a configured machine.

    The first configured machine is used when no ID is provided. Restricting
    IDs to the factory configuration prevents telemetry from silently being
    emitted for an unknown asset.
    """
    selected_machine_id = machine_id or MACHINE_IDS[0]
    if selected_machine_id not in MACHINE_IDS:
        raise ValueError(
            f"Unknown machine_id '{selected_machine_id}'. "
            f"Expected one of: {', '.join(MACHINE_IDS)}"
        )

    return Machine(selected_machine_id).generate_telemetry()


def generate_all_readings() -> list[dict]:
    """Generate one telemetry reading for every configured machine."""
    return Factory().generate_all_telemetry()


def main() -> None:
    """Print one complete simulated factory snapshot as newline-delimited JSON."""
    for telemetry in generate_all_readings():
        print(json.dumps(telemetry))


if __name__ == "__main__":
    main()
