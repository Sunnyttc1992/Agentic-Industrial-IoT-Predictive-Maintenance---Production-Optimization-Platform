"""Dynamic Industrial IoT telemetry simulator API.

Run locally with: uvicorn app:app --reload
"""

from __future__ import annotations

import math
import random
from datetime import UTC, datetime
from threading import Lock
from typing import Literal

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field


class TelemetryReading(BaseModel):
    timestamp: datetime
    machine_id: str
    machine_name: str
    status: Literal["healthy", "warning", "critical"]
    health_score: float = Field(ge=0, le=100)
    temperature_c: float
    vibration_mm_s: float
    pressure_bar: float
    power_kw: float
    rpm: int
    production_rate_units_hr: int


class MachineState:
    def __init__(self, machine_id: str, name: str, base_temp: float, base_power: float) -> None:
        self.machine_id = machine_id
        self.name = name
        self.base_temp = base_temp
        self.base_power = base_power
        self.tick = 0
        self.wear = random.uniform(0.0, 2.5)

    def next_reading(self) -> TelemetryReading:
        self.tick += 1
        # Slow cycles model normal operation; noise keeps each API call distinct.
        cycle = math.sin(self.tick / 7)
        load = math.sin(self.tick / 11 + 0.8)
        self.wear = max(0, min(22, self.wear + random.uniform(-0.18, 0.35)))

        temperature = self.base_temp + 3.8 * cycle + self.wear * 0.55 + random.gauss(0, 0.45)
        vibration = 1.25 + 0.28 * load + self.wear * 0.13 + random.gauss(0, 0.06)
        pressure = 6.5 + 0.35 * cycle - self.wear * 0.025 + random.gauss(0, 0.04)
        power = self.base_power + 4 * load + self.wear * 0.20 + random.gauss(0, 0.35)
        rpm = round(1450 + 45 * cycle + random.gauss(0, 5))
        production_rate = round(500 - self.wear * 5 - abs(cycle) * 15 + random.gauss(0, 4))

        severity = max((temperature - 78) / 10, (vibration - 3.4) / 0.8, (self.wear - 13) / 8)
        status: Literal["healthy", "warning", "critical"]
        if severity >= 0.7:
            status = "critical"
        elif severity >= 0.15:
            status = "warning"
        else:
            status = "healthy"
        health = max(0, min(100, 100 - self.wear * 3.3 - max(0, temperature - 70) * 1.4 - max(0, vibration - 2.5) * 7))

        return TelemetryReading(
            timestamp=datetime.now(UTC),
            machine_id=self.machine_id,
            machine_name=self.name,
            status=status,
            health_score=round(health, 1),
            temperature_c=round(temperature, 2),
            vibration_mm_s=round(vibration, 3),
            pressure_bar=round(pressure, 2),
            power_kw=round(power, 2),
            rpm=rpm,
            production_rate_units_hr=max(0, production_rate),
        )


app = FastAPI(title="Industrial IoT Simulator API", version="0.1.0")
_lock = Lock()
_machines = {
    "CNC-01": MachineState("CNC-01", "CNC Milling Machine", 63, 18),
    "PUMP-01": MachineState("PUMP-01", "Hydraulic Pump", 58, 12),
    "CONV-01": MachineState("CONV-01", "Conveyor Drive", 52, 9),
}


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/v1/machines")
def list_machines() -> list[dict[str, str]]:
    return [{"machine_id": item.machine_id, "machine_name": item.name} for item in _machines.values()]


@app.get("/api/v1/telemetry", response_model=list[TelemetryReading])
def get_all_telemetry() -> list[TelemetryReading]:
    """Return one new, dynamically simulated reading for every machine."""
    with _lock:
        return [machine.next_reading() for machine in _machines.values()]


@app.get("/api/v1/telemetry/{machine_id}", response_model=TelemetryReading)
def get_machine_telemetry(machine_id: str) -> TelemetryReading:
    """Return the next simulated reading for a single machine."""
    with _lock:
        machine = _machines.get(machine_id.upper())
        if machine is None:
            raise HTTPException(status_code=404, detail=f"Unknown machine: {machine_id}")
        return machine.next_reading()


@app.post("/api/v1/simulation/reset")
def reset_simulation(seed: int | None = Query(default=None)) -> dict[str, str]:
    """Reset simulator state. Supply a seed for reproducible demonstration data."""
    with _lock:
        if seed is not None:
            random.seed(seed)
        for machine in _machines.values():
            machine.tick = 0
            machine.wear = random.uniform(0.0, 2.5)
    return {"message": "Simulation reset"}
