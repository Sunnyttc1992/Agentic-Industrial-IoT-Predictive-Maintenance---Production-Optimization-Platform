from .machine import Machine
from .config import MACHINE_IDS


class Factory:
    def __init__(self):
        self.machines = [
            Machine(machine_id)
            for machine_id in MACHINE_IDS]

    def generate_all_telemetry(self) -> list:
        telemetry_data = []

        for machine in self.machines:
            telemetry = machine.generate_telemetry()
            telemetry_data.append(telemetry)

        return telemetry_data

