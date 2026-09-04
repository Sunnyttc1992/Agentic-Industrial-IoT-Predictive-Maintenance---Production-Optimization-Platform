import random
from datetime import datetime, timezone


class Machine:

    def __init__(self, machine_id):
        self.machine_id = machine_id

    def generate_telemetry(self) -> dict:

        # ----------------------------------------------------
        # NORMAL SENSOR VALUES
        # ----------------------------------------------------

        temperature = random.uniform(60, 75)
        vibration = random.uniform(1, 3)
        current = random.uniform(10, 16)
        pressure = random.uniform(5.5, 7.0)
        rpm = random.randint(3000, 3300)

        # Default ground-truth state
        fault_type = "NORMAL"
        is_fault = 0


        # ----------------------------------------------------
        # FAULT INJECTION
        # ----------------------------------------------------

        fault_roll = random.random()

        # About 10% of telemetry will contain a fault
        if fault_roll >= 0.90:

            is_fault = 1


            # -----------------------------------------------
            # Bearing degradation
            # -----------------------------------------------

            if fault_roll < 0.925:

                vibration += random.uniform(
                    2.0,
                    4.0
                )

                fault_type = (
                    "BEARING_DEGRADATION"
                )


            # -----------------------------------------------
            # Overheating
            # -----------------------------------------------

            elif fault_roll < 0.95:

                temperature += random.uniform(
                    15.0,
                    30.0
                )

                fault_type = "OVERHEATING"


            # -----------------------------------------------
            # Motor overload
            # -----------------------------------------------

            elif fault_roll < 0.975:

                current += random.uniform(
                    6.0,
                    12.0
                )

                fault_type = "MOTOR_OVERLOAD"


            # -----------------------------------------------
            # Pressure leak
            # -----------------------------------------------

            else:

                pressure -= random.uniform(
                    1.5,
                    3.0
                )

                fault_type = "PRESSURE_LEAK"


        # ----------------------------------------------------
        # BUILD TELEMETRY PAYLOAD
        # ----------------------------------------------------

        telemetry = {

            "machine_id": self.machine_id,

            "timestamp": (
                datetime
                .now(timezone.utc)
                .isoformat()
            ),

            "temperature_c": round(
                temperature,
                2
            ),

            "vibration_mm_s": round(
                vibration,
                2
            ),

            "current_amp": round(
                current,
                2
            ),

            "pressure_bar": round(
                pressure,
                2
            ),

            "rpm": rpm,

            "status": "RUNNING",

            "fault_type": fault_type,

            "is_fault": is_fault
        }


        return telemetry
    