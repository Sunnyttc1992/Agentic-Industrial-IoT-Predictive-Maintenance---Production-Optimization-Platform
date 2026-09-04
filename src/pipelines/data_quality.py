REQUIRED_TELEMETRY_FIELDS = {"timestamp", "machine_id", "temperature_c", "vibration_mm_s", "rpm"}

def validate_event(event: dict) -> bool:
    return REQUIRED_TELEMETRY_FIELDS.issubset(event)
