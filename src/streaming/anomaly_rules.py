def is_anomalous(event: dict) -> bool:
    return event.get("temperature_c", 0) >= 78 or event.get("vibration_mm_s", 0) >= 3.4
