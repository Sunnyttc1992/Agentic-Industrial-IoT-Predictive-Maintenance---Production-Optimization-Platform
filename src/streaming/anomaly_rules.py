"""Deterministic safety rules for live machine telemetry."""

SENSOR_FIELDS = (
    "temperature_c",
    "vibration_mm_s",
    "current_amp",
    "pressure_bar",
)


def evaluate_rules(event: dict) -> dict:
    """Return the anomaly decision and operational context."""
    missing = [field for field in SENSOR_FIELDS if field not in event]

    if missing:
        raise ValueError(f"Missing required sensor fields: {missing}")

    try:
        temperature = float(event["temperature_c"])
        vibration = float(event["vibration_mm_s"])
        current = float(event["current_amp"])
        pressure = float(event["pressure_bar"])
    except (TypeError, ValueError) as error:
        raise ValueError("Sensor values must be numeric.") from error

    critical_rules = []
    warning_rules = []

    if temperature >= 90:
        critical_rules.append("OVERHEATING_CRITICAL")
    elif temperature >= 75:
        warning_rules.append("OVERHEATING_WARNING")

    if vibration >= 8:
        critical_rules.append("BEARING_VIBRATION_CRITICAL")
    elif vibration >= 5:
        warning_rules.append("BEARING_VIBRATION_WARNING")

    if current >= 24:
        critical_rules.append("MOTOR_OVERLOAD_CRITICAL")
    elif current >= 18:
        warning_rules.append("MOTOR_OVERLOAD_WARNING")

    if pressure <= 2.8:
        critical_rules.append("PRESSURE_LEAK_CRITICAL")
    elif pressure <= 4.0:
        warning_rules.append("PRESSURE_LEAK_WARNING")

    triggered_rules = critical_rules or warning_rules

    if critical_rules:
        severity = "CRITICAL"
    elif warning_rules:
        severity = "WARNING"
    else:
        severity = "NORMAL"

    return {
        "anomaly_detected": bool(triggered_rules),
        "severity": severity,
        "triggered_rules": triggered_rules,
    }
