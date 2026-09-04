"""Apply anomaly rules and derive stream-level events."""
from .anomaly_rules import is_anomalous

def process(event: dict) -> dict:
    return {**event, "anomaly_detected": is_anomalous(event)}
