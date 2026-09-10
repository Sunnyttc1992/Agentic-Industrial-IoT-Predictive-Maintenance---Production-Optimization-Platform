"""Produce inspectable root-cause hypotheses from actionable events."""

from __future__ import annotations

from typing import Any


RULE_HYPOTHESES = {
    "OVERHEATING_CRITICAL": "Cooling or lubrication degradation, excessive load, or sensor error.",
    "OVERHEATING_WARNING": "Developing cooling inefficiency, elevated load, or sensor drift.",
    "BEARING_VIBRATION_CRITICAL": "Bearing damage, inadequate lubrication, misalignment, or loose mounting.",
    "BEARING_VIBRATION_WARNING": "Developing bearing wear, imbalance, or alignment drift.",
    "MOTOR_OVERLOAD_CRITICAL": "Mechanical binding, abnormal process load, or electrical supply issue.",
    "MOTOR_OVERLOAD_WARNING": "Increasing process resistance or an emerging electrical-load issue.",
    "PRESSURE_LEAK_CRITICAL": "Fluid or air leak, failed seal, pump issue, or pressure-sensor fault.",
    "PRESSURE_LEAK_WARNING": "Developing leak, declining pump performance, or pressure-sensor drift.",
}


def investigate_root_cause(action_event: dict[str, Any]) -> dict[str, Any]:
    """Return evidence-linked hypotheses; this agent does not assert a diagnosis."""
    rules = action_event.get("triggered_rules", [])
    hypotheses = [
        {
            "triggered_rule": rule,
            "hypothesis": RULE_HYPOTHESES.get(
                rule,
                "Insufficient evidence; inspect the machine and validate sensors.",
            ),
            "confidence": "REQUIRES_INSPECTION",
        }
        for rule in rules
    ]

    return {
        "agent": "root_cause",
        "machine_id": action_event["machine_id"],
        "hypotheses": hypotheses,
        "evidence": rules,
        "execution_status": "PENDING_HUMAN_REVIEW",
        "review_required": True,
    }
