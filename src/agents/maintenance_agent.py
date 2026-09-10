"""Create reviewable maintenance recommendations from actionable events."""

from __future__ import annotations

from typing import Any


RULE_ACTIONS = {
    "OVERHEATING_CRITICAL": [
        "Inspect cooling, lubrication, and airflow systems.",
        "Verify the temperature sensor before diagnosing equipment failure.",
    ],
    "OVERHEATING_WARNING": [
        "Inspect cooling performance during the next planned maintenance window.",
        "Increase temperature monitoring frequency.",
    ],
    "BEARING_VIBRATION_CRITICAL": [
        "Inspect bearing condition and mounting before the next production cycle.",
        "Collect a vibration spectrum for confirmation.",
    ],
    "BEARING_VIBRATION_WARNING": [
        "Schedule a bearing inspection and trend vibration readings.",
    ],
    "MOTOR_OVERLOAD_CRITICAL": [
        "Inspect motor load, electrical connections, and driven equipment.",
        "Keep the machine out of automatic restart until reviewed.",
    ],
    "MOTOR_OVERLOAD_WARNING": [
        "Review motor load and inspect for developing mechanical resistance.",
    ],
    "PRESSURE_LEAK_CRITICAL": [
        "Inspect hydraulic or pneumatic lines for leaks before continued operation.",
        "Verify the pressure transmitter against a reference gauge.",
    ],
    "PRESSURE_LEAK_WARNING": [
        "Inspect pressure-system fittings and trend pressure loss.",
    ],
}


def recommend_maintenance(action_event: dict[str, Any]) -> dict[str, Any]:
    """Return a non-executing maintenance recommendation with its evidence."""
    rules = action_event.get("triggered_rules", [])
    actions = [
        action
        for rule in rules
        for action in RULE_ACTIONS.get(rule, [])
    ]
    if not actions:
        actions = [
            "Review the event and inspect the affected machine before action."
        ]

    return {
        "agent": "maintenance",
        "machine_id": action_event["machine_id"],
        "priority": action_event["severity"],
        "recommended_actions": list(dict.fromkeys(actions)),
        "evidence": rules,
        "execution_status": "PENDING_HUMAN_REVIEW",
        "review_required": True,
    }
