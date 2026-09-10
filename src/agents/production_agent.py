"""Recommend production-safe responses to actionable machine events."""

from __future__ import annotations

from typing import Any


def recommend_production(action_event: dict[str, Any]) -> dict[str, Any]:
    """Return a production recommendation without changing machine operation."""
    severity = action_event["severity"]

    if severity == "CRITICAL":
        operating_mode = "HOLD_OR_REDUCE_LOAD_PENDING_REVIEW"
        recommendation = (
            "Do not automatically restart or increase load; shift work only after "
            "an operator reviews maintenance risk and available capacity."
        )
    else:
        operating_mode = "CONTINUE_WITH_HEIGHTENED_MONITORING"
        recommendation = (
            "Continue only within approved operating limits while maintenance "
            "reviews the anomaly and production capacity is monitored."
        )

    return {
        "agent": "production",
        "machine_id": action_event["machine_id"],
        "operating_mode": operating_mode,
        "recommendation": recommendation,
        "evidence": action_event.get("triggered_rules", []),
        "execution_status": "PENDING_HUMAN_REVIEW",
        "review_required": True,
    }
