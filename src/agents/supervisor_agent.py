"""Route deterministic action events to the appropriate specialist agents."""

from __future__ import annotations

from typing import Any


def select_agents(action_event: dict[str, Any]) -> list[str]:
    """Return the explicitly authorized specialist targets for an event."""
    return list(action_event.get("agent_targets", []))
