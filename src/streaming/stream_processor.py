"""Apply anomaly rules and route stream-level events to specialist agents."""

from .anomaly_rules import evaluate_rules


def process(event: dict) -> dict:
    rule_result = evaluate_rules(event)
    severity = rule_result["severity"]

    if severity == "CRITICAL":
        agent_route = "maintenance_and_root_cause"
        agent_targets = ["maintenance", "root_cause", "production"]
    elif severity == "WARNING":
        agent_route = "maintenance"
        agent_targets = ["maintenance", "production"]
    else:
        agent_route = "none"
        agent_targets = []

    triggered_rules = rule_result["triggered_rules"]
    event_type = triggered_rules[0] if triggered_rules else "NORMAL_OPERATION"

    return {
        **event,
        **rule_result,
        "event_type": event_type,
        "agent_route": agent_route,
        "agent_targets": agent_targets,
        "review_required": severity != "NORMAL",
    }
