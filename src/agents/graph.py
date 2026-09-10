"""LangGraph supervisor for reviewable industrial-agent recommendations."""

from __future__ import annotations

import operator
from typing import Annotated, Any, TypedDict

from langgraph.graph import END, START, StateGraph

from src.streaming.stream_processor import process as process_stream_event

from .maintenance_agent import recommend_maintenance
from .production_agent import recommend_production
from .root_cause_agent import investigate_root_cause
from .supervisor_agent import select_agents


class AgentState(TypedDict, total=False):
    """Shared, serializable state for the supervisor and specialist nodes."""

    event: dict[str, Any]
    action_event: dict[str, Any]
    selected_agents: list[str]
    review_required: bool
    maintenance_recommendation: dict[str, Any]
    root_cause_analysis: dict[str, Any]
    production_recommendation: dict[str, Any]
    audit_trail: Annotated[list[dict[str, Any]], operator.add]


def supervisor_node(state: AgentState) -> dict[str, Any]:
    """Canonicalize the event, select agents, and record the routing decision."""
    action_event = process_stream_event(state["event"])
    selected_agents = select_agents(action_event)

    return {
        "action_event": action_event,
        "selected_agents": selected_agents,
        "review_required": action_event["review_required"],
        "audit_trail": [
            {
                "actor": "supervisor",
                "event_type": action_event["event_type"],
                "severity": action_event["severity"],
                "selected_agents": selected_agents,
                "review_required": action_event["review_required"],
            }
        ],
    }


def maintenance_node(state: AgentState) -> dict[str, Any]:
    """Create a maintenance recommendation from the canonical action event."""
    recommendation = recommend_maintenance(state["action_event"])
    return {
        "maintenance_recommendation": recommendation,
        "audit_trail": [{"actor": "maintenance", "status": "RECOMMENDED"}],
    }


def root_cause_node(state: AgentState) -> dict[str, Any]:
    """Create root-cause hypotheses from the canonical action event."""
    analysis = investigate_root_cause(state["action_event"])
    return {
        "root_cause_analysis": analysis,
        "audit_trail": [{"actor": "root_cause", "status": "RECOMMENDED"}],
    }


def production_node(state: AgentState) -> dict[str, Any]:
    """Create a production-safe recommendation from the action event."""
    recommendation = recommend_production(state["action_event"])
    return {
        "production_recommendation": recommendation,
        "audit_trail": [{"actor": "production", "status": "RECOMMENDED"}],
    }


def route_to_agents(state: AgentState) -> list[str]:
    """Fan out only to the agents selected by the supervisor."""
    return state["selected_agents"] or [END]


def build_supervisor_graph():
    """Build the explicit supervisor-to-specialist LangGraph workflow."""
    builder = StateGraph(AgentState)
    builder.add_node("supervisor", supervisor_node)
    builder.add_node("maintenance", maintenance_node)
    builder.add_node("root_cause", root_cause_node)
    builder.add_node("production", production_node)

    builder.add_edge(START, "supervisor")
    builder.add_conditional_edges(
        "supervisor",
        route_to_agents,
        {
            "maintenance": "maintenance",
            "root_cause": "root_cause",
            "production": "production",
            END: END,
        },
    )
    builder.add_edge("maintenance", END)
    builder.add_edge("root_cause", END)
    builder.add_edge("production", END)

    return builder.compile()


SUPERVISOR_GRAPH = build_supervisor_graph()


def run_supervisor(event: dict[str, Any]) -> dict[str, Any]:
    """Run the reviewable agent workflow for one raw telemetry event."""
    return SUPERVISOR_GRAPH.invoke({"event": event, "audit_trail": []})
