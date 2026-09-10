"""Phase 5 agent analysis and human-approval routes."""

from fastapi import APIRouter, HTTPException

from src.agents.graph import run_supervisor

from ..schemas import ApprovalDecisionRequest, TelemetryEvent
from ..services import approval_store


router = APIRouter(prefix="/api/v1", tags=["agents", "approvals"])


@router.post("/agents/analyze")
def analyze_event(event: TelemetryEvent) -> dict[str, object]:
    """Run the supervisor and create a human-review record when required."""
    state = run_supervisor(event.model_dump(mode="json"))
    action_event = state["action_event"]
    approval = (
        approval_store.create(action_event)
        if action_event["review_required"]
        else None
    )

    return {
        "action_event": action_event,
        "selected_agents": state["selected_agents"],
        "review_required": state["review_required"],
        "maintenance_recommendation": state.get("maintenance_recommendation"),
        "root_cause_analysis": state.get("root_cause_analysis"),
        "production_recommendation": state.get("production_recommendation"),
        "audit_trail": state["audit_trail"],
        "approval": approval,
    }


@router.get("/approvals")
def list_approvals() -> dict[str, object]:
    """List local review records, newest first."""
    approvals = approval_store.list()
    return {"count": len(approvals), "approvals": approvals}


@router.post("/approvals/{approval_id}")
def decide_approval(
    approval_id: str, request: ApprovalDecisionRequest
) -> dict[str, object]:
    """Record a human decision without triggering any operational command."""
    approval = approval_store.decide(
        approval_id,
        request.decision.value,
        request.reviewer,
        request.notes,
    )
    if approval is None:
        raise HTTPException(status_code=404, detail=f"Unknown approval: {approval_id}")
    return approval
