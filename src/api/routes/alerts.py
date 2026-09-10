"""Actionable alert query and acknowledgement routes."""

from fastapi import APIRouter, HTTPException

from ..schemas import AlertAcknowledgementRequest
from ..services import alert_store, current_alerts


router = APIRouter(prefix="/api/v1/alerts", tags=["alerts"])


@router.get("")
def list_alerts() -> dict[str, object]:
    """Return current reviewable alerts from the latest telemetry."""
    alerts = current_alerts()
    return {"count": len(alerts), "alerts": alerts}


@router.post("/{alert_id}/acknowledge")
def acknowledge_alert(
    alert_id: str, request: AlertAcknowledgementRequest
) -> dict[str, object]:
    """Record acknowledgement only; it does not approve or execute an action."""
    matching_alert = next(
        (alert for alert in current_alerts() if alert["alert_id"] == alert_id),
        None,
    )
    if matching_alert is None:
        raise HTTPException(status_code=404, detail=f"Unknown alert: {alert_id}")

    acknowledgement = alert_store.acknowledge(
        alert_id, request.acknowledged_by, request.note
    )
    return {**matching_alert, "status": "ACKNOWLEDGED", **acknowledgement}
